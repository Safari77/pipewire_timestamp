/*
 * pipewire_timestamp.c
 *
 * This program captures audio from a PipeWire source and writes an output file
 * (e.g. Matroska) that contains:
 * - an audio stream (re-encoded from the capture data, optionally filtered),
 * and
 * - a subtitle stream (SubRip) where we inject a new timestamp every 20 ms
 * (SUB_INTERVAL_US).
 *
 * The subtitle stream is “disabled” for encoding (AVDISCARD_ALL) and we write
 * our SRT packets manually.
 *
 * The captured audio (interleaved float) is converted to whatever the encoder
 * wants (sample format, sample rate, channel layout) with libswresample, so the
 * encoder no longer has to match the capture format. If an audio filter chain
 * is given it runs on the converted frames, and an AVAudioFifo re-chunks the
 * result into the encoder's required frame size.
 *
 * Output can optionally be split into time-based segments (--segment_time),
 * like ffmpeg's segment muxer. Segmentation happens at encoded-packet
 * boundaries, so no encoded packet is ever dropped at a cut: the current file's
 * trailer is written (flushing the interleaver) and the very packet that
 * crossed the boundary is the first packet of the next file.
 */

/* tm_gmtoff and setenv() live behind glibc feature macros; request them
   explicitly so the timezone-offset code also builds under strict -std=c11. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <locale.h>
#include <math.h>
#include <pipewire/pipewire.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spa/param/audio/format-utils.h>
#include <spa/param/audio/raw-utils.h>  // for spa_audio_info_raw
#include <spa/utils/result.h>           // for spa_strerror
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define OPTPARSE_IMPLEMENTATION
#include "optparse.h"

/* FFmpeg headers */
#include <libavcodec/avcodec.h>
#include <libavfilter/avfilter.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libavformat/avformat.h>
#include <libavutil/audio_fifo.h>
#include <libavutil/channel_layout.h>
#include <libavutil/dict.h>
#include <libavutil/mathematics.h>
#include <libavutil/opt.h>
#include <libavutil/samplefmt.h>
#include <libavutil/time.h>
#include <libavutil/timestamp.h>
#include <libavutil/version.h>
#include <libswresample/swresample.h>

/* STREAMING_CHUNK:Defining lock-free ring buffer and global state... */
/* Assume FFmpeg 7.0+ so we always use the new
   AVChannelLayout API. (No HAVE_CH_LAYOUT checks.) */

#define SUB_INTERVAL_US 20000  // 20 ms

/* ---------------------------
   Lock-Free Ring Buffer (Real-Time safety)
   --------------------------- */
/* Whisper inference blocks for six seconds */
#define RING_BUFFER_SIZE (1048576 * 16)  // Must be power of 2
#define RING_BUFFER_MASK (RING_BUFFER_SIZE - 1)

typedef struct {
    float data[RING_BUFFER_SIZE];
    atomic_size_t head;  // written ONLY by PW RT callback (producer)
    atomic_size_t tail;  // written ONLY by worker thread (consumer)
} FloatRingBuffer;

static FloatRingBuffer g_ring_buffer = {{0}, 0, 0};
static pthread_t worker_thread;
static atomic_int worker_quit_flag = 0;
static pthread_mutex_t g_worker_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_worker_cond = PTHREAD_COND_INITIALIZER;
/* Worker-private reused frame for swr output (avoids alloc/free per chunk).
 * Owned exclusively by the worker thread; allocated/resized under g_mutex
 * in init_resampler_locked and resample_to_staging_locked. */
static AVFrame* g_worker_resampled_frame = NULL;
static int g_worker_resampled_frame_cap = 0;

/* Set once EOF has been pushed into the filter graph (worker shutdown
   drain). Afterwards filtering bypasses the graph (buffersrc
   rejects frames after EOF) and writes straight to the encoder FIFO. */
static int g_filter_eof = 0;

/* Lock-free event counters bumped by the realtime PW callback instead of
   logging from it: pw_log_*() can format, allocate and do I/O, none of which is
   safe on the RT thread. A non-RT thread (the main loop) observes these and
   prints whenever they advance, coalescing bursts. */
static atomic_ullong g_rt_xruns = 0;           // ring-buffer overflows (drops)
static atomic_ullong g_rt_out_of_buffers = 0;  // pw_stream_dequeue_buffer NULL
static atomic_ullong g_rt_bad_chunks = 0;      // unusable/misaligned buffer
static atomic_ullong g_rt_ts_overflows =
    0;  // timestamp ring full (anchor drops)

/* Negotiated capture channel count, published by param_changed purely so the
   RT callback can round each buffer down to whole frames. The worker keeps its
   own copy (g_capture_channels) under g_mutex, which the RT thread never
   takes. */
static atomic_int g_rt_capture_channels = 0;

/* ---------------------------
   Global FFmpeg output state
   --------------------------- */
struct ffmpeg_data {
    AVFormatContext* ofmt_ctx;
    AVStream* audio_out_stream;
    AVStream* sub_out_stream;
    AVCodecContext* audio_enc_ctx;
    /* We don't use a subtitle encoder. */
    int64_t
        total_samples;    // encoder-domain samples already sent to the encoder
    int64_t next_sub_ts;  // next subtitle time in ms, relative to the current
                          // segment (reset to 0 at every file/segment boundary)
    int sample_rate;      // encoder sample rate (mirrors audio_enc_ctx)
    int channels;         // encoder channel count (mirrors audio_enc_ctx)
};
static struct ffmpeg_data ffmpeg_out = {0};

// Global mutex protecting the resampler / FIFO / capture-format fields shared
// between the worker thread and the (main-loop) param_changed handler. The RT
// capture callback never takes this lock; it only touches the lock-free ring.
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global filter graph pointers (if -af or --ebur is given) */
static AVFilterGraph* filter_graph = NULL;
static AVFilterContext* buffersrc_ctx = NULL;
static AVFilterContext* buffersink_ctx = NULL;

/* Resampling / buffering state (capture format -> encoder format). */
static struct SwrContext* g_swr_ctx =
    NULL;  // capture (interleaved F32) -> encoder format
static AVAudioFifo* g_fifo = NULL;  // holds converted (encoder-format) samples
static int g_capture_rate = 0;      // negotiated PipeWire rate
static int g_capture_channels = 0;  // negotiated PipeWire channel count

/* Pending capture-format handoff (param_changed -> worker), guarded by
 * g_mutex. param_changed publishes the new format plus the ring index at
 * which it begins; the worker splits the buffered audio at that seam,
 * drains the old half through the old resampler, then rebuilds swr -- so
 * the lock-free ring indices keep a single writer each and no buffered
 * audio is discarded on a format change. */
static int g_format_change_pending = 0;
static int g_pending_rate = 0;
static int g_pending_channels = 0;
/* Ring head snapshotted in on_pw_param_changed() BEFORE the RT callback
 * switched to the new channel count: every float below this index was
 * produced under the OLD format, everything at/above it under the NEW one.
 * Guarded by g_mutex. */
static size_t g_format_boundary = 0;
/* Start of a superseded (never-applied intermediate) format region, set
 * when a second format change arrives before the worker applied the first.
 * Those floats cannot be framed with either parameter set; they are skipped
 * and logged. Guarded by g_mutex. */
static int g_has_format_skip = 0;
static size_t g_format_skip_from = 0;
static AVChannelLayout g_capture_ch_layout = {
    0};  // default layout for g_capture_channels
static int64_t g_filter_pts =
    0;  // running pts (in encoder samples) fed to filter graph
static atomic_llong g_filter_in_samples = 0;
static atomic_llong g_filter_out_samples = 0;

/* STREAMING_CHUNK:Defining timestamp anchor queue and segment state... */
/* ---------------------------
   Capture-time propagation (exact subtitle timestamps).
   --------------------------- */
#define TS_RING_SIZE 16384  // power of 2; one record per PipeWire buffer/cycle
#define TS_RING_MASK (TS_RING_SIZE - 1)
typedef struct {
    size_t start_index;   // g_ring_buffer absolute FLOAT index of this buffer
    int64_t cap_wall_us;  // wall-clock (us) of this buffer's first sample
} TsRecord;
typedef struct {
    TsRecord recs[TS_RING_SIZE];
    atomic_size_t head;  // written ONLY by RT callback (producer)
    atomic_size_t tail;  // written ONLY by worker thread (consumer)
} TsRing;
static TsRing g_ts_ring = {{{0, 0}}, 0, 0};

/* Cumulative encoder samples written to the FIFO. A sample written at position
   P is later drained with pts == P (the FIFO is strictly in order and no
   padding is inserted mid-stream), so this counter lives in the same space as
   the audio packet pts the subtitle grid is built from. Touched only by the
   encoding thread (worker). */
static int64_t g_fifo_write_pos = 0;

/* History of capture-time anchors mapping encoder sample position -> wall-clock
   capture time. Using a queue rather than a single scalar prevents post-gap /
   post-reconnect anchors from prematurely shifting the timestamps of pre-gap
   audio still buffered inside swr, filter_graph, FIFOs, or the encoder. */
#define ENC_ANCHOR_RING_SIZE 4096
#define ENC_ANCHOR_RING_MASK (ENC_ANCHOR_RING_SIZE - 1)
typedef struct {
    int64_t enc_pts;
    int64_t wall_us;
} EncTsAnchor;
static EncTsAnchor g_enc_anchors[ENC_ANCHOR_RING_SIZE];
static size_t g_enc_anchor_head = 0;
static size_t g_enc_anchor_tail = 0;
static int g_ts_anchor_valid = 0;

/* Segmentation / filename-template state. */
static const char* g_output_template =
    NULL;                        // raw -o argument (may contain specifiers)
static int g_use_strftime = 0;   // --strftime given
static int g_segment_clock = 0;  // --segment-clock given
static int64_t g_next_segment_clock_us =
    0;  // next cut boundary in absolute wall-clock microseconds
static double g_segment_time =
    0.0;                         // --segment_time seconds; 0 == no segmentation
static int g_segment_index = 0;  // current segment number (0-based)
static int64_t g_segment_time_samples =
    0;  // segment_time expressed in encoder samples
static int64_t g_next_segment_pts = 0;  // next cut boundary, in encoder samples
static int64_t g_segment_pts_offset =
    0;  // subtracted from pts so each file starts near 0

/* Capture wall-clock (microseconds) at which the next output file begins,
   used by --strftime so the expanded name describes the audio the file
   actually contains instead of the moment the encoder happened to catch up.
   0 == not known yet (the very first file), which falls back to time(NULL). */
static int64_t g_next_file_wall_us = 0;

static char g_last_expanded[4096] = "";
static int g_collision_seq = 0;
static char g_current_filename[4096] = "";

/* Output channel override (--ac). 0 == use the encoder's preferred layout;
   otherwise the resampler mixes the capture layout to this channel count. */
static int g_out_channels = 0;

/* Periodic durability flush (--fsync-ms). */
static double g_fsync_ms = 0.0;  // --fsync-ms milliseconds; 0 == off
static int64_t g_fsync_interval_samples = 0;  // fsync period in encoder samples
static int64_t g_next_fsync_pts = 0;  // next fsync boundary (enc samples)
static int g_out_fd = -1;             // sync fd for the current file

/* Asynchronous fsync worker. */
static pthread_t g_fsync_thread;
static int g_fsync_thread_started = 0;
static pthread_mutex_t g_fsync_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_fsync_cond = PTHREAD_COND_INITIALIZER;
static int g_fsync_quit = 0;
static int g_fsync_periodic_fd = -1;  // coalesced slot; fd owned by the thread
static int g_fsync_final_fds[16];     // close-time fds, serviced in order
static int g_fsync_final_count = 0;

/* Subtitle timezone (--tz). */
static const char* g_tz = NULL;  // --tz value, e.g. "Europe/Helsinki"
static int g_tz_active = 0;      // set once TZ has been applied

/* Subtitle latency compensation (--latency). */
static double g_latency_ms = 0.0;

/* Container/track metadata (--title, and the --pw-target name as track
   comment). Applied to every (re)opened output file/segment. */
static const char* g_title = NULL;          // --title -> Matroska title
static const char* g_track_comment = NULL;  // -> audio track "comment" tag

/* Retention of segment files (--keep-segments N). */
static int g_keep_segments = 0;     // 0 == keep everything
static char** g_kept_files = NULL;  // FIFO of opened segment paths
static int g_kept_count = 0;
static int g_kept_cap = 0;

/* Dynamic subtitle metadata (--metadata-pipe, repeatable). */
#define META_MAX_PIPES 16
#define META_MAX_CP 40
#define META_LINE_BYTES (META_MAX_CP * 4 + 1)  // worst-case UTF-8 bytes + NUL

struct meta_pipe {
    char* path;        // FIFO path (from --metadata-pipe)
    int fd;            // open descriptor (O_RDWR so it never sees EOF)
    int created_fifo;  // we mkfifo()'d it and should unlink it at exit
    char shared[META_LINE_BYTES];  // latest line, guarded by g_meta_mutex
    char cached[META_LINE_BYTES];  // subtitle-thread private sticky copy
    char partial[1024];            // reader-thread private line accumulator
    size_t partial_len;            // bytes in partial[]
    int overflow;                  // current line exceeded partial[]
};
static struct meta_pipe g_meta_pipes[META_MAX_PIPES];
static int g_meta_count = 0;
static pthread_mutex_t g_meta_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_meta_thread;
static atomic_int g_meta_quit = 0;
static int g_meta_thread_started = 0;

/* Pre-filter staging: receives swr output (outside g_mutex in worker).
 * Post-filter staging: receives Whisper/filter output (outside g_mutex).
 * NEVER shared — prevents the NULL-dest memcpy crash from FIFO reuse. */
static AVAudioFifo* g_pre_filter_fifo = NULL;
static AVAudioFifo* g_post_filter_fifo = NULL;

/* Dedicated Whisper result queue: decouples the blocking GPU
 * inference from g_mutex so shutdown is never stalled by Whisper. */
static pthread_mutex_t g_whisper_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_whisper_cond = PTHREAD_COND_INITIALIZER;
static char g_whisper_result[META_LINE_BYTES] = "";
static int g_whisper_result_ready = 0;

/* EBU R128 loudness measurement (--ebur). */
static int g_ebur = 0;
static pthread_mutex_t g_ebur_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_ebur_m[16] = "";
static char g_ebur_s[16] = "";
static char g_ebur_i[16] = "";
static char g_ebur_lra[16] = "";
static int g_ebur_updated = 0;

/* STREAMING_CHUNK:Declaring helper macros and forward declarations... */
/* ---------------------------
   Helper macros & functions for FFmpeg
   --------------------------- */
#define FF_CHECK(err, msg)                              \
    do {                                                \
        if ((err) < 0) {                                \
            char errbuf[128];                           \
            av_strerror((err), errbuf, sizeof(errbuf)); \
            fprintf(stderr, "%s: %s\n", (msg), errbuf); \
            exit(1);                                    \
        }                                               \
    } while (0)

/* Process-wide termination flag set by signal handler/thread */
static atomic_int quit_flag = 0;

/* Forward declarations */
static void init_ffmpeg_encoder(const char* audio_encoder_name,
                                const char* audio_filter_str,
                                int64_t audio_bitrate);
static void open_output_file(const char* filename);
static void close_output_file(void);
static void fsync_output_file(void);
static int build_output_filename(char* buf, size_t sz, int index);
static int init_resampler_locked(void);
static void flush_swr_to_staging_locked(void);
static void apply_format_change_locked(float* chunk, size_t chunk_cap);
static AVFrame* alloc_audio_frame(int nb_samples);
static void drain_fifo_to_encoder(int final);
static void encode_frame(AVFrame* frame);
static void process_encoded_packet(AVPacket* pkt);
static void generate_subtitle_packet(int64_t grid_rel_ms);
static int64_t capture_wall_us_at(int64_t abs_enc_pts);
static void capture_anchor_update_locked(size_t tail);
static void advance_ts_ring_past_locked(size_t min_start_index);
static void flush_audio_encoder(void);
static void* audio_worker_thread(void* arg);
static void resample_to_staging_locked(const float* interleaved, int in_frames);
static void track_segment_file(const char* filename);
static int setup_metadata_pipes(void);
static void teardown_metadata_pipes(void);
static void sd_notify_ready(void);
static void sd_notify_status(const char* status);
static void extract_frame_metadata(AVFrame* frame);

/* Use avcodec_get_supported_config() to choose a sample rate: the supported
   rate nearest to 48000, falling back to 48000 when the encoder lists none. */
static int select_sample_rate(const AVCodec* codec) {
    const int* supported_samplerates = NULL;
    int num_samplerates = 0;
    int ret = avcodec_get_supported_config(
        NULL, codec, AV_CODEC_CONFIG_SAMPLE_RATE, 0,
        (const void**)&supported_samplerates, &num_samplerates);
    if (ret < 0 || !supported_samplerates || num_samplerates <= 0) return 48000;
    int best_samplerate = 0;
    for (int i = 0; i < num_samplerates; i++) {
        if (!best_samplerate || abs(48000 - supported_samplerates[i]) <
                                    abs(48000 - best_samplerate))
            best_samplerate = supported_samplerates[i];
    }
    return best_samplerate;
}

/* STREAMING_CHUNK:Implementing codec probing and resampler flushing... */
/* ---------------------------
   Test if an audio encoder can successfully encode at 48000 Hz.
   --------------------------- */
static int test_codec_at_48k(const AVCodec* codec) {
    /* Check if 48000 Hz is supported by the codec if a rate list exists. */
    const int* supported_samplerates = NULL;
    int num_samplerates = 0;
    int ret = avcodec_get_supported_config(
        NULL, codec, AV_CODEC_CONFIG_SAMPLE_RATE, 0,
        (const void**)&supported_samplerates, &num_samplerates);
    if (ret >= 0 && supported_samplerates && num_samplerates > 0) {
        int rate_ok = 0;
        for (int i = 0; i < num_samplerates; i++) {
            if (supported_samplerates[i] == 48000) {
                rate_ok = 1;
                break;
            }
        }
        if (!rate_ok) return 0;
    }

    AVCodecContext* ctx = avcodec_alloc_context3(codec);
    if (!ctx) return 0;

    ctx->sample_rate = 48000;
    ctx->time_base = (AVRational){1, 48000};
    ctx->bit_rate = 64000;
    ctx->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;

    /* Select sample format */
    const void* supported_fmts = NULL;
    int num_fmts = 0;
    ret =
        avcodec_get_supported_config(ctx, codec, AV_CODEC_CONFIG_SAMPLE_FORMAT,
                                     0, &supported_fmts, &num_fmts);
    if (ret >= 0 && num_fmts > 0 && supported_fmts) {
        const enum AVSampleFormat* sample_fmts = supported_fmts;
        ctx->sample_fmt = sample_fmts[0];
    } else {
        ctx->sample_fmt = AV_SAMPLE_FMT_FLT;
    }

    /* Select channel layout */
    const AVChannelLayout* ch_layouts = NULL;
    int num_ch_layouts = 0;
    ret = avcodec_get_supported_config(
        ctx, codec, AV_CODEC_CONFIG_CHANNEL_LAYOUT, 0,
        (const void**)&ch_layouts, &num_ch_layouts);
    if (ret >= 0 && ch_layouts && num_ch_layouts > 0) {
        if (av_channel_layout_copy(&ctx->ch_layout, &ch_layouts[0]) < 0) {
            avcodec_free_context(&ctx);
            return 0;
        }
    } else {
        av_channel_layout_default(&ctx->ch_layout, 1);
    }

    /* Suppress log output during probe */
    int orig_log_level = av_log_get_level();
    av_log_set_level(AV_LOG_QUIET);

    if (avcodec_open2(ctx, codec, NULL) < 0) {
        av_log_set_level(orig_log_level);
        av_channel_layout_uninit(&ctx->ch_layout);
        avcodec_free_context(&ctx);
        return 0;
    }

    /* Try encoding a test frame */
    int frame_size = ctx->frame_size > 0 ? ctx->frame_size : 1024;
    AVFrame* frame = av_frame_alloc();
    AVPacket* pkt = av_packet_alloc();
    int valid = 0;

    if (frame && pkt) {
        frame->nb_samples = frame_size;
        frame->format = ctx->sample_fmt;
        frame->sample_rate = 48000;
        if (av_channel_layout_copy(&frame->ch_layout, &ctx->ch_layout) == 0 &&
            av_frame_get_buffer(frame, 0) == 0) {
            av_samples_set_silence(frame->extended_data, 0, frame_size,
                                   ctx->ch_layout.nb_channels, ctx->sample_fmt);
            frame->pts = 0;
            int send_ret = avcodec_send_frame(ctx, frame);
            if (send_ret >= 0) {
                int recv_ret = avcodec_receive_packet(ctx, pkt);
                if (recv_ret >= 0 || recv_ret == AVERROR(EAGAIN) ||
                    recv_ret == AVERROR_EOF) {
                    valid = 1;
                }
            }
        }
    }

    /* Flush encoder queue to prevent warnings on close */
    avcodec_send_frame(ctx, NULL);
    if (pkt) {
        while (avcodec_receive_packet(ctx, pkt) >= 0) {
            av_packet_unref(pkt);
        }
    }

    if (frame) av_frame_free(&frame);
    if (pkt) av_packet_free(&pkt);

    av_channel_layout_uninit(&ctx->ch_layout);
    avcodec_free_context(&ctx);
    av_log_set_level(orig_log_level);

    return valid;
}

/* Append the resampler's internal delay to g_fifo (or g_pre_filter_fifo if
 * filtering) so that a pending swr_free() discards nothing. Must only be
 * called once ALL input for this context has been fed, so the retrieved
 * samples land directly after them in stream order. Loops calling
 * swr_convert(..., NULL, 0) until it returns <= 0, because swr_get_delay()
 * can report 0 due to integer truncation while filter phase tail samples
 * still remain inside SwrContext.
 * Caller holds g_mutex. */
static void flush_swr_to_staging_locked(void) {
    if (!g_swr_ctx) return;
    AVCodecContext* enc = ffmpeg_out.audio_enc_ctx;
    for (int guard = 0; guard < 32; guard++) {
        int64_t delay = swr_get_delay(g_swr_ctx, enc->sample_rate);
        int cap = (int)FFMIN(FFMAX(delay + 256, 256), 65536);
        AVFrame* f = alloc_audio_frame(cap);
        if (!f) break;
        int got = swr_convert(g_swr_ctx, f->extended_data, cap, NULL, 0);
        if (got > 0) {
            if (!filter_graph) {
                av_audio_fifo_write(g_fifo, (void**)f->extended_data, got);
                g_fifo_write_pos += got;
            } else {
                av_audio_fifo_write(g_pre_filter_fifo, (void**)f->extended_data,
                                    got);
            }
        }
        av_frame_free(&f);
        if (got <= 0) break;
    }
}

/* ---------------------------
   List available audio codecs verified to encode at 48000 Hz.
   --------------------------- */
static void list_supported_codecs(void) {
    void* opaque = NULL;
    const AVCodec* codec = NULL;

    printf("Available 48000 Hz audio encoding codecs:\n\n");
    printf(" %-20s %s\n", "CODEC", "DESCRIPTION");
    printf(" %-20s %s\n", "--------------------",
           "--------------------------------------------------");

    while ((codec = av_codec_iterate(&opaque))) {
        if (av_codec_is_encoder(codec) && codec->type == AVMEDIA_TYPE_AUDIO) {
            if (test_codec_at_48k(codec)) {
                printf(" %-20s %s\n", codec->name,
                       codec->long_name ? codec->long_name : "");
            }
        }
    }
}

/* STREAMING_CHUNK:Implementing metadata pipe reader and systemd notify... */
/* ---------------------------
   Dynamic subtitle metadata (--metadata-pipe) + sd-notify helpers
   --------------------------- */

/* Truncate src to at most max_cp UTF-8 codepoints into dst (NUL-terminated),
   never splitting a multibyte sequence. */
static void utf8_copy_truncate(char* dst, size_t dstsz, const char* src,
                               size_t max_cp) {
    if (!dst || dstsz == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    size_t di = 0, cp = 0, si = 0;
    while (src[si] != '\0' && cp < max_cp) {
        unsigned char c = (unsigned char)src[si];
        size_t len = (c >= 0xF0) ? 4 : (c >= 0xE0) ? 3 : (c >= 0xC0) ? 2 : 1;
        /* Count the continuation bytes actually present and valid. */
        size_t have = 1;
        while (have < len && ((unsigned char)src[si + have] & 0xC0) == 0x80)
            have++;
        if (have < len) {
            if (src[si + have] == '\0') break;
            len = 1;
        }
        if (di + len >= dstsz) break;
        memcpy(dst + di, src + si, len);
        di += len;
        si += len;
        cp++;
    }
    dst[di] = '\0';
}

/* Extract metadata entries from filtered audio frames (Whisper / EBU R128). */
static void extract_frame_metadata(AVFrame* frame) {
    if (!frame || !frame->metadata) return;
    AVDictionaryEntry* e = NULL;
    while ((e = av_dict_get(frame->metadata, "", e, AV_DICT_IGNORE_SUFFIX))) {
        if (!strcmp(e->key, "lavfi.whisper.text")) {
            pthread_mutex_lock(&g_whisper_mutex);
            utf8_copy_truncate(g_whisper_result, sizeof(g_whisper_result),
                               e->value, META_MAX_CP);
            g_whisper_result_ready = 1;
            pthread_cond_signal(&g_whisper_cond);
            pthread_mutex_unlock(&g_whisper_mutex);
            break;
        }
    }

    if (g_ebur) {
        AVDictionaryEntry* em =
            av_dict_get(frame->metadata, "lavfi.r128.M", NULL, 0);
        AVDictionaryEntry* es =
            av_dict_get(frame->metadata, "lavfi.r128.S", NULL, 0);
        AVDictionaryEntry* ei =
            av_dict_get(frame->metadata, "lavfi.r128.I", NULL, 0);
        AVDictionaryEntry* elra =
            av_dict_get(frame->metadata, "lavfi.r128.LRA", NULL, 0);
        if (em || es || ei || elra) {
            pthread_mutex_lock(&g_ebur_mutex);
            if (em)
                utf8_copy_truncate(g_ebur_m, sizeof(g_ebur_m), em->value,
                                   sizeof(g_ebur_m) - 1);
            if (es)
                utf8_copy_truncate(g_ebur_s, sizeof(g_ebur_s), es->value,
                                   sizeof(g_ebur_s) - 1);
            if (ei)
                utf8_copy_truncate(g_ebur_i, sizeof(g_ebur_i), ei->value,
                                   sizeof(g_ebur_i) - 1);
            if (elra)
                utf8_copy_truncate(g_ebur_lra, sizeof(g_ebur_lra), elra->value,
                                   sizeof(g_ebur_lra) - 1);
            g_ebur_updated = 1;
            pthread_mutex_unlock(&g_ebur_mutex);
        }
    }
}

/* Reader thread: commit one completed line into shared[] under the lock. */
static void meta_commit_line(struct meta_pipe* p) {
    size_t len = p->partial_len;
    if (len > 0 && p->partial[len - 1] == '\r') len--;
    p->partial[len] = '\0';
    char line[META_LINE_BYTES];
    utf8_copy_truncate(line, sizeof(line), p->partial, META_MAX_CP);
    pthread_mutex_lock(&g_meta_mutex);
    memcpy(p->shared, line, strlen(line) + 1);
    pthread_mutex_unlock(&g_meta_mutex);
    p->partial_len = 0;
    p->overflow = 0;
}

static void meta_consume(struct meta_pipe* p, const char* buf, size_t n) {
    for (size_t i = 0; i < n; i++) {
        char c = buf[i];
        if (c == '\n') {
            meta_commit_line(p);
        } else if (!p->overflow) {
            if (p->partial_len + 1 >= sizeof(p->partial))
                p->overflow = 1;  // line too long: keep prefix, drop the rest
            else
                p->partial[p->partial_len++] = c;
        }
    }
}

static void* metadata_reader_thread(void* arg) {
    (void)arg;
    struct pollfd pfds[META_MAX_PIPES];
    while (!atomic_load(&g_meta_quit)) {
        for (int i = 0; i < g_meta_count; i++) {
            pfds[i].fd = g_meta_pipes[i].fd;
            pfds[i].events = POLLIN;
            pfds[i].revents = 0;
        }
        int r = poll(pfds, g_meta_count, 200);
        if (r <= 0) continue;
        for (int i = 0; i < g_meta_count; i++) {
            if (!(pfds[i].revents & (POLLIN | POLLHUP | POLLERR))) continue;
            char tmp[512];
            ssize_t got;
            while ((got = read(g_meta_pipes[i].fd, tmp, sizeof(tmp))) > 0)
                meta_consume(&g_meta_pipes[i], tmp, (size_t)got);
        }
    }
    return NULL;
}

/* Create/open every --metadata-pipe FIFO and start the reader thread. */
static int setup_metadata_pipes(void) {
    if (g_meta_count == 0) return 0;
    for (int i = 0; i < g_meta_count; i++) {
        struct meta_pipe* p = &g_meta_pipes[i];
        p->fd = -1;
        p->shared[0] = p->cached[0] = '\0';
        p->partial_len = 0;
        p->overflow = 0;

        struct stat st, stcheck;
        if (stat(p->path, &st) != 0) {
            if (mkfifo(p->path, 0666) != 0) {
                fprintf(stderr, "metadata-pipe: cannot create FIFO %s: %s\n",
                        p->path, strerror(errno));
                return -1;
            }
            p->created_fifo = 1;
        } else if (!S_ISFIFO(st.st_mode)) {
            fprintf(stderr,
                    "metadata-pipe: %s exists and is not a FIFO; refusing it\n",
                    p->path);
            return -1;
        }

        p->fd = open(p->path, O_RDWR | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
        if (p->fd < 0) {
            fprintf(stderr, "metadata-pipe: cannot open %s: %s\n", p->path,
                    strerror(errno));
            return -1;
        }
        if (fstat(p->fd, &stcheck) != 0) {
            fprintf(stderr, "metadata-pipe: fstat failed for fd=%d: %s\n",
                    p->fd, strerror(errno));
            return -1;
        }
        if (!S_ISFIFO(stcheck.st_mode)) {
            fprintf(stderr, "metadata-pipe: %s was not FIFO\n", p->path);
            return -1;
        }
        fprintf(stderr, "metadata-pipe: subtitle line %d <- %s (fd=%d)\n",
                i + 1, p->path, p->fd);
    }
    if (pthread_create(&g_meta_thread, NULL, metadata_reader_thread, NULL) !=
        0) {
        fprintf(stderr, "metadata-pipe: failed to start reader thread\n");
        return -1;
    }
    g_meta_thread_started = 1;
    return 0;
}

static void teardown_metadata_pipes(void) {
    if (g_meta_count == 0) return;
    if (g_meta_thread_started) {
        atomic_store(&g_meta_quit, 1);
        pthread_join(g_meta_thread, NULL);
        g_meta_thread_started = 0;
    }
    for (int i = 0; i < g_meta_count; i++) {
        struct meta_pipe* p = &g_meta_pipes[i];
        if (p->fd >= 0) {
            close(p->fd);
            p->fd = -1;
        }
        if (p->created_fifo) unlink(p->path);
        free(p->path);
        p->path = NULL;
    }
}

/* Minimal sd_notify() without linking libsystemd */
static void sd_notify_send(const char* payload) {
    const char* sock = getenv("NOTIFY_SOCKET");
    if (!sock || !*sock) return;
    int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    size_t pl = strlen(sock);
    if (pl >= sizeof(addr.sun_path)) {
        close(fd);
        return;
    }
    memcpy(addr.sun_path, sock, pl + 1);
    socklen_t alen;
    if (addr.sun_path[0] == '@') {
        addr.sun_path[0] = '\0';
        alen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + pl);
    } else {
        alen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + pl + 1);
    }
    (void)sendto(fd, payload, strlen(payload), MSG_NOSIGNAL,
                 (struct sockaddr*)&addr, alen);
    close(fd);
}

static void sd_notify_ready(void) { sd_notify_send("READY=1\n"); }

static void sd_notify_status(const char* status) {
    char msg[4300];
    snprintf(msg, sizeof(msg), "STATUS=%s\n", status ? status : "");
    sd_notify_send(msg);
}

/* STREAMING_CHUNK:Implementing subtitle timestamp lookup and generation... */
/* Look up the capture wall-clock time (in microseconds) for encoder sample
   position abs_enc_pts using the anchor history queue so that pre-gap buffered
   audio uses its own pre-gap anchor instead of a newer post-gap anchor. */
static int64_t capture_wall_us_at(int64_t abs_enc_pts) {
    int64_t us;
    if (g_enc_anchor_head != g_enc_anchor_tail && ffmpeg_out.audio_enc_ctx) {
        const int enc_rate = ffmpeg_out.audio_enc_ctx->sample_rate;
        /* Prune anchors older than abs_enc_pts while keeping at least the most
           recent anchor at or before abs_enc_pts. */
        while (g_enc_anchor_tail + 1 < g_enc_anchor_head &&
               g_enc_anchors[(g_enc_anchor_tail + 1) & ENC_ANCHOR_RING_MASK]
                       .enc_pts <= abs_enc_pts) {
            g_enc_anchor_tail++;
        }
        EncTsAnchor a = g_enc_anchors[g_enc_anchor_tail & ENC_ANCHOR_RING_MASK];
        us = a.wall_us + av_rescale(abs_enc_pts - a.enc_pts, 1000000, enc_rate);
    } else {
        us = av_gettime();
    }
    return us - (int64_t)llround(g_latency_ms * 1000.0);
}

/* Emit one timestamp subtitle. */
static void generate_subtitle_packet(int64_t grid_rel_ms) {
    int ret;
    int64_t start_ms = grid_rel_ms;
    if (start_ms < 0) start_ms = 0;

    /* One full grid step, so this subtitle lasts until the next one begins. */
    int64_t dur_ms = SUB_INTERVAL_US / 1000;
    const int enc_rate = ffmpeg_out.audio_enc_ctx->sample_rate;
    int64_t abs_enc =
        g_segment_pts_offset + av_rescale(grid_rel_ms, enc_rate, 1000);
    int64_t utc_micro = capture_wall_us_at(abs_enc);
    int64_t utc_sec_i = utc_micro / 1000000;
    int64_t frac = utc_micro % 1000000;
    if (frac < 0) {
        frac += 1000000;
        utc_sec_i -= 1;
    }
    time_t utc_sec = (time_t)utc_sec_i;
    unsigned int usec = (unsigned int)frac;

    char srt_data[3072];
    int pos;
    if (g_tz_active) {
        struct tm tm_local;
        localtime_r(&utc_sec, &tm_local);
        char time_str[64];
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_local);
        long off = tm_local.tm_gmtoff;
        long aoff = off < 0 ? -off : off;
        pos = snprintf(srt_data, sizeof(srt_data), "%s.%06u %c%02ld:%02ld",
                       time_str, usec, off < 0 ? '-' : '+', aoff / 3600,
                       (aoff % 3600) / 60);
    } else {
        struct tm tm_utc;
        gmtime_r(&utc_sec, &tm_utc);
        char time_str[64];
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_utc);
        pos = snprintf(srt_data, sizeof(srt_data), "%s.%06u", time_str, usec);
    }
    if (pos < 0) pos = 0;

    if (g_meta_count > 0) {
        if (pthread_mutex_trylock(&g_meta_mutex) == 0) {
            for (int i = 0; i < g_meta_count; i++)
                memcpy(g_meta_pipes[i].cached, g_meta_pipes[i].shared,
                       strlen(g_meta_pipes[i].shared) + 1);
            pthread_mutex_unlock(&g_meta_mutex);
        }
    }

    {
        static char whisper_cached[META_LINE_BYTES] = "";
        if (pthread_mutex_trylock(&g_whisper_mutex) == 0) {
            if (g_whisper_result_ready) {
                memcpy(whisper_cached, g_whisper_result,
                       strlen(g_whisper_result) + 1);
                g_whisper_result_ready = 0;
            }
            pthread_mutex_unlock(&g_whisper_mutex);
        }

        if (whisper_cached[0] != '\0' && pos < (int)sizeof(srt_data) - 1) {
            int w = snprintf(srt_data + pos, sizeof(srt_data) - pos, "\nW:%s",
                             whisper_cached);
            if (w > 0) {
                pos += w;
                if (pos >= (int)sizeof(srt_data))
                    pos = (int)sizeof(srt_data) - 1;
            }
        }
    }

    if (g_ebur) {
        static char ebur_m_c[16] = "";
        static char ebur_s_c[16] = "";
        static char ebur_i_c[16] = "";
        static char ebur_lra_c[16] = "";
        if (pthread_mutex_trylock(&g_ebur_mutex) == 0) {
            if (g_ebur_m[0]) memcpy(ebur_m_c, g_ebur_m, sizeof(ebur_m_c));
            if (g_ebur_s[0]) memcpy(ebur_s_c, g_ebur_s, sizeof(ebur_s_c));
            if (g_ebur_i[0]) memcpy(ebur_i_c, g_ebur_i, sizeof(ebur_i_c));
            if (g_ebur_lra[0])
                memcpy(ebur_lra_c, g_ebur_lra, sizeof(ebur_lra_c));
            pthread_mutex_unlock(&g_ebur_mutex);
        }
        if ((ebur_m_c[0] || ebur_s_c[0] || ebur_i_c[0]) &&
            pos < (int)sizeof(srt_data) - 1) {
            int w = snprintf(srt_data + pos, sizeof(srt_data) - pos,
                             "\nEBU: M:%s S:%s I:%s LUFS LRA:%s LU",
                             ebur_m_c[0] ? ebur_m_c : "--",
                             ebur_s_c[0] ? ebur_s_c : "--",
                             ebur_i_c[0] ? ebur_i_c : "--",
                             ebur_lra_c[0] ? ebur_lra_c : "--");
            if (w > 0) {
                pos += w;
                if (pos >= (int)sizeof(srt_data))
                    pos = (int)sizeof(srt_data) - 1;
            }
        }
    }

    for (int i = 0; i < g_meta_count && pos < (int)sizeof(srt_data) - 1; i++) {
        int w = snprintf(srt_data + pos, sizeof(srt_data) - pos, "\nM%d:%s",
                         i + 1, g_meta_pipes[i].cached);
        if (w < 0) break;
        pos += w;
        if (pos >= (int)sizeof(srt_data)) {
            pos = (int)sizeof(srt_data) - 1;
            break;
        }
    }

    AVPacket* sub_pkt = av_packet_alloc();
    if (!sub_pkt) return;
    if (av_new_packet(sub_pkt, pos) < 0) {
        av_packet_free(&sub_pkt);
        return;
    }
    memcpy(sub_pkt->data, srt_data, (size_t)pos);
    sub_pkt->pts = sub_pkt->dts = start_ms;
    sub_pkt->duration = dur_ms;
    av_packet_rescale_ts(sub_pkt, (AVRational){1, 1000},
                         ffmpeg_out.sub_out_stream->time_base);
    sub_pkt->stream_index = ffmpeg_out.sub_out_stream->index;

    ret = av_interleaved_write_frame(ffmpeg_out.ofmt_ctx, sub_pkt);
    if (ret < 0) fprintf(stderr, "Error writing subtitle packet\n");
    av_packet_free(&sub_pkt);
}

/* STREAMING_CHUNK:Implementing packet muxing, segmentation, and format
 * transitions... */
/* Write one encoded audio packet, generate associated subtitles, and handle
   segmentation at packet boundaries. */
static void process_encoded_packet(AVPacket* pkt) {
    /* 1. Segment rollover: cut before writing the boundary-crossing packet so
       that packet becomes the first packet of the next file and nothing is
       dropped. */
    if (g_segment_time > 0.0 && pkt->pts != AV_NOPTS_VALUE) {
        if (g_segment_clock) {
            int64_t now_us = capture_wall_us_at(pkt->pts);
            if (now_us >= g_next_segment_clock_us) {
                close_output_file();
                g_segment_index++;
                g_segment_pts_offset = pkt->pts;
                /* Restart the subtitle grid so the new file's subtitles tile
                   cleanly from 0; otherwise the unaligned clock offset shifts
                   them off the SUB_INTERVAL_US grid and overlaps the first two
                   subtitles of the segment. */
                ffmpeg_out.next_sub_ts = 0;

                while (now_us >= g_next_segment_clock_us) {
                    g_next_segment_clock_us +=
                        (int64_t)(g_segment_time * 1000000.0);
                }

                g_next_file_wall_us = now_us;
                char fname[4096];
                if (build_output_filename(fname, sizeof(fname),
                                          g_segment_index) < 0) {
                    fprintf(stderr, "Failed to build segment filename\n");
                    exit(1);
                }
                open_output_file(fname);
            }
        } else {
            if (pkt->pts >= g_next_segment_pts) {
                close_output_file();
                g_segment_index++;
                g_segment_pts_offset = pkt->pts;
                while (pkt->pts >= g_next_segment_pts) {
                    g_next_segment_pts += g_segment_time_samples;
                }
                ffmpeg_out.next_sub_ts = 0;

                g_next_file_wall_us = capture_wall_us_at(pkt->pts);
                char fname[4096];
                if (build_output_filename(fname, sizeof(fname),
                                          g_segment_index) < 0) {
                    fprintf(stderr, "Failed to build segment filename\n");
                    exit(1);
                }
                open_output_file(fname);
            }
        }
    }

    /* Generate subtitles matching the encoded packet's position. */
    if (pkt->pts != AV_NOPTS_VALUE) {
        int64_t rel_pts = pkt->pts - g_segment_pts_offset;
        if (rel_pts < 0) rel_pts = 0;
        int64_t pkt_ms =
            av_rescale_q(rel_pts, ffmpeg_out.audio_enc_ctx->time_base,
                         (AVRational){1, 1000});
        while (pkt_ms >= ffmpeg_out.next_sub_ts) {
            generate_subtitle_packet(ffmpeg_out.next_sub_ts);
            ffmpeg_out.next_sub_ts += (SUB_INTERVAL_US / 1000);
        }
    }

    int64_t abs_pts = pkt->pts;
    if (pkt->pts != AV_NOPTS_VALUE) pkt->pts -= g_segment_pts_offset;
    if (pkt->dts != AV_NOPTS_VALUE) pkt->dts -= g_segment_pts_offset;

    pkt->stream_index = ffmpeg_out.audio_out_stream->index;
    av_packet_rescale_ts(pkt, ffmpeg_out.audio_enc_ctx->time_base,
                         ffmpeg_out.audio_out_stream->time_base);

    int ret = av_interleaved_write_frame(ffmpeg_out.ofmt_ctx, pkt);
    if (ret < 0) fprintf(stderr, "Error writing audio packet\n");

    if (g_fsync_ms > 0.0 && abs_pts != AV_NOPTS_VALUE &&
        abs_pts >= g_next_fsync_pts) {
        fsync_output_file();
        /* Advance to the next boundary strictly past this packet so a single
           packet never triggers more than one fsync, even for a tiny
           --fsync-ms, while keeping boundaries on the original grid. */
        int64_t steps =
            (abs_pts - g_next_fsync_pts) / g_fsync_interval_samples + 1;
        g_next_fsync_pts += steps * g_fsync_interval_samples;
    }
}

static AVFrame* alloc_audio_frame(int nb_samples) {
    if (nb_samples <= 0) return NULL;

    AVFrame* frame = av_frame_alloc();
    if (!frame) return NULL;

    frame->nb_samples = nb_samples;
    frame->format = ffmpeg_out.audio_enc_ctx->sample_fmt;
    frame->sample_rate = ffmpeg_out.audio_enc_ctx->sample_rate;

    if (av_channel_layout_copy(&frame->ch_layout,
                               &ffmpeg_out.audio_enc_ctx->ch_layout) < 0) {
        av_frame_free(&frame);
        return NULL;
    }

    if (av_frame_get_buffer(frame, 0) < 0) {
        av_frame_free(&frame);
        return NULL;
    }

    if (!frame->extended_data) {
        av_frame_free(&frame);
        return NULL;
    }

    return frame;
}

/* Discard TsRecord entries belonging to ring buffer indices strictly before
   min_start_index so skipped regions cannot pollute subsequent anchors.
   Caller holds g_mutex. */
static void advance_ts_ring_past_locked(size_t min_start_index) {
    size_t th = atomic_load_explicit(&g_ts_ring.head, memory_order_acquire);
    size_t tt = atomic_load_explicit(&g_ts_ring.tail, memory_order_relaxed);
    while (tt < th && (ssize_t)(g_ts_ring.recs[tt & TS_RING_MASK].start_index -
                                min_start_index) < 0) {
        tt++;
    }
    atomic_store_explicit(&g_ts_ring.tail, tt, memory_order_release);
}

/* Apply a pending capture-format change without losing buffered audio.
 * Caller holds g_mutex. */
static void apply_format_change_locked(float* chunk, size_t chunk_cap) {
    size_t h = atomic_load_explicit(&g_ring_buffer.head, memory_order_acquire);

    /* 1. old-format backlog -> old resampler -> g_pre_filter_fifo */
    size_t limit = g_has_format_skip ? g_format_skip_from : g_format_boundary;
    if ((ssize_t)(limit - h) > 0) limit = h;

    size_t t = atomic_load_explicit(&g_ring_buffer.tail, memory_order_relaxed);
    if (g_swr_ctx && g_capture_channels > 0 && g_capture_rate > 0) {
        while ((ssize_t)(limit - t) > 0) {
            size_t n = FFMIN(limit - t, chunk_cap);
            n -= n % (size_t)g_capture_channels;
            if (n == 0) break; /* sub-frame remainder: handled by step 2 */
            capture_anchor_update_locked(t);
            for (size_t i = 0; i < n; i++)
                chunk[i] = g_ring_buffer.data[(t + i) & RING_BUFFER_MASK];
            atomic_store_explicit(&g_ring_buffer.tail, t + n,
                                  memory_order_release);
            resample_to_staging_locked(chunk,
                                       (int)(n / (size_t)g_capture_channels));
            t += n;
        }
    }

    t = atomic_load_explicit(&g_ring_buffer.tail, memory_order_relaxed);
    if ((ssize_t)(limit - t) > 0) {
        /* Unprocessable old-format floats: no live resampler (init failed,
           or the previous format was never applied), or a sub-frame tail.
           Skip so the new format starts at a clean frame boundary and advance
           g_ts_ring past the skipped region. */
        atomic_store_explicit(&g_ring_buffer.tail, limit, memory_order_release);
        advance_ts_ring_past_locked(limit);
        g_ts_anchor_valid = 0; /* content gap -> must re-anchor */
        fprintf(stderr,
                "format change: dropped %zu old-format floats "
                "(no active resampler)\n",
                limit - t);
    }

    /* 3. superseded intermediate-format region */
    if (g_has_format_skip) {
        size_t target =
            (ssize_t)(g_format_boundary - h) > 0 ? h : g_format_boundary;
        t = atomic_load_explicit(&g_ring_buffer.tail, memory_order_relaxed);
        if ((ssize_t)(target - t) > 0) {
            atomic_store_explicit(&g_ring_buffer.tail, target,
                                  memory_order_release);
            advance_ts_ring_past_locked(target);
            g_ts_anchor_valid = 0;
            fprintf(stderr,
                    "format change: skipped %zu floats of a superseded "
                    "format\n",
                    target - t);
        }
        g_has_format_skip = 0;
        g_format_skip_from = 0;
    }

    /* 4. swap + rebuild (flushes the old swr's delay line first) */
    g_capture_rate = g_pending_rate;
    g_capture_channels = g_pending_channels;
    if (init_resampler_locked() < 0) {
        g_capture_rate = 0;
        g_capture_channels = 0;
    } else {
        fprintf(stderr,
                "capture format change applied: %d Hz, %d channels "
                "(buffered backlog drained losslessly)\n",
                g_capture_rate, g_capture_channels);
    }
    g_format_change_pending = 0;
}

/* STREAMING_CHUNK:Implementing encoder FIFO draining and anchor calculation...
 */
static void drain_fifo_to_encoder(int final) {
    if (!g_fifo) return;
    AVCodecContext* enc = ffmpeg_out.audio_enc_ctx;
    int frame_size = enc->frame_size;
    int fixed = (frame_size > 0) &&
                !(enc->codec->capabilities & AV_CODEC_CAP_VARIABLE_FRAME_SIZE);
    if (frame_size <= 0) frame_size = 4096;

    while (av_audio_fifo_size(g_fifo) >= frame_size ||
           (final && av_audio_fifo_size(g_fifo) > 0)) {
        int avail = av_audio_fifo_size(g_fifo);
        int want = FFMIN(frame_size, avail);

        AVFrame* frame = alloc_audio_frame(frame_size);
        if (!frame) {
            fprintf(stderr, "Could not allocate audio frame\n");
            break;
        }

        int got_read =
            av_audio_fifo_read(g_fifo, (void**)frame->extended_data, want);
        if (got_read < 0) {
            fprintf(stderr, "Error reading from audio FIFO\n");
            av_frame_free(&frame);
            break;
        }
        if (fixed && got_read < frame_size) {
            av_samples_set_silence(frame->extended_data, got_read,
                                   frame_size - got_read,
                                   enc->ch_layout.nb_channels, enc->sample_fmt);
            frame->nb_samples = frame_size;
        } else {
            frame->nb_samples = got_read;
        }

        frame->pts = ffmpeg_out.total_samples;
        ffmpeg_out.total_samples += frame->nb_samples;

        encode_frame(frame);
        av_frame_free(&frame);

        if (final && av_audio_fifo_size(g_fifo) == 0) break;
    }
}

static void encode_frame(AVFrame* frame) {
    int ret = avcodec_send_frame(ffmpeg_out.audio_enc_ctx, frame);
    if (ret < 0) {
        fprintf(stderr, "Error sending audio frame to encoder\n");
        return;
    }
    AVPacket* pkt = av_packet_alloc();
    if (!pkt) return;
    while ((ret = avcodec_receive_packet(ffmpeg_out.audio_enc_ctx, pkt)) >= 0) {
        process_encoded_packet(pkt);
        av_packet_unref(pkt);
    }
    av_packet_free(&pkt);
}

static void capture_anchor_update_locked(size_t tail) {
    size_t th = atomic_load_explicit(&g_ts_ring.head, memory_order_acquire);
    size_t tt = atomic_load_explicit(&g_ts_ring.tail, memory_order_relaxed);
    while (tt + 1 < th &&
           (ssize_t)(g_ts_ring.recs[(tt + 1) & TS_RING_MASK].start_index -
                     tail) <= 0)
        tt++;
    atomic_store_explicit(&g_ts_ring.tail, tt, memory_order_release);
    if (tt >= th) return;

    TsRecord r = g_ts_ring.recs[tt & TS_RING_MASK];
    int ch = g_capture_channels > 0 ? g_capture_channels : 1;
    int rate = g_capture_rate > 0 ? g_capture_rate : 1;
    int64_t off_frames = (int64_t)(ssize_t)(tail - r.start_index) / ch;
    int64_t W = r.cap_wall_us + av_rescale(off_frames, 1000000, rate);

    int64_t enc_rate = ffmpeg_out.audio_enc_ctx->sample_rate;
    int64_t swr_pending = g_swr_ctx ? swr_get_delay(g_swr_ctx, enc_rate) : 0;
    int64_t filter_in =
        atomic_load_explicit(&g_filter_in_samples, memory_order_relaxed);
    int64_t filter_out =
        atomic_load_explicit(&g_filter_out_samples, memory_order_relaxed);
    int64_t filter_buffered =
        (filter_graph && filter_in > filter_out) ? (filter_in - filter_out) : 0;
    int64_t staged =
        (g_pre_filter_fifo ? av_audio_fifo_size(g_pre_filter_fifo) : 0) +
        filter_buffered +
        (g_post_filter_fifo ? av_audio_fifo_size(g_post_filter_fifo) : 0);
    int64_t anchor_enc = g_fifo_write_pos + staged + swr_pending;

    if (g_enc_anchor_head != g_enc_anchor_tail) {
        EncTsAnchor* last =
            &g_enc_anchors[(g_enc_anchor_head - 1) & ENC_ANCHOR_RING_MASK];
        if (anchor_enc <= last->enc_pts) {
            last->enc_pts = anchor_enc;
            last->wall_us = W;
            g_ts_anchor_valid = 1;
            return;
        }
    }
    if (g_enc_anchor_head - g_enc_anchor_tail >= ENC_ANCHOR_RING_SIZE) {
        g_enc_anchor_tail++;
    }
    g_enc_anchors[g_enc_anchor_head & ENC_ANCHOR_RING_MASK].enc_pts =
        anchor_enc;
    g_enc_anchors[g_enc_anchor_head & ENC_ANCHOR_RING_MASK].wall_us = W;
    g_enc_anchor_head++;
    g_ts_anchor_valid = 1;
}

/* Convert interleaved float capture samples to encoder format and store in
 * g_fifo directly (when no filter graph) or g_pre_filter_fifo (when filtering).
 * Does NOT touch filter graph. Caller MUST hold g_mutex. */
static void resample_to_staging_locked(const float* interleaved,
                                       int in_frames) {
    if (!g_swr_ctx || g_capture_channels <= 0 || in_frames <= 0 ||
        g_capture_rate <= 0)
        return;

    AVCodecContext* enc = ffmpeg_out.audio_enc_ctx;

    int64_t delay = swr_get_delay(g_swr_ctx, g_capture_rate);
    int64_t raw_out = av_rescale_rnd(delay + in_frames, enc->sample_rate,
                                     g_capture_rate, AV_ROUND_UP);
    int out_frames = (int)FFMIN(raw_out, 65536);
    if (out_frames < 1) out_frames = 1;

    if (!g_worker_resampled_frame ||
        g_worker_resampled_frame_cap < out_frames) {
        av_frame_free(&g_worker_resampled_frame);
        g_worker_resampled_frame = alloc_audio_frame(out_frames);
        if (!g_worker_resampled_frame) {
            g_worker_resampled_frame_cap = 0;
            return;
        }
        g_worker_resampled_frame_cap = out_frames;
    }
    g_worker_resampled_frame->nb_samples = out_frames;

    const uint8_t* in_data[1] = {(const uint8_t*)interleaved};
    int got = swr_convert(g_swr_ctx, g_worker_resampled_frame->extended_data,
                          out_frames, in_data, in_frames);
    if (got <= 0) {
        if (got < 0) {
            char errbuf[255];
            av_strerror(got, errbuf, sizeof(errbuf));
            fprintf(stderr, "resample: swr_convert failed: %s\n", errbuf);
        }
        return;
    }

    g_worker_resampled_frame->nb_samples = got;
    if (!filter_graph) {
        av_audio_fifo_write(
            g_fifo, (void**)g_worker_resampled_frame->extended_data, got);
        g_fifo_write_pos += got;
    } else {
        av_audio_fifo_write(g_pre_filter_fifo,
                            (void**)g_worker_resampled_frame->extended_data,
                            got);
    }
}

/* STREAMING_CHUNK:Implementing filter graph feeding and audio worker thread...
 */
static void shutdown_drain_filter(void) {
    if (!filter_graph || g_filter_eof) return;
    if (av_buffersrc_add_frame_flags(buffersrc_ctx, NULL, 0) < 0)
        fprintf(stderr, "Error signalling EOF to the filter graph\n");
    g_filter_eof = 1;
    AVFrame* fout = av_frame_alloc();
    if (!fout) return;
    int r;
    while ((r = av_buffersink_get_frame(buffersink_ctx, fout)) >= 0) {
        av_audio_fifo_write(g_post_filter_fifo, (void**)fout->extended_data,
                            fout->nb_samples);
        atomic_fetch_add_explicit(&g_filter_out_samples, fout->nb_samples,
                                  memory_order_relaxed);
        extract_frame_metadata(fout);
        av_frame_unref(fout);
    }
    av_frame_free(&fout);
}

static void feed_and_drain_filter_graph(void) {
    if (!filter_graph || g_filter_eof) return;

    int available = av_audio_fifo_size(g_pre_filter_fifo);
    if (available <= 0) return;

    while (av_audio_fifo_size(g_pre_filter_fifo) > 0) {
        int chunk = FFMIN(av_audio_fifo_size(g_pre_filter_fifo), 4096);

        AVFrame* push_frame = alloc_audio_frame(chunk);
        if (!push_frame) break;

        int read = av_audio_fifo_read(g_pre_filter_fifo,
                                      (void**)push_frame->extended_data, chunk);
        if (read <= 0) {
            av_frame_free(&push_frame);
            break;
        }
        push_frame->nb_samples = read;
        push_frame->pts = g_filter_pts;
        g_filter_pts += read;

        if (av_buffersrc_add_frame_flags(buffersrc_ctx, push_frame,
                                         AV_BUFFERSRC_FLAG_KEEP_REF) < 0) {
            fprintf(stderr, "Error feeding filter graph\n");
            av_frame_free(&push_frame);
            break;
        }
        atomic_fetch_add_explicit(&g_filter_in_samples, read,
                                  memory_order_relaxed);
        av_frame_free(&push_frame);

        AVFrame* fout = av_frame_alloc();
        if (!fout) break;
        int r;
        while ((r = av_buffersink_get_frame(buffersink_ctx, fout)) >= 0) {
            av_audio_fifo_write(g_post_filter_fifo, (void**)fout->extended_data,
                                fout->nb_samples);
            atomic_fetch_add_explicit(&g_filter_out_samples, fout->nb_samples,
                                      memory_order_relaxed);
            extract_frame_metadata(fout);
            av_frame_unref(fout);
        }
        av_frame_free(&fout);
    }
}

/* Worker thread */
static void* audio_worker_thread(void* arg) {
    (void)arg;
    float chunk[4096];

    while (!atomic_load_explicit(&worker_quit_flag, memory_order_acquire)) {
        size_t to_read = 0;

        pthread_mutex_lock(&g_mutex);

        if (g_format_change_pending)
            apply_format_change_locked(chunk, sizeof(chunk) / sizeof(chunk[0]));

        size_t head =
            atomic_load_explicit(&g_ring_buffer.head, memory_order_acquire);
        size_t tail =
            atomic_load_explicit(&g_ring_buffer.tail, memory_order_relaxed);
        size_t available = head - tail;
        to_read = available > 4096 ? 4096 : available;

        int ch = g_capture_channels;
        if (ch > 0)
            to_read -= to_read % (size_t)ch;
        else
            to_read = 0;

        if (to_read > 0) {
            capture_anchor_update_locked(tail);
            for (size_t i = 0; i < to_read; i++)
                chunk[i] = g_ring_buffer.data[(tail + i) & RING_BUFFER_MASK];
            atomic_store_explicit(&g_ring_buffer.tail, tail + to_read,
                                  memory_order_release);
            int in_frames = (int)(to_read / (size_t)ch);
            resample_to_staging_locked(chunk, in_frames);
        }

        if (!filter_graph) {
            int staged =
                g_pre_filter_fifo ? av_audio_fifo_size(g_pre_filter_fifo) : 0;
            if (staged > 0) {
                AVFrame* tmp = alloc_audio_frame(staged);
                if (tmp) {
                    int rd = av_audio_fifo_read(
                        g_pre_filter_fifo, (void**)tmp->extended_data, staged);
                    if (rd > 0) {
                        av_audio_fifo_write(g_fifo, (void**)tmp->extended_data,
                                            rd);
                        g_fifo_write_pos += rd;
                    }
                    av_frame_free(&tmp);
                }
            }
            drain_fifo_to_encoder(0);
        }

        pthread_mutex_unlock(&g_mutex);

        if (filter_graph) {
            feed_and_drain_filter_graph();

            pthread_mutex_lock(&g_mutex);
            int staged =
                g_post_filter_fifo ? av_audio_fifo_size(g_post_filter_fifo) : 0;
            if (staged > 0) {
                AVFrame* tmp = alloc_audio_frame(staged);
                if (tmp) {
                    int rd = av_audio_fifo_read(
                        g_post_filter_fifo, (void**)tmp->extended_data, staged);
                    if (rd > 0) {
                        av_audio_fifo_write(g_fifo, (void**)tmp->extended_data,
                                            rd);
                        g_fifo_write_pos += rd;
                    }
                    av_frame_free(&tmp);
                }
            }
            drain_fifo_to_encoder(0);
            pthread_mutex_unlock(&g_mutex);
        }

        if (to_read == 0) {
            if (atomic_load_explicit(&worker_quit_flag, memory_order_acquire))
                break;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 2000000;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec += 1;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_mutex_lock(&g_worker_mutex);
            pthread_cond_timedwait(&g_worker_cond, &g_worker_mutex, &ts);
            pthread_mutex_unlock(&g_worker_mutex);
        }
    }

    /* ── Shutdown drain ── */
    pthread_mutex_lock(&g_mutex);

    if (g_format_change_pending)
        apply_format_change_locked(chunk, sizeof(chunk) / sizeof(chunk[0]));

    size_t head =
        atomic_load_explicit(&g_ring_buffer.head, memory_order_acquire);
    size_t tail =
        atomic_load_explicit(&g_ring_buffer.tail, memory_order_relaxed);
    size_t available = head - tail;
    while (available > 0) {
        int ch = g_capture_channels;
        if (ch <= 0) break; /* no negotiated format: nothing more to frame */
        size_t to_read = available > 4096 ? 4096 : available;
        to_read -= to_read % (size_t)ch;
        if (to_read == 0) break;
        capture_anchor_update_locked(tail);
        for (size_t i = 0; i < to_read; i++)
            chunk[i] = g_ring_buffer.data[(tail + i) & RING_BUFFER_MASK];
        atomic_store_explicit(&g_ring_buffer.tail, tail + to_read,
                              memory_order_release);
        int in_frames = (int)(to_read / (size_t)ch);
        resample_to_staging_locked(chunk, in_frames);
        tail += to_read;
        available -= to_read;
    }

    /* Flush internal delay out of swr (loops until exhausted). */
    flush_swr_to_staging_locked();
    pthread_mutex_unlock(&g_mutex);

    if (filter_graph) {
        feed_and_drain_filter_graph();
        shutdown_drain_filter();

        pthread_mutex_lock(&g_mutex);
        int staged =
            g_post_filter_fifo ? av_audio_fifo_size(g_post_filter_fifo) : 0;
        if (staged > 0) {
            AVFrame* tmp = alloc_audio_frame(staged);
            if (tmp) {
                int rd = av_audio_fifo_read(g_post_filter_fifo,
                                            (void**)tmp->extended_data, staged);
                if (rd > 0) {
                    av_audio_fifo_write(g_fifo, (void**)tmp->extended_data, rd);
                    g_fifo_write_pos += rd;
                }
                av_frame_free(&tmp);
            }
        }
        drain_fifo_to_encoder(1);
        pthread_mutex_unlock(&g_mutex);
    } else {
        pthread_mutex_lock(&g_mutex);
        int staged =
            g_pre_filter_fifo ? av_audio_fifo_size(g_pre_filter_fifo) : 0;
        if (staged > 0) {
            AVFrame* tmp = alloc_audio_frame(staged);
            if (tmp) {
                int rd = av_audio_fifo_read(g_pre_filter_fifo,
                                            (void**)tmp->extended_data, staged);
                if (rd > 0) {
                    av_audio_fifo_write(g_fifo, (void**)tmp->extended_data, rd);
                    g_fifo_write_pos += rd;
                }
                av_frame_free(&tmp);
            }
        }
        drain_fifo_to_encoder(1);
        pthread_mutex_unlock(&g_mutex);
    }

    return NULL;
}

/* STREAMING_CHUNK:Implementing encoder and resampler initialization... */
static int select_channel_layout(const AVCodec* codec, AVChannelLayout* dst) {
    const AVChannelLayout* ch_layouts = NULL;
    int num_ch_layouts = 0;
    int ret = avcodec_get_supported_config(
        NULL, codec, AV_CODEC_CONFIG_CHANNEL_LAYOUT, 0,
        (const void**)&ch_layouts, &num_ch_layouts);
    if (ret >= 0 && ch_layouts && num_ch_layouts > 0) {
        const AVChannelLayout* best = &ch_layouts[0];
        return av_channel_layout_copy(dst, best);
    }
    AVChannelLayout stereo;
    av_channel_layout_default(&stereo, 2);
    ret = av_channel_layout_copy(dst, &stereo);
    av_channel_layout_uninit(&stereo);
    return ret;
}

static void flush_audio_encoder(void) {
    int ret = avcodec_send_frame(ffmpeg_out.audio_enc_ctx, NULL);
    if (ret < 0 && ret != AVERROR_EOF) {
        fprintf(stderr, "Error flushing audio encoder\n");
        return;
    }
    AVPacket* pkt = av_packet_alloc();
    if (!pkt) return;
    while ((ret = avcodec_receive_packet(ffmpeg_out.audio_enc_ctx, pkt)) >= 0) {
        process_encoded_packet(pkt);
        av_packet_unref(pkt);
    }
    av_packet_free(&pkt);
}

static int init_resampler_locked(void) {
    AVCodecContext* enc = ffmpeg_out.audio_enc_ctx;

    flush_swr_to_staging_locked();
    if (g_swr_ctx) {
        swr_free(&g_swr_ctx);
    }
    av_channel_layout_uninit(&g_capture_ch_layout);
    av_channel_layout_default(&g_capture_ch_layout, g_capture_channels);

    int ret = swr_alloc_set_opts2(&g_swr_ctx, &enc->ch_layout, enc->sample_fmt,
                                  enc->sample_rate, &g_capture_ch_layout,
                                  AV_SAMPLE_FMT_FLT, g_capture_rate, 0, NULL);
    if (ret < 0 || (ret = swr_init(g_swr_ctx)) < 0) {
        char errbuf[128] = {0};
        av_strerror(ret, errbuf, sizeof(errbuf));
        fprintf(stderr, "Failed to initialise resampler: %s\n", errbuf);
        if (g_swr_ctx) swr_free(&g_swr_ctx);
        av_channel_layout_uninit(&g_capture_ch_layout);
        return -1;
    }

    return 0;
}

/*
   Whitelist validation of the segment filename template (non-strftime mode):
   every '%' must be either "%%" or one plain printf integer conversion --
   optional flags [-+ 0#] and a small width, conversion d/i/u/x/X/o, no length
   modifiers -- and exactly one such conversion must be present (it receives
   the int segment index). */
static int validate_segment_template(const char* t) {
    int conversions = 0;
    for (const char* p = t; *p; p++) {
        if (*p != '%') continue;
        p++;
        if (*p == '%') continue;
        while (*p && strchr("-+ 0#", *p)) p++;
        int width = 0;
        while (*p >= '0' && *p <= '9') {
            width = width * 10 + (*p - '0');
            if (width > 64) return -1;
            p++;
        }
        if (!*p || !strchr("diuxXo", *p)) return -1;
        conversions++;
    }
    return conversions == 1 ? 0 : -1;
}

static int append_collision_suffix(char* buf, size_t sz, int seq) {
    char tmp[4096];
    int n = snprintf(tmp, sizeof(tmp), "%s", buf);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    char* slash = strrchr(tmp, '/');
    char* dot = strrchr(slash ? slash + 1 : tmp, '.');
    if (dot)
        n = snprintf(buf, sz, "%.*s-%d%s", (int)(dot - tmp), tmp, seq, dot);
    else
        n = snprintf(buf, sz, "%s-%d", tmp, seq);
    return (n < 0 || (size_t)n >= sz) ? -1 : 0;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#pragma GCC diagnostic ignored "-Wformat-security"
static int build_output_filename(char* buf, size_t sz, int index) {
    if (g_use_strftime) {
        time_t now = g_next_file_wall_us > 0
                         ? (time_t)(g_next_file_wall_us / 1000000)
                         : time(NULL);
        struct tm tm_local;
        localtime_r(&now, &tm_local);
        if (strftime(buf, sz, g_output_template, &tm_local) == 0) {
            fprintf(stderr, "strftime: output filename too long or empty\n");
            return -1;
        }
    } else if (g_segment_time > 0.0) {
        int n = snprintf(buf, sz, g_output_template, index);
        if (n < 0 || (size_t)n >= sz) {
            fprintf(stderr, "Output filename does not fit: %s\n",
                    g_output_template);
            return -1;
        }
    } else {
        int n = snprintf(buf, sz, "%s", g_output_template);
        if (n < 0 || (size_t)n >= sz) {
            fprintf(stderr, "Output filename does not fit: %s\n",
                    g_output_template);
            return -1;
        }
    }

    if (strcmp(buf, g_last_expanded) == 0) {
        g_collision_seq++;
        if (append_collision_suffix(buf, sz, g_collision_seq) < 0) {
            fprintf(stderr,
                    "Output filename collision suffix does not fit: %s\n", buf);
            return -1;
        }
    } else {
        snprintf(g_last_expanded, sizeof(g_last_expanded), "%s", buf);
        g_collision_seq = 0;
    }
    return 0;
}
#pragma GCC diagnostic pop

static void init_ffmpeg_encoder(const char* audio_encoder_name,
                                const char* audio_filter_str,
                                int64_t audio_bitrate) {
    int ret;
    const AVCodec* audio_encoder = NULL;

    if (audio_encoder_name)
        audio_encoder = avcodec_find_encoder_by_name(audio_encoder_name);
    else
        audio_encoder = avcodec_find_encoder(AV_CODEC_ID_OPUS);
    if (!audio_encoder) {
        fprintf(stderr, "Necessary audio encoder not found\n");
        exit(1);
    }
    ffmpeg_out.audio_enc_ctx = avcodec_alloc_context3(audio_encoder);
    if (!ffmpeg_out.audio_enc_ctx) {
        fprintf(stderr, "Could not allocate audio encoder context\n");
        exit(1);
    }
    ffmpeg_out.audio_enc_ctx->sample_rate = select_sample_rate(audio_encoder);

    {
        const void* supported_sample_fmt = NULL;
        int num_sample_fmts = 0;
        ret = avcodec_get_supported_config(
            ffmpeg_out.audio_enc_ctx, audio_encoder,
            AV_CODEC_CONFIG_SAMPLE_FORMAT, 0, &supported_sample_fmt,
            &num_sample_fmts);
        if (ret >= 0 && num_sample_fmts > 0 && supported_sample_fmt) {
            const enum AVSampleFormat* sample_fmts = supported_sample_fmt;
            ffmpeg_out.audio_enc_ctx->sample_fmt = sample_fmts[0];
        } else {
            ffmpeg_out.audio_enc_ctx->sample_fmt = AV_SAMPLE_FMT_FLT;
        }
    }

    if (g_out_channels > 0) {
        AVChannelLayout want = {0};
        const AVChannelLayout* ch_layouts = NULL;
        int num_ch_layouts = 0;
        int sret = avcodec_get_supported_config(
            ffmpeg_out.audio_enc_ctx, audio_encoder,
            AV_CODEC_CONFIG_CHANNEL_LAYOUT, 0, (const void**)&ch_layouts,
            &num_ch_layouts);
        if (sret >= 0 && ch_layouts && num_ch_layouts > 0) {
            const AVChannelLayout* match = NULL;
            for (int i = 0; i < num_ch_layouts; i++) {
                if (ch_layouts[i].nb_channels == g_out_channels) {
                    match = &ch_layouts[i];
                    break;
                }
            }
            if (!match) {
                fprintf(stderr,
                        "Encoder '%s' does not support %d output channel(s) "
                        "requested via --ac\n",
                        audio_encoder->name, g_out_channels);
                exit(1);
            }
            ret = av_channel_layout_copy(&want, match);
            FF_CHECK(ret, "Could not copy channel layout");
        } else {
            av_channel_layout_default(&want, g_out_channels);
        }
        ret =
            av_channel_layout_copy(&ffmpeg_out.audio_enc_ctx->ch_layout, &want);
        av_channel_layout_uninit(&want);
        FF_CHECK(ret, "Could not set output channel layout");
    } else {
        ret = select_channel_layout(audio_encoder,
                                    &ffmpeg_out.audio_enc_ctx->ch_layout);
        FF_CHECK(ret, "Could not select channel layout");
    }

    ffmpeg_out.channels = ffmpeg_out.audio_enc_ctx->ch_layout.nb_channels;
    ffmpeg_out.sample_rate = ffmpeg_out.audio_enc_ctx->sample_rate;

    ffmpeg_out.audio_enc_ctx->time_base =
        (AVRational){1, ffmpeg_out.audio_enc_ctx->sample_rate};
    ffmpeg_out.audio_enc_ctx->bit_rate = audio_bitrate;

    /* Guess output container format from the output template (defaulting to
       setting AV_CODEC_FLAG_GLOBAL_HEADER if unknown or if AVFMT_GLOBALHEADER
       is set) so encoders like native AAC populate extradata during
       avcodec_open2(). */
    const AVOutputFormat* guessed_oformat =
        g_output_template ? av_guess_format(NULL, g_output_template, NULL)
                          : NULL;
    if (!guessed_oformat || (guessed_oformat->flags & AVFMT_GLOBALHEADER)) {
        ffmpeg_out.audio_enc_ctx->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;
    }

    ret = avcodec_open2(ffmpeg_out.audio_enc_ctx, audio_encoder, NULL);
    FF_CHECK(ret, "Cannot open audio encoder");

    {
        char enc_layout_desc[64];
        av_channel_layout_describe(&ffmpeg_out.audio_enc_ctx->ch_layout,
                                   enc_layout_desc, sizeof(enc_layout_desc));
        fprintf(stdout,
                "Output encoder format: codec:%s rate:%d channels:%d "
                "layout:%s fmt:%s bitrate:%" PRId64 "\n",
                audio_encoder->name, ffmpeg_out.audio_enc_ctx->sample_rate,
                ffmpeg_out.audio_enc_ctx->ch_layout.nb_channels,
                enc_layout_desc,
                av_get_sample_fmt_name(ffmpeg_out.audio_enc_ctx->sample_fmt),
                ffmpeg_out.audio_enc_ctx->bit_rate);
    }

    if (audio_filter_str || g_ebur) {
        char args[512];
        char layout_desc[64];
        filter_graph = avfilter_graph_alloc();
        if (!filter_graph) {
            fprintf(stderr, "Unable to create filter graph\n");
            exit(1);
        }
        const AVFilter* abuffersrc = avfilter_get_by_name("abuffer");
        const AVFilter* abuffersink = avfilter_get_by_name("abuffersink");

        av_channel_layout_describe(&ffmpeg_out.audio_enc_ctx->ch_layout,
                                   layout_desc, sizeof(layout_desc));
        int an = snprintf(
            args, sizeof(args),
            "time_base=%d/%d:sample_rate=%d:sample_fmt=%s:channel_layout=%s",
            ffmpeg_out.audio_enc_ctx->time_base.num,
            ffmpeg_out.audio_enc_ctx->time_base.den,
            ffmpeg_out.audio_enc_ctx->sample_rate,
            av_get_sample_fmt_name(ffmpeg_out.audio_enc_ctx->sample_fmt),
            layout_desc);
        if (an < 0 || (size_t)an >= sizeof(args)) {
            fprintf(stderr, "Audio buffer source arguments do not fit\n");
            exit(1);
        }
        ret = avfilter_graph_create_filter(&buffersrc_ctx, abuffersrc, "in",
                                           args, NULL, filter_graph);
        FF_CHECK(ret, "Cannot create audio buffer source");
        ret = avfilter_graph_create_filter(&buffersink_ctx, abuffersink, "out",
                                           NULL, NULL, filter_graph);
        FF_CHECK(ret, "Cannot create audio buffer sink");
        AVFilterInOut* outputs = avfilter_inout_alloc();
        AVFilterInOut* inputs = avfilter_inout_alloc();
        if (!outputs || !inputs) {
            fprintf(stderr, "Failed to allocate filter graph inputs/outputs\n");
            exit(1);
        }
        outputs->name = av_strdup("in");
        outputs->filter_ctx = buffersrc_ctx;
        outputs->pad_idx = 0;
        outputs->next = NULL;
        inputs->name = av_strdup("out");
        inputs->filter_ctx = buffersink_ctx;
        inputs->pad_idx = 0;
        inputs->next = NULL;

        char full_filter[1024];
        int fn;
        if (audio_filter_str && g_ebur) {
            fn = snprintf(
                full_filter, sizeof(full_filter),
                "%s,ebur128=metadata=1,aformat=sample_fmts=%s:sample_rates=%d:"
                "channel_layouts=%s",
                audio_filter_str,
                av_get_sample_fmt_name(ffmpeg_out.audio_enc_ctx->sample_fmt),
                ffmpeg_out.audio_enc_ctx->sample_rate, layout_desc);
        } else if (audio_filter_str) {
            fn = snprintf(
                full_filter, sizeof(full_filter),
                "%s,aformat=sample_fmts=%s:sample_rates=%d:channel_layouts=%s",
                audio_filter_str,
                av_get_sample_fmt_name(ffmpeg_out.audio_enc_ctx->sample_fmt),
                ffmpeg_out.audio_enc_ctx->sample_rate, layout_desc);
        } else {
            fn = snprintf(
                full_filter, sizeof(full_filter),
                "ebur128=metadata=1,aformat=sample_fmts=%s:sample_rates=%d:"
                "channel_layouts=%s",
                av_get_sample_fmt_name(ffmpeg_out.audio_enc_ctx->sample_fmt),
                ffmpeg_out.audio_enc_ctx->sample_rate, layout_desc);
        }
        if (fn < 0 || (size_t)fn >= sizeof(full_filter)) {
            fprintf(stderr,
                    "--af filter chain is too long (limit is %zu bytes "
                    "including the appended aformat)\n",
                    sizeof(full_filter) - 1);
            exit(1);
        }

        ret = avfilter_graph_parse_ptr(filter_graph, full_filter, &inputs,
                                       &outputs, NULL);
        FF_CHECK(ret, "Error parsing filter graph");
        ret = avfilter_graph_config(filter_graph, NULL);
        FF_CHECK(ret, "Error configuring filter graph");
        if (inputs) avfilter_inout_free(&inputs);
        if (outputs) avfilter_inout_free(&outputs);
    }

    AVCodecContext* enc = ffmpeg_out.audio_enc_ctx;
    int initial_fifo_samples =
        enc->frame_size > 0 ? FFMAX(enc->frame_size * 8, 8192) : 8192;

    if (!g_fifo) {
        g_fifo = av_audio_fifo_alloc(
            enc->sample_fmt, enc->ch_layout.nb_channels, initial_fifo_samples);
        if (!g_fifo) {
            fprintf(stderr, "Failed to allocate audio FIFO\n");
            exit(1);
        }
    }
    if (!g_pre_filter_fifo) {
        g_pre_filter_fifo = av_audio_fifo_alloc(
            enc->sample_fmt, enc->ch_layout.nb_channels, initial_fifo_samples);
        if (!g_pre_filter_fifo) {
            fprintf(stderr, "Failed to allocate pre-filter staging FIFO\n");
            exit(1);
        }
    }
    if (filter_graph && !g_post_filter_fifo) {
        g_post_filter_fifo = av_audio_fifo_alloc(
            enc->sample_fmt, enc->ch_layout.nb_channels, initial_fifo_samples);
        if (!g_post_filter_fifo) {
            fprintf(stderr, "Failed to allocate post-filter staging FIFO\n");
            exit(1);
        }
    }
}

/* STREAMING_CHUNK:Implementing segment file management and async fsync... */
static void track_segment_file(const char* filename) {
    if (g_keep_segments <= 0) return;

    if (g_kept_count == g_kept_cap) {
        int newcap = g_kept_cap ? g_kept_cap * 2 : (g_keep_segments + 1);
        char** p = realloc(g_kept_files, (size_t)newcap * sizeof(char*));
        if (!p) return;
        g_kept_files = p;
        g_kept_cap = newcap;
    }
    char* dup = strdup(filename);
    if (!dup) return;
    g_kept_files[g_kept_count++] = dup;

    while (g_kept_count > g_keep_segments) {
        char* oldest = g_kept_files[0];
        int in_use = (strcmp(oldest, g_current_filename) == 0);
        for (int i = 1; !in_use && i < g_kept_count; i++)
            if (strcmp(oldest, g_kept_files[i]) == 0) in_use = 1;
        if (in_use)
            fprintf(stderr,
                    "keep-segments: not deleting %s (name still in use)\n",
                    oldest);
        else if (unlink(oldest) == 0)
            fprintf(stderr, "keep-segments: deleted old segment %s\n", oldest);
        else
            fprintf(stderr, "keep-segments: could not delete %s: %s\n", oldest,
                    strerror(errno));
        free(oldest);
        memmove(&g_kept_files[0], &g_kept_files[1],
                (size_t)(g_kept_count - 1) * sizeof(char*));
        g_kept_count--;
    }
}

static void open_output_file(const char* filename) {
    int ret;
    AVDictionary* opts = NULL;

    snprintf(g_current_filename, sizeof(g_current_filename), "%s", filename);

    ret = avformat_alloc_output_context2(&ffmpeg_out.ofmt_ctx, NULL, NULL,
                                         filename);
    FF_CHECK(ret, "Could not create output context");

    ffmpeg_out.audio_out_stream =
        avformat_new_stream(ffmpeg_out.ofmt_ctx, NULL);
    if (!ffmpeg_out.audio_out_stream) {
        fprintf(stderr, "Failed allocating output audio stream\n");
        exit(1);
    }
    ret = avcodec_parameters_from_context(ffmpeg_out.audio_out_stream->codecpar,
                                          ffmpeg_out.audio_enc_ctx);
    FF_CHECK(ret, "Failed to copy encoder parameters to output audio stream");

    /* Only the very first file carries the encoder's start-up padding (Opus
       pre-skip, written as Matroska CodecDelay). */
    if (g_segment_index > 0) {
        AVCodecParameters* par = ffmpeg_out.audio_out_stream->codecpar;
        par->initial_padding = 0;
        /* Opus additionally carries the pre-skip inside its OpusHead
           extradata, and the decoder honours that even when CodecDelay is 0,
           so clear it there too. */
        if (par->codec_id == AV_CODEC_ID_OPUS && par->extradata &&
            par->extradata_size >= 12 &&
            memcmp(par->extradata, "OpusHead", 8) == 0) {
            par->extradata[10] = 0;
            par->extradata[11] = 0;
        }
    }

    if (g_title)
        av_dict_set(&ffmpeg_out.ofmt_ctx->metadata, "title", g_title, 0);
    if (g_track_comment)
        av_dict_set(&ffmpeg_out.audio_out_stream->metadata, "comment",
                    g_track_comment, 0);

    ffmpeg_out.sub_out_stream = avformat_new_stream(ffmpeg_out.ofmt_ctx, NULL);
    if (!ffmpeg_out.sub_out_stream) {
        fprintf(stderr, "Failed allocating output subtitle stream\n");
        exit(1);
    }
    ffmpeg_out.sub_out_stream->codecpar->codec_type = AVMEDIA_TYPE_SUBTITLE;
    ffmpeg_out.sub_out_stream->codecpar->codec_id = AV_CODEC_ID_SUBRIP;
    ffmpeg_out.sub_out_stream->time_base = (AVRational){1, 1000};
    /* Mark subtitle stream as discard so that the muxer does not expect a
     * subtitle encoder */
    ffmpeg_out.sub_out_stream->discard = AVDISCARD_ALL;

    if (!(ffmpeg_out.ofmt_ctx->oformat->flags & AVFMT_NOFILE)) {
        /* ffmpeg does not use O_EXCL or O_NOFOLLOW, and without extra cflags
         * such as "_GNU_SOURCE" it doesn't use O_CLOEXEC either; but later it
         * calls fcntl with FD_CLOEXEC . */
        ret = avio_open(&ffmpeg_out.ofmt_ctx->pb, filename, AVIO_FLAG_WRITE);
        FF_CHECK(ret, "Could not open output file");

        if (g_fsync_ms > 0.0) {
            g_out_fd = open(filename, O_RDONLY | O_CLOEXEC);
            if (g_out_fd < 0)
                fprintf(stderr,
                        "Warning: could not open '%s' for fsync(); durability "
                        "flushes disabled for this file\n",
                        filename);
        }
    }
    av_dict_set(&opts, "live", "1", 0);
    ret = avformat_write_header(ffmpeg_out.ofmt_ctx, &opts);
    FF_CHECK(ret, "Error occurred when writing header");
    av_dict_free(&opts);

    if (g_segment_time > 0.0)
        fprintf(stderr, "Opened segment #%d: %s\n", g_segment_index, filename);
    else
        fprintf(stderr, "Recording to: %s\n", filename);

    {
        char st[4200];
        if (g_segment_time > 0.0)
            snprintf(st, sizeof(st), "Writing segment #%d: %s", g_segment_index,
                     filename);
        else
            snprintf(st, sizeof(st), "Recording to %s", filename);
        sd_notify_status(st);
    }

    track_segment_file(filename);
}

static void* fsync_thread_main(void* arg) {
    (void)arg;
    pthread_mutex_lock(&g_fsync_mutex);
    for (;;) {
        while (g_fsync_final_count == 0 && g_fsync_periodic_fd < 0 &&
               !g_fsync_quit)
            pthread_cond_wait(&g_fsync_cond, &g_fsync_mutex);
        int fd = -1;
        if (g_fsync_final_count > 0) {
            fd = g_fsync_final_fds[0];
            memmove(&g_fsync_final_fds[0], &g_fsync_final_fds[1],
                    (size_t)(g_fsync_final_count - 1) * sizeof(int));
            g_fsync_final_count--;
        } else if (g_fsync_periodic_fd >= 0) {
            fd = g_fsync_periodic_fd;
            g_fsync_periodic_fd = -1;
        }
        if (fd >= 0) {
            pthread_mutex_unlock(&g_fsync_mutex);
            fsync(fd);
            close(fd);
            pthread_mutex_lock(&g_fsync_mutex);
            continue;
        }
        if (g_fsync_quit) break;
    }
    pthread_mutex_unlock(&g_fsync_mutex);
    return NULL;
}

static int fsync_thread_start(void) {
    if (pthread_create(&g_fsync_thread, NULL, fsync_thread_main, NULL) != 0)
        return -1;
    g_fsync_thread_started = 1;
    return 0;
}

static void fsync_thread_stop(void) {
    if (!g_fsync_thread_started) return;
    pthread_mutex_lock(&g_fsync_mutex);
    g_fsync_quit = 1;
    pthread_cond_signal(&g_fsync_cond);
    pthread_mutex_unlock(&g_fsync_mutex);
    pthread_join(g_fsync_thread, NULL);
    g_fsync_thread_started = 0;
}

static void fsync_enqueue_periodic(int fd) {
    if (fd < 0) return;
    if (!g_fsync_thread_started) {
        fsync(fd);
        close(fd);
        return;
    }
    pthread_mutex_lock(&g_fsync_mutex);
    if (g_fsync_periodic_fd >= 0) close(g_fsync_periodic_fd);
    g_fsync_periodic_fd = fd;
    pthread_cond_signal(&g_fsync_cond);
    pthread_mutex_unlock(&g_fsync_mutex);
}

static void fsync_enqueue_final(int fd) {
    if (fd < 0) return;
    if (g_fsync_thread_started) {
        pthread_mutex_lock(&g_fsync_mutex);
        if (g_fsync_final_count <
            (int)(sizeof(g_fsync_final_fds) / sizeof(g_fsync_final_fds[0]))) {
            g_fsync_final_fds[g_fsync_final_count++] = fd;
            pthread_cond_signal(&g_fsync_cond);
            pthread_mutex_unlock(&g_fsync_mutex);
            return;
        }
        pthread_mutex_unlock(&g_fsync_mutex);
    }
    fsync(fd);
    close(fd);
}

static void fsync_output_file(void) {
    if (!ffmpeg_out.ofmt_ctx) return;

    av_interleaved_write_frame(ffmpeg_out.ofmt_ctx, NULL);
    av_write_frame(ffmpeg_out.ofmt_ctx, NULL);

    if (ffmpeg_out.ofmt_ctx->pb) avio_flush(ffmpeg_out.ofmt_ctx->pb);
    if (g_out_fd >= 0) {
        int d = dup(g_out_fd);
        if (d >= 0)
            fsync_enqueue_periodic(d);
        else
            fsync(g_out_fd);
    }
}

static void close_output_file(void) {
    if (!ffmpeg_out.ofmt_ctx) return;
    av_write_trailer(ffmpeg_out.ofmt_ctx);
    if (g_fsync_ms > 0.0 && ffmpeg_out.ofmt_ctx->pb)
        avio_flush(ffmpeg_out.ofmt_ctx->pb);
    if (g_out_fd >= 0) {
        fsync_enqueue_final(g_out_fd);
        g_out_fd = -1;
    }
    if (!(ffmpeg_out.ofmt_ctx->oformat->flags & AVFMT_NOFILE))
        avio_closep(&ffmpeg_out.ofmt_ctx->pb);
    avformat_free_context(ffmpeg_out.ofmt_ctx);
    ffmpeg_out.ofmt_ctx = NULL;
    ffmpeg_out.audio_out_stream = NULL;
    ffmpeg_out.sub_out_stream = NULL;
}

/* STREAMING_CHUNK:Implementing PipeWire target watcher... */
#define PW_WATCHDOG_MS 2000  /* no-data tolerance once we were streaming */
#define PW_FIRSTLINK_MS 3000 /* grace for the very first link            */
#define PW_RETRY_MIN_MS 500  /* min spacing between reconnect attempts   */

struct pw_data {
    struct pw_main_loop* mainloop;
    struct pw_loop* l;
    struct pw_stream* stream;
    struct spa_audio_info_raw format;
    const char* target;
    int need_reconnect;
    int stopping;
    int ever_streamed;          /* reached STREAMING at least once        */
    atomic_llong last_buf_us;   /* RT heartbeat, read by the watchdog     */
    int64_t settle_deadline_us; /* when a suspect state turns fatal       */
    int force_reconnect_now;    // set by the watcher, consumed by the reconnect
                                // block
    char linked_serial[32];     // serial used when creating the current stream
};

#define WATCHER_IDLE_RETRY_MS 5000
#define WATCHER_REBUILD_MS 2000

struct watched_node {
    uint32_t id;        /* node global id                              */
    uint32_t device_id; /* PW_KEY_DEVICE_ID of backing device; 0=virtual */
    uint32_t serial;    /* object.serial (zombie-proof link target)     */
    int is_sink;        /* 1 if media.class starts with Audio/Sink      */
};

struct target_watcher {
    struct pw_data* data;
    struct pw_loop* loop;
    int enabled;
    int ready;
    int was_lost;
    struct watched_node nodes[32];
    int n_nodes;
    uint32_t dev_ids[64]; /* live Device globals */
    int n_devs;
    int64_t rebuild_at_us;
    char target[256];
    char resolved_serial[32]; /* object.serial of newest HEALTHY node */
    struct pw_context* ctx;
    struct pw_core* core;
    struct pw_registry* reg;
    struct spa_hook core_hook;
    struct spa_hook reg_hook;
};
static struct target_watcher g_watcher = {0};

static int watcher_dev_present(const struct target_watcher* w, uint32_t dev) {
    for (int i = 0; i < w->n_devs; i++)
        if (w->dev_ids[i] == dev) return 1;
    return 0;
}

/* Newest matching node that is backed by a live device (device_id == 0 means
 * a virtual node, which is always healthy). Prefer non-Sink (capture source)
 * nodes over Sink nodes when both match (e.g. shared PW_KEY_NODE_DESCRIPTION).
 */
static const struct watched_node* watcher_healthy_node(
    const struct target_watcher* w) {
    const struct watched_node* best = NULL;
    for (int i = 0; i < w->n_nodes; i++) {
        const struct watched_node* n = &w->nodes[i];
        if (n->device_id != 0 && !watcher_dev_present(w, n->device_id))
            continue; /* orphaned zombie: device gone, node lingers */
        if (!best) {
            best = n;
        } else if (best->is_sink && !n->is_sink) {
            best = n;
        } else if (best->is_sink == n->is_sink && n->id > best->id) {
            best = n;
        }
    }
    return best;
}

static void watcher_update_resolved(struct target_watcher* w) {
    w->resolved_serial[0] = '\0';
    const struct watched_node* n = watcher_healthy_node(w);
    if (n && n->serial)
        snprintf(w->resolved_serial, sizeof(w->resolved_serial), "%u",
                 n->serial);
}

static int watcher_node_matches(const struct spa_dict* props, const char* t,
                                int* out_is_sink) {
    if (!props || !t || !*t) return 0;
    const char* mc = spa_dict_lookup(props, PW_KEY_MEDIA_CLASS);
    if (mc) {
        /* Ignore non-audio nodes (Video/Source, Midi/Bridge, etc.) */
        if (strncmp(mc, "Audio/", 6) != 0 &&
            strncmp(mc, "Stream/Output/Audio", 19) != 0)
            return 0;
    }
    int is_sink = (mc && strncmp(mc, "Audio/Sink", 10) == 0);
    if (out_is_sink) *out_is_sink = is_sink;

    /* Exact unique keys (node.name, object.path, object.serial) may
       intentionally target a Sink to capture its monitor. */
    const char* unique_keys[] = {
        PW_KEY_NODE_NAME,
        PW_KEY_OBJECT_PATH,
        PW_KEY_OBJECT_SERIAL,
    };
    for (unsigned i = 0; i < 3; i++) {
        const char* v = spa_dict_lookup(props, unique_keys[i]);
        if (v && strcmp(v, t) == 0) return 1;
    }

    /* Ambiguous human-readable description: Audio devices often expose both an
       Audio/Sink and an Audio/Source with the exact same
       PW_KEY_NODE_DESCRIPTION. Only match PW_KEY_NODE_DESCRIPTION if it is not
       an Audio/Sink, or let watcher_healthy_node() strictly prefer non-Sink
       nodes. */
    const char* desc = spa_dict_lookup(props, PW_KEY_NODE_DESCRIPTION);
    if (desc && strcmp(desc, t) == 0 && !is_sink) return 1;

    return 0;
}

static void watcher_global(void* userdata, uint32_t id, uint32_t permissions,
                           const char* type, uint32_t version,
                           const struct spa_dict* props) {
    (void)permissions;
    (void)version;
    struct target_watcher* w = userdata;
    if (!w->enabled || !type) return;

    int is_sink = 0;
    if (strcmp(type, PW_TYPE_INTERFACE_Device) == 0) {
        int exists = 0;
        for (int i = 0; i < w->n_devs; i++) {
            if (w->dev_ids[i] == id) {
                exists = 1;
                break;
            }
        }
        if (!exists &&
            w->n_devs < (int)(sizeof(w->dev_ids) / sizeof(w->dev_ids[0])))
            w->dev_ids[w->n_devs++] = id;
    } else if (strcmp(type, PW_TYPE_INTERFACE_Node) == 0 &&
               watcher_node_matches(props, w->target, &is_sink)) {
        struct watched_node* slot = NULL;
        for (int i = 0; i < w->n_nodes; i++)
            if (w->nodes[i].id == id) {
                slot = &w->nodes[i];
                break;
            }
        if (!slot && w->n_nodes < (int)(sizeof(w->nodes) / sizeof(w->nodes[0])))
            slot = &w->nodes[w->n_nodes++];
        if (slot) {
            slot->id = id;
            const char* d = spa_dict_lookup(props, PW_KEY_DEVICE_ID);
            slot->device_id = d ? (uint32_t)strtoul(d, NULL, 10) : 0;
            const char* s = spa_dict_lookup(props, PW_KEY_OBJECT_SERIAL);
            slot->serial = s ? (uint32_t)strtoul(s, NULL, 10) : 0;
            slot->is_sink = is_sink;
        }
    } else {
        return;
    }

    if (!w->ready) {
        watcher_update_resolved(w);
        return;
    }
    watcher_update_resolved(w);

    struct pw_data* d = w->data;
    if (watcher_healthy_node(w) && d && !d->stopping) {
        int serial_changed =
            (w->resolved_serial[0] &&
             strcmp(d->linked_serial, w->resolved_serial) != 0);
        if (w->was_lost || d->need_reconnect || !d->stream || serial_changed) {
            fprintf(stderr,
                    "watcher: target '%s' present again; (re)connecting\n",
                    w->target);
            w->was_lost = 0;
            d->need_reconnect = 1;
            d->force_reconnect_now = 1;
        }
    }
}

static void watcher_global_remove(void* userdata, uint32_t id) {
    struct target_watcher* w = userdata;
    if (!w->enabled) return;
    int was_present = (watcher_healthy_node(w) != NULL);

    for (int i = 0; i < w->n_devs; i++)
        if (w->dev_ids[i] == id) {
            memmove(&w->dev_ids[i], &w->dev_ids[i + 1],
                    sizeof(uint32_t) * (w->n_devs - i - 1));
            w->n_devs--;
            break;
        }
    for (int i = 0; i < w->n_nodes; i++)
        if (w->nodes[i].id == id) {
            memmove(&w->nodes[i], &w->nodes[i + 1],
                    sizeof(struct watched_node) * (w->n_nodes - i - 1));
            w->n_nodes--;
            break;
        }

    if (!w->ready) return;
    watcher_update_resolved(w);

    if (was_present && !watcher_healthy_node(w)) {
        fprintf(stderr, "watcher: target '%s' lost (%s); forcing reconnect\n",
                w->target,
                w->n_nodes > 0 ? "backing device vanished, node is a zombie"
                               : "node removed");
        w->was_lost = 1;
        struct pw_data* d = w->data;
        if (d && !d->stopping) {
            d->need_reconnect = 1;
            d->force_reconnect_now = 1;
        }
    }
}

static void watcher_core_done(void* userdata, uint32_t id, int seq) {
    (void)seq;
    struct target_watcher* w = userdata;
    if (id != PW_ID_CORE || w->ready) return;
    w->ready = 1;
    watcher_update_resolved(w);
    const struct watched_node* n = watcher_healthy_node(w);
    fprintf(stderr, "watcher: target '%s' %s\n", w->target,
            n ? "currently present" : "NOT present");
    if (n) {
        fprintf(stderr, "watcher: node id=%u serial=%u device=%u%s\n", n->id,
                n->serial, n->device_id,
                (n->device_id && n->serial)
                    ? ""
                    : " (WARNING: registry props incomplete -> zombie "
                      "detection degraded; report this line)");
        w->was_lost = 0;
        struct pw_data* d = w->data;
        if (d && !d->stopping && w->resolved_serial[0] &&
            strcmp(d->linked_serial, w->resolved_serial) != 0) {
            d->need_reconnect = 1;
            d->force_reconnect_now = 1;
        }
    } else {
        w->was_lost = 1;
        struct pw_data* d = w->data;
        if (d && !d->stopping) {
            d->need_reconnect = 1;
        }
    }
}

static void watcher_core_error(void* userdata, uint32_t id, int seq, int res,
                               const char* message) {
    (void)id;
    (void)seq;
    struct target_watcher* w = userdata;
    fprintf(stderr, "watcher: registry client error (%d: %s); rebuilding\n",
            res, message ? message : "");
    w->ready = 0; /* presence is unknown until rebuilt */
    w->rebuild_at_us = av_gettime() + WATCHER_REBUILD_MS * 1000LL;
}

static const struct pw_registry_events watcher_registry_events = {
    PW_VERSION_REGISTRY_EVENTS,
    .global = watcher_global,
    .global_remove = watcher_global_remove,
};
static const struct pw_core_events watcher_core_events = {
    PW_VERSION_CORE_EVENTS,
    .done = watcher_core_done,
    .error = watcher_core_error,
};

static void watcher_cleanup(struct target_watcher* w) {
    if (w->reg) {
        spa_hook_remove(&w->reg_hook);
        pw_proxy_destroy((struct pw_proxy*)w->reg);
        w->reg = NULL;
    }
    if (w->core) {
        spa_hook_remove(&w->core_hook);
        pw_core_disconnect(w->core);
        w->core = NULL;
    }
    if (w->ctx) {
        pw_context_destroy(w->ctx);
        w->ctx = NULL;
    }
    w->n_nodes = 0;
    w->n_devs = 0;
    w->resolved_serial[0] = '\0';
}

static void watcher_build(struct target_watcher* w) {
    watcher_cleanup(w);
    w->ready = 0;

    struct pw_properties* props = pw_properties_new(
        PW_KEY_APP_NAME, "pipewire_timestamp-target-watcher", NULL);
    w->ctx = pw_context_new(w->loop, props, 0);
    if (!w->ctx) goto fail;
    w->core = pw_context_connect(w->ctx, NULL, 0);
    if (!w->core) goto fail;
    pw_core_add_listener(w->core, &w->core_hook, &watcher_core_events, w);
    w->reg = pw_core_get_registry(w->core, PW_VERSION_REGISTRY, 0);
    if (!w->reg) goto fail;
    pw_registry_add_listener(w->reg, &w->reg_hook, &watcher_registry_events, w);
    pw_core_sync(w->core, PW_ID_CORE, 0);
    return;
fail:
    fprintf(stderr, "watcher: build failed; retrying\n");
    watcher_cleanup(w);
    w->rebuild_at_us = av_gettime() + WATCHER_REBUILD_MS * 1000LL;
}

static void watcher_service(struct pw_data* data) {
    if (!g_watcher.enabled) return;
    int64_t now = av_gettime();

    if (g_watcher.rebuild_at_us && now >= g_watcher.rebuild_at_us) {
        g_watcher.rebuild_at_us = 0;
        watcher_build(&g_watcher);
    }
    if (!g_watcher.ready || data->stopping) return;

    if (watcher_healthy_node(&g_watcher) == NULL && !data->need_reconnect) {
        static int64_t last_arm_us = 0;
        if (now - last_arm_us >= WATCHER_IDLE_RETRY_MS * 1000LL) {
            last_arm_us = now;
            fprintf(stderr,
                    "watcher: capture running but target '%s' is gone "
                    "(linked to a fallback?); arming reconnect\n",
                    g_watcher.target);
            g_watcher.was_lost = 1;
            data->need_reconnect = 1;
        }
    }
}

/* STREAMING_CHUNK:Implementing PipeWire stream callbacks and loop thread... */
static atomic_ullong g_rt_stale_drop = 0;

static void on_pw_process(void* userdata) {
    struct pw_data* data = userdata;

    /* heartbeat for the watchdog (relaxed store; av_gettime() is vdso) */
    atomic_store_explicit(&data->last_buf_us, (long long)av_gettime(),
                          memory_order_relaxed);

    int rt_ch =
        atomic_load_explicit(&g_rt_capture_channels, memory_order_relaxed);
    struct pw_buffer* b = pw_stream_dequeue_buffer(data->stream);
    if (!b) {
        atomic_fetch_add_explicit(&g_rt_out_of_buffers, 1,
                                  memory_order_relaxed);
        return;
    }
    if (rt_ch <= 0) {
        atomic_fetch_add_explicit(&g_rt_stale_drop, 1, memory_order_relaxed);
        pw_stream_queue_buffer(data->stream, b);
        return;
    }

    struct spa_buffer* buf = b->buffer;
    if (buf->n_datas < 1 || !buf->datas[0].data || !buf->datas[0].chunk) {
        atomic_fetch_add_explicit(&g_rt_bad_chunks, 1, memory_order_relaxed);
        pw_stream_queue_buffer(data->stream, b);
        return;
    }
    struct spa_data* d = &buf->datas[0];

    uint32_t offs = d->chunk->offset;
    if (offs > d->maxsize) offs = d->maxsize;
    uint32_t size = d->chunk->size;
    if (size > d->maxsize - offs) size = d->maxsize - offs;
    if (offs % sizeof(float) != 0) {
        atomic_fetch_add_explicit(&g_rt_bad_chunks, 1, memory_order_relaxed);
        pw_stream_queue_buffer(data->stream, b);
        return;
    }
    float* samples = SPA_PTROFF(d->data, offs, float);
    uint32_t num_floats = size / sizeof(float);
    num_floats -= num_floats % (uint32_t)rt_ch;

    if (num_floats == 0) {
        pw_stream_queue_buffer(data->stream, b);
        return;
    }

    struct pw_time pwt;
    int have_t = (pw_stream_get_time_n(data->stream, &pwt, sizeof(pwt)) == 0);
    uint64_t graph_now_ns = pw_stream_get_nsec(data->stream);
    struct timespec rt;
    clock_gettime(CLOCK_REALTIME, &rt);
    int64_t realtime_now_ns = (int64_t)rt.tv_sec * 1000000000LL + rt.tv_nsec;
    int64_t delay_ns =
        (have_t && pwt.rate.denom)
            ? av_rescale(pwt.delay, 1000000000LL * pwt.rate.num, pwt.rate.denom)
            : 0;
    int64_t cap_graph_ns = (b->time != 0) ? ((int64_t)b->time - delay_ns)
                                          : ((int64_t)graph_now_ns - delay_ns);
    int64_t cap_wall_us =
        (realtime_now_ns + (cap_graph_ns - (int64_t)graph_now_ns)) / 1000;

    size_t head =
        atomic_load_explicit(&g_ring_buffer.head, memory_order_relaxed);
    size_t tail =
        atomic_load_explicit(&g_ring_buffer.tail, memory_order_acquire);
    size_t used = head - tail;
    size_t free_space = RING_BUFFER_SIZE - used;

    if (free_space >= num_floats) {
        for (uint32_t i = 0; i < num_floats; i++) {
            g_ring_buffer.data[(head + i) & RING_BUFFER_MASK] = samples[i];
        }
        atomic_store_explicit(&g_ring_buffer.head, head + num_floats,
                              memory_order_release);

        size_t th = atomic_load_explicit(&g_ts_ring.head, memory_order_relaxed);
        size_t tt = atomic_load_explicit(&g_ts_ring.tail, memory_order_acquire);
        if (th - tt < TS_RING_SIZE) {
            g_ts_ring.recs[th & TS_RING_MASK].start_index = head;
            g_ts_ring.recs[th & TS_RING_MASK].cap_wall_us = cap_wall_us;
            atomic_store_explicit(&g_ts_ring.head, th + 1,
                                  memory_order_release);
        } else {
            atomic_fetch_add_explicit(&g_rt_ts_overflows, 1,
                                      memory_order_relaxed);
        }
    } else {
        atomic_fetch_add_explicit(&g_rt_xruns, 1, memory_order_relaxed);
    }

    pw_stream_queue_buffer(data->stream, b);
}

static void on_pw_param_changed(void* userdata, uint32_t id,
                                const struct spa_pod* param) {
    struct pw_data* data = userdata;
    if (id != SPA_PARAM_Format) return;
    if (param == NULL) {
        atomic_store_explicit(&g_rt_capture_channels, 0, memory_order_relaxed);
        fprintf(stderr,
                "PipeWire capture format cleared (device "
                "lost/reconfiguring)\n");
        return;
    }
    int r = spa_format_audio_raw_parse(param, &data->format);
    if (r < 0) {
        fprintf(stderr, "Failed to parse raw audio format: %d\n", r);
        return;
    }
    fprintf(stdout, "PipeWire capture format: rate:%d channels:%d\n",
            data->format.rate, data->format.channels);

    /* Acquire g_mutex BEFORE snapshotting the ring head and publishing
       g_rt_capture_channels so the worker thread cannot read past the
       old/new format boundary using the old channel count while the RT
       thread appends new-format samples. */
    pthread_mutex_lock(&g_mutex);
    {
        size_t boundary =
            atomic_load_explicit(&g_ring_buffer.head, memory_order_acquire);

        int cur_rate =
            g_format_change_pending ? g_pending_rate : g_capture_rate;
        int cur_channels =
            g_format_change_pending ? g_pending_channels : g_capture_channels;
        if (g_swr_ctx == NULL || (int)data->format.rate != cur_rate ||
            (int)data->format.channels != cur_channels) {
            if (g_format_change_pending && !g_has_format_skip) {
                g_format_skip_from = g_format_boundary;
                g_has_format_skip = 1;
            }
            g_format_boundary = boundary;
            g_pending_rate = (int)data->format.rate;
            g_pending_channels = (int)data->format.channels;
            g_format_change_pending = 1;
        }

        atomic_store_explicit(&g_rt_capture_channels,
                              (int)data->format.channels, memory_order_release);
    }
    pthread_mutex_unlock(&g_mutex);
}

static void on_pw_state_changed(void* userdata, enum pw_stream_state old,
                                enum pw_stream_state state, const char* error) {
    struct pw_data* data = userdata;
    fprintf(stderr, "PipeWire stream state: %s -> %s%s%s\n",
            pw_stream_state_as_string(old), pw_stream_state_as_string(state),
            error ? ": " : "", error ? error : "");

    if (state == PW_STREAM_STATE_STREAMING) {
        data->ever_streamed = 1;
        data->need_reconnect = 0;
        data->settle_deadline_us = 0;
        g_watcher.was_lost = 0;
        return;
    }

    if (data->stopping) return;
    if (state == PW_STREAM_STATE_ERROR ||
        (old >= PW_STREAM_STATE_CONNECTING &&
         state == PW_STREAM_STATE_UNCONNECTED)) {
        data->need_reconnect = 1;
    } else if (old == PW_STREAM_STATE_STREAMING &&
               state == PW_STREAM_STATE_PAUSED) {
        fprintf(stderr,
                "Capture fell back to PAUSED; if no data resumes in %d ms "
                "the source will be treated as lost.\n",
                PW_WATCHDOG_MS);
    }
}

static const struct pw_stream_events pw_stream_events = {
    PW_VERSION_STREAM_EVENTS,
    .state_changed = on_pw_state_changed,
    .param_changed = on_pw_param_changed,
    .process = on_pw_process,
};

static int start_pw_capture(struct pw_data* data) {
    struct pw_properties* props =
        pw_properties_new(PW_KEY_MEDIA_TYPE, "Audio", PW_KEY_MEDIA_CATEGORY,
                          "Capture", PW_KEY_MEDIA_ROLE, "Music", NULL);
    if (!props) return -1;

    data->linked_serial[0] = '\0';
    if (data->target) {
        const char* tgt = data->target;
        if (g_watcher.enabled && g_watcher.resolved_serial[0]) {
            tgt = g_watcher.resolved_serial;
            snprintf(data->linked_serial, sizeof(data->linked_serial), "%s",
                     tgt);
            fprintf(stderr, "pw: linking by object.serial %s\n", tgt);
        }
        pw_properties_set(props, PW_KEY_TARGET_OBJECT, tgt);
        pw_properties_set(props, PW_KEY_NODE_DONT_RECONNECT, "true");
    }

    atomic_store_explicit(&data->last_buf_us, 0, memory_order_relaxed);
    data->settle_deadline_us = 0;

    data->stream = pw_stream_new_simple(data->l, "pipewire-audio-capture",
                                        props, &pw_stream_events, data);
    if (!data->stream) {
        fprintf(stderr, "pw_stream_new_simple() failed\n");
        return -1;
    }

    uint8_t buffer[1024];
    struct spa_pod_builder b = SPA_POD_BUILDER_INIT(buffer, sizeof(buffer));
    const struct spa_pod* params[1];
    struct spa_audio_info_raw info =
        SPA_AUDIO_INFO_RAW_INIT(.format = SPA_AUDIO_FORMAT_F32, .rate = 48000,
                                .channels = 2,
                                .position = {SPA_AUDIO_CHANNEL_FL,
                                             SPA_AUDIO_CHANNEL_FR});
    params[0] = spa_format_audio_raw_build(&b, SPA_PARAM_EnumFormat, &info);

    int ret = pw_stream_connect(data->stream, PW_DIRECTION_INPUT, PW_ID_ANY,
                                PW_STREAM_FLAG_AUTOCONNECT |
                                    PW_STREAM_FLAG_MAP_BUFFERS |
                                    PW_STREAM_FLAG_RT_PROCESS,
                                params, 1);
    if (ret < 0) {
        fprintf(stderr, "pw_stream_connect() failed: %s\n", spa_strerror(ret));
        pw_stream_destroy(data->stream);
        data->stream = NULL;
        return -1;
    }
    return 0;
}

static int parse_double_arg(const char* s, double* out) {
    char* end = NULL;
    double v = strtod(s, &end);
    if (end == s || *end != '\0' || !isfinite(v)) return -1;
    *out = v;
    return 0;
}

static void* signal_thread(void* arg) {
    (void)arg;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    int sig, quit_count = 0;
    while (sigwait(&set, &sig) == 0) {
        if (++quit_count > 1) {
            static const char msg[] =
                "\nForce quit requested. Exiting immediately.\n";
            (void)!write(STDERR_FILENO, msg, sizeof(msg) - 1);
            _Exit(1);
        }
        fprintf(stderr,
                "\nShutting down (press Ctrl-C again to force quit)...\n");
        atomic_store(&quit_flag, 1);
    }
    return NULL;
}

static void* pw_loop_thread(void* arg) {
    struct pw_data* data = arg;
    int64_t last_reconnect_us = 0;
    int64_t last_health_us = 0;
    unsigned long long reported_xruns = 0, reported_oob = 0, reported_bad = 0,
                       reported_stale = 0;

    pw_loop_enter(data->l);

    if (data->target && *data->target) {
        snprintf(g_watcher.target, sizeof(g_watcher.target), "%s",
                 data->target);
        g_watcher.data = data;
        g_watcher.loop = data->l;
        g_watcher.enabled = 1;
        fprintf(stderr, "watcher: watching registry for target '%s'\n",
                g_watcher.target);
        watcher_build(&g_watcher);
        /* Run one brief iteration so the initial registry sync can resolve
           object.serial before start_pw_capture() links the stream. */
        pw_loop_iterate(data->l, 50);
    } else {
        fprintf(stderr,
                "watcher: disabled (run with -p <node.name> to detect "
                "mic unplug/replug)\n");
    }

    if (start_pw_capture(data) < 0) {
        fprintf(stderr,
                "Initial PipeWire connection failed; will keep "
                "retrying...\n");
        data->need_reconnect = 1;
    }

    while (!atomic_load(&quit_flag)) {
        pw_loop_iterate(data->l, 100);
        int64_t now = av_gettime();

        /* ---- RT event counters ---- */
        unsigned long long x = (unsigned long long)atomic_load_explicit(
            &g_rt_xruns, memory_order_relaxed);
        if (x != reported_xruns) {
            fprintf(stderr, "RT: ring buffer overflow (xrun), total=%llu\n", x);
            reported_xruns = x;
        }
        unsigned long long o = (unsigned long long)atomic_load_explicit(
            &g_rt_out_of_buffers, memory_order_relaxed);
        if (o != reported_oob) {
            fprintf(stderr, "RT: out of buffers, total=%llu\n", o);
            reported_oob = o;
        }
        unsigned long long bc = (unsigned long long)atomic_load_explicit(
            &g_rt_bad_chunks, memory_order_relaxed);
        if (bc != reported_bad) {
            fprintf(stderr, "RT: unusable capture chunk skipped, total=%llu\n",
                    bc);
            reported_bad = bc;
        }
        unsigned long long sd = (unsigned long long)atomic_load_explicit(
            &g_rt_stale_drop, memory_order_relaxed);
        if (sd != reported_stale) {
            fprintf(stderr,
                    "RT: dropped buffer with no negotiated format, "
                    "total=%llu\n",
                    sd);
            reported_stale = sd;
        }
        unsigned long long tso = (unsigned long long)atomic_load_explicit(
            &g_rt_ts_overflows, memory_order_relaxed);
        static unsigned long long reported_tso = 0;
        if (tso != reported_tso) {
            fprintf(stderr,
                    "RT: timestamp ring overflow (anchor lost), "
                    "total=%llu\n",
                    tso);
            reported_tso = tso;
        }

        /* ---- 5 s health check: log only when unhealthy ---- */
        if (now - last_health_us >= 5000000) {
            last_health_us = now;
            enum pw_stream_state st_code =
                data->stream ? pw_stream_get_state(data->stream, NULL)
                             : PW_STREAM_STATE_UNCONNECTED;
            const char* st = data->stream ? pw_stream_state_as_string(st_code)
                                          : "(no stream)";
            int64_t last = (int64_t)atomic_load_explicit(&data->last_buf_us,
                                                         memory_order_relaxed);
            int ch = (int)atomic_load_explicit(&g_rt_capture_channels,
                                               memory_order_relaxed);
            int target_ok = (!g_watcher.enabled || !g_watcher.ready ||
                             watcher_healthy_node(&g_watcher) != NULL);
            int streaming_ok = (st_code == PW_STREAM_STATE_STREAMING);
            int data_ok = (last > 0 && (now - last) < PW_WATCHDOG_MS * 1000LL);

            if (!target_ok || !streaming_ok || !data_ok || ch <= 0) {
                char age[32];
                if (last > 0)
                    snprintf(age, sizeof(age), "%" PRId64 "ms",
                             (now - last) / 1000);
                else
                    snprintf(age, sizeof(age), "never");
                fprintf(
                    stderr,
                    "pw health: state=%s in_ch=%d out_ch=%d data_age=%s%s\n",
                    st,
                    (int)atomic_load_explicit(&g_rt_capture_channels,
                                              memory_order_relaxed),
                    ffmpeg_out.channels, age,
                    target_ok ? " target=present" : " target=ABSENT");
            }
        }

        /* ---- data watchdog ---- */
        if (!data->stopping && !data->need_reconnect) {
            const char* err = NULL;
            enum pw_stream_state st =
                data->stream ? pw_stream_get_state(data->stream, &err)
                             : PW_STREAM_STATE_UNCONNECTED;
            int healthy = 0;
            if (st == PW_STREAM_STATE_STREAMING &&
                atomic_load_explicit(&g_rt_capture_channels,
                                     memory_order_relaxed) > 0) {
                int64_t last = (int64_t)atomic_load_explicit(
                    &data->last_buf_us, memory_order_relaxed);
                healthy = (last > 0) && (now - last < PW_WATCHDOG_MS * 1000LL);
            }
            if (!healthy) {
                if (data->settle_deadline_us == 0) {
                    data->settle_deadline_us =
                        now + (data->ever_streamed ? PW_WATCHDOG_MS * 1000LL
                                                   : PW_FIRSTLINK_MS * 1000LL);
                } else if (now >= data->settle_deadline_us) {
                    data->need_reconnect = 1;
                }
            } else {
                data->settle_deadline_us = 0;
            }
        }

        /* ---- target watcher ---- */
        watcher_service(data);

        /* ---- reconnect ---- */
        if (data->need_reconnect && !atomic_load(&quit_flag)) {
            int absent = (g_watcher.enabled && g_watcher.ready &&
                          watcher_healthy_node(&g_watcher) == NULL);
            if (absent) {
                if (data->stream) {
                    fprintf(stderr,
                            "pw: no live device behind target; capture parked "
                            "until it returns\n");
                    pw_stream_destroy(data->stream);
                    data->stream = NULL;
                    atomic_store_explicit(&g_rt_capture_channels, 0,
                                          memory_order_relaxed);
                }
                /* idle; watcher_global re-arms force_reconnect_now on return */
            } else if (data->force_reconnect_now ||
                       now - last_reconnect_us >=
                           (int64_t)PW_RETRY_MIN_MS * 1000LL) {
                data->force_reconnect_now = 0;
                last_reconnect_us = now;
                atomic_store_explicit(&g_rt_capture_channels, 0,
                                      memory_order_relaxed);
                if (data->stream) {
                    pw_stream_destroy(data->stream);
                    data->stream = NULL;
                }
                fprintf(stderr, "PipeWire connection lost; reconnecting...\n");
                data->settle_deadline_us = 0;
                if (start_pw_capture(data) == 0) {
                    /* Hand the connecting stream over to the watchdog's settle
                       timer so linking/negotiation has PW_FIRSTLINK_MS /
                       PW_WATCHDOG_MS to complete instead of being killed on
                       the next 500 ms retry tick. */
                    data->need_reconnect = 0;
                }
            }
        }
    }

    data->stopping = 1;
    if (g_watcher.enabled) watcher_cleanup(&g_watcher);
    if (data->stream) {
        pw_stream_destroy(data->stream);
        data->stream = NULL;
    }
    pw_loop_leave(data->l);
    return NULL;
}

/* STREAMING_CHUNK:Implementing CLI argument parsing and main entry point... */
int main(int argc, char* argv[]) {
    struct pw_data data = {0};
    const char* output_filename = NULL;
    const char* audio_encoder_name = NULL;
    const char* audio_filter_str = NULL;
    const char* pw_target = NULL;
    double max_run_time = 0.0;
    double segment_time = 0.0;
    int use_strftime = 0;
    int segment_clock = 0;          // --segment-clock alignment
    int64_t audio_bitrate = 64000;  // target audio bitrate in bits/sec
    int out_channels = 0;           // --ac; 0 means encoder default
    double fsync_ms = 0.0;          // --fsync-ms; 0 means no periodic fsync
    double latency_ms = 0.0;        // --latency; subtracted from subtitle time
    const char* tz = NULL;          // --tz timezone name
    const char* title = NULL;       // --title container title
    int keep_segments = 0;          // --keep-segments; 0 means keep all
    int show_codecs = 0;            // --codecs flag
    int ebur = 0;                   // --ebur flag
    int option;

    atomic_init(&data.last_buf_us, 0);

    enum {
        OPT_AC = 256,
        OPT_FSYNC_MS,
        OPT_TZ,
        OPT_KEEP_SEGMENTS,
        OPT_TITLE,
        OPT_METADATA_PIPE,
        OPT_SEGMENT_CLOCK,
        OPT_LATENCY,
        OPT_CODECS,
        OPT_EBUR
    };

    struct optparse options = optparse_init(argv, NULL);
    struct optparse_long longopts[] = {
        {"output", 'o', OPTPARSE_REQUIRED},
        {"codec", 'c', OPTPARSE_REQUIRED},
        {"af", 'a', OPTPARSE_REQUIRED},
        {"pw-target", 'p', OPTPARSE_REQUIRED},
        {"time", 't', OPTPARSE_REQUIRED},
        {"segment_time", 's', OPTPARSE_REQUIRED},
        {"bitrate", 'b', OPTPARSE_REQUIRED},
        {"strftime", 'f', OPTPARSE_NONE},
        {"ac", OPT_AC, OPTPARSE_REQUIRED},
        {"fsync-ms", OPT_FSYNC_MS, OPTPARSE_REQUIRED},
        {"tz", OPT_TZ, OPTPARSE_REQUIRED},
        {"keep-segments", OPT_KEEP_SEGMENTS, OPTPARSE_REQUIRED},
        {"title", OPT_TITLE, OPTPARSE_REQUIRED},
        {"metadata-pipe", OPT_METADATA_PIPE, OPTPARSE_REQUIRED},
        {"segment-clock", OPT_SEGMENT_CLOCK, OPTPARSE_NONE},
        {"latency", OPT_LATENCY, OPTPARSE_REQUIRED},
        {"codecs", OPT_CODECS, OPTPARSE_NONE},
        {"ebur", OPT_EBUR, OPTPARSE_NONE},
        {0, 0, 0}};

    while ((option = optparse_long(&options, longopts, NULL)) != -1) {
        switch (option) {
            case 'o':
                output_filename = options.optarg;
                break;
            case 'c':
                audio_encoder_name = options.optarg;
                break;
            case 'a':
                audio_filter_str = options.optarg;
                break;
            case 'p':
                pw_target = options.optarg;
                break;
            case 't':
                if (parse_double_arg(options.optarg, &max_run_time) < 0 ||
                    max_run_time < 0.0) {
                    fprintf(stderr, "Invalid -t (runtime) parameter: %s\n",
                            options.optarg);
                    exit(1);
                }
                break;
            case 's':
                if (parse_double_arg(options.optarg, &segment_time) < 0 ||
                    segment_time < 0.0) {
                    fprintf(stderr, "Invalid --segment_time parameter: %s\n",
                            options.optarg);
                    exit(1);
                }
                break;
            case 'b': {
                char* end = NULL;
                long long v = strtoll(options.optarg, &end, 10);
                if (end == options.optarg || *end != '\0' || v <= 0) {
                    fprintf(stderr, "Invalid -b (bitrate) parameter: %s\n",
                            options.optarg);
                    exit(1);
                }
                audio_bitrate = (int64_t)v;
                break;
            }
            case 'f':
                use_strftime = 1;
                break;
            case OPT_AC: {
                char* end = NULL;
                long v = strtol(options.optarg, &end, 10);
                if (end == options.optarg || *end != '\0' || v < 1 || v > 64) {
                    fprintf(stderr,
                            "Invalid --ac (output channels) parameter: "
                            "%s\n",
                            options.optarg);
                    exit(1);
                }
                out_channels = (int)v;
                break;
            }
            case OPT_FSYNC_MS: {
                double v;
                if (parse_double_arg(options.optarg, &v) < 0 || v < 1.0) {
                    fprintf(stderr,
                            "Invalid --fsync-ms parameter (must be >= "
                            "1): %s\n",
                            options.optarg);
                    exit(1);
                }
                fsync_ms = v;
                break;
            }
            case OPT_LATENCY: {
                double v;
                if (parse_double_arg(options.optarg, &v) < 0 || v < -10000.0 ||
                    v > 10000.0) {
                    fprintf(stderr,
                            "Invalid --latency parameter (ms, "
                            "-10000..10000): "
                            "%s\n",
                            options.optarg);
                    exit(1);
                }
                latency_ms = v;
                break;
            }
            case OPT_TZ:
                tz = options.optarg;
                break;
            case OPT_TITLE:
                title = options.optarg;
                break;
            case OPT_KEEP_SEGMENTS: {
                char* end = NULL;
                long v = strtol(options.optarg, &end, 10);
                if (end == options.optarg || *end != '\0' || v < 1) {
                    fprintf(stderr,
                            "Invalid --keep-segments parameter (must "
                            "be >= 1): "
                            "%s\n",
                            options.optarg);
                    exit(1);
                }
                keep_segments = (int)v;
                break;
            }
            case OPT_METADATA_PIPE: {
                if (g_meta_count >= META_MAX_PIPES) {
                    fprintf(stderr,
                            "Too many --metadata-pipe options (max %d)\n",
                            META_MAX_PIPES);
                    exit(1);
                }
                char* dup = strdup(options.optarg);
                if (!dup) {
                    fprintf(stderr, "Out of memory for --metadata-pipe\n");
                    exit(1);
                }
                g_meta_pipes[g_meta_count].path = dup;
                g_meta_count++;
                break;
            }
            case OPT_SEGMENT_CLOCK:
                segment_clock = 1;
                break;
            case OPT_CODECS:
                show_codecs = 1;
                break;
            case OPT_EBUR:
                ebur = 1;
                break;
            case '?':
                fprintf(stderr, "%s: %s\n", argv[0], options.errmsg);
                exit(1);
        }
    }

    if (show_codecs) {
        list_supported_codecs();
        return 0;
    }

    if (!output_filename) {
        fprintf(stderr,
                "Usage: %s -o output.mkv [--codec encoder] [--af "
                "\"audio_filters\"] "
                "[-p pipewire_target] [-t runtime_in_seconds] "
                "[-b audio_bitrate] "
                "[--segment_time seconds] [--segment-clock] [--strftime] "
                "[--keep-segments N] "
                "[--ac channels] [--fsync-ms ms] [--latency ms] [--tz "
                "ZONE] "
                "[--title TITLE] "
                "[--metadata-pipe PATH ...] [--ebur] [--codecs]\n",
                argv[0]);
        exit(1);
    }
    if (max_run_time != 0.0 &&
        (max_run_time > ((INT64_MAX - 1) / 1000000U) || max_run_time < 0.001)) {
        fprintf(stderr, "Invalid -t parameter\n");
        exit(1);
    }
    if (segment_time < 0.0 || (segment_time > 0.0 && segment_time < 0.001)) {
        fprintf(stderr, "Invalid --segment_time parameter\n");
        exit(1);
    }
    if (segment_time > 0.0 && !use_strftime &&
        validate_segment_template(output_filename) < 0) {
        fprintf(stderr,
                "With --segment_time (and without --strftime) the "
                "output filename "
                "must contain exactly one printf integer specifier "
                "(%%d/%%i/%%u/%%x/%%X/%%o with optional flags/width; "
                "use %%%% for "
                "a literal %%), e.g. out-%%03d.mkv\n");
        exit(1);
    }
    if (keep_segments > 0 && segment_time <= 0.0) {
        fprintf(stderr, "--keep-segments requires --segment_time\n");
        exit(1);
    }
    if (segment_clock && segment_time <= 0.0) {
        fprintf(stderr, "--segment-clock requires --segment_time\n");
        exit(1);
    }

    g_output_template = output_filename;
    g_use_strftime = use_strftime;
    g_segment_clock = segment_clock;
    g_segment_time = segment_time;
    g_out_channels = out_channels;
    g_fsync_ms = fsync_ms;
    g_latency_ms = latency_ms;
    g_keep_segments = keep_segments;
    g_title = title;
    g_track_comment = pw_target;
    g_tz = tz;
    g_ebur = ebur;

    sigset_t sigmask;
    sigemptyset(&sigmask);
    sigaddset(&sigmask, SIGINT);
    sigaddset(&sigmask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigmask, NULL);

#ifdef MCL_ONFAULT
    if (mlockall(MCL_CURRENT | MCL_FUTURE | MCL_ONFAULT) != 0)
#else
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0)
#endif
        fprintf(stderr,
                "Warning: mlockall() failed (%s); realtime pages may "
                "be paged "
                "out. Grant CAP_IPC_LOCK or raise RLIMIT_MEMLOCK.\n",
                strerror(errno));

    memset(&g_ring_buffer, 0, sizeof(g_ring_buffer));
    memset(&g_ts_ring, 0, sizeof(g_ts_ring));

    if (g_tz) {
        setenv("TZ", g_tz, 1);
        g_tz_active = 1;
    }
    tzset();
    if (g_tz_active) {
        time_t now = time(NULL);
        struct tm tmn;
        localtime_r(&now, &tmn);
        char zbuf[64];
        strftime(zbuf, sizeof(zbuf), "%Z %z", &tmn);
        fprintf(stderr, "Subtitle timezone: %s (currently %s)\n", g_tz, zbuf);
    }
    avformat_network_init();

    pw_init(&argc, &argv);
    data.mainloop = pw_main_loop_new(NULL);
    if (!data.mainloop) {
        fprintf(stderr, "error: pw_main_loop_new() failed\n");
        exit(1);
    }
    data.l = pw_main_loop_get_loop(data.mainloop);
    pthread_t sig_thread;
    if (pthread_create(&sig_thread, NULL, signal_thread, &data) != 0) {
        fprintf(stderr, "Failed to create signal thread\n");
        exit(1);
    }
    pthread_detach(sig_thread);

    fprintf(stderr, "Initialize encoder/filter graph/FIFOs...\n");
    init_ffmpeg_encoder(audio_encoder_name, audio_filter_str, audio_bitrate);

    data.target = pw_target;
    pthread_t pw_thread;
    if (pthread_create(&pw_thread, NULL, pw_loop_thread, &data) != 0) {
        fprintf(stderr, "Failed to create pipewire loop thread\n");
        exit(1);
    }

    if (atomic_load(&quit_flag)) {
        pthread_join(pw_thread, NULL);
        avcodec_free_context(&ffmpeg_out.audio_enc_ctx);
        if (filter_graph) avfilter_graph_free(&filter_graph);
        pw_main_loop_destroy(data.mainloop);
        pw_deinit();
        return 0;
    }

    if (segment_time > 0.0) {
        g_segment_time_samples =
            llround(segment_time * ffmpeg_out.audio_enc_ctx->sample_rate);
        if (g_segment_time_samples < 1) g_segment_time_samples = 1;

        if (segment_clock) {
            int64_t now_us = av_gettime();
            time_t now_sec = (time_t)(now_us / 1000000);
            struct tm tm_local;
            localtime_r(&now_sec, &tm_local);

            int64_t sec_since_midnight = tm_local.tm_hour * 3600 +
                                         tm_local.tm_min * 60 + tm_local.tm_sec;
            int64_t usec_since_midnight =
                sec_since_midnight * 1000000LL + (now_us % 1000000);
            int64_t interval_us = (int64_t)(segment_time * 1000000.0);

            int64_t next_boundary_midnight_us =
                ((usec_since_midnight / interval_us) + 1) * interval_us;

            g_next_segment_clock_us =
                now_us + (next_boundary_midnight_us - usec_since_midnight);

            double first_segment_sec =
                (next_boundary_midnight_us - usec_since_midnight) / 1000000.0;
            fprintf(stderr,
                    "Segment clock alignment enabled: first segment "
                    "will run "
                    "for %.3f seconds.\n",
                    first_segment_sec);
        } else {
            g_next_segment_pts = g_segment_time_samples;
        }
    }

    if (fsync_ms > 0.0) {
        g_fsync_interval_samples = llround(
            (fsync_ms / 1000.0) * ffmpeg_out.audio_enc_ctx->sample_rate);
        if (g_fsync_interval_samples < 1) g_fsync_interval_samples = 1;
        g_next_fsync_pts = g_fsync_interval_samples;
        if (fsync_thread_start() < 0) {
            fprintf(stderr, "Failed to create fsync thread\n");
            exit(1);
        }
    }

    char first_file[4096];
    if (build_output_filename(first_file, sizeof(first_file), 0) < 0) exit(1);
    open_output_file(first_file);
    ffmpeg_out.total_samples = 0;
    ffmpeg_out.next_sub_ts = 0;

    if (setup_metadata_pipes() < 0) {
        fprintf(stderr, "Failed to set up metadata pipe(s)\n");
        exit(1);
    }

    atomic_init(&worker_quit_flag, 0);
    if (pthread_create(&worker_thread, NULL, audio_worker_thread, NULL) != 0) {
        fprintf(stderr, "Failed to create audio worker thread\n");
        exit(1);
    }

    fprintf(stderr,
            "DEBUG: RT-safe event logging active (ring=%d floats / %zu "
            "bytes); "
            "xruns=%llu out_of_buffers=%llu reported from the main loop.\n",
            RING_BUFFER_SIZE, sizeof(g_ring_buffer.data),
            (unsigned long long)atomic_load_explicit(&g_rt_xruns,
                                                     memory_order_relaxed),
            (unsigned long long)atomic_load_explicit(&g_rt_out_of_buffers,
                                                     memory_order_relaxed));

    sd_notify_ready();

    int64_t start_time = av_gettime();
    int64_t last_ebur_print_us = 0;
    while (!atomic_load(&quit_flag)) {
        if (max_run_time > 0) {
            if (av_gettime() - start_time >=
                (int64_t)(max_run_time * 1000000U)) {
                atomic_store(&quit_flag, 1);
            }
        }
        if (g_ebur) {
            int64_t now_us = av_gettime();
            if (now_us - last_ebur_print_us >= 200000) {
                last_ebur_print_us = now_us;
                char m[16] = "--", s[16] = "--", i[16] = "--", lra[16] = "--";
                int updated = 0;
                pthread_mutex_lock(&g_ebur_mutex);
                if (g_ebur_updated) {
                    if (g_ebur_m[0]) memcpy(m, g_ebur_m, sizeof(m));
                    if (g_ebur_s[0]) memcpy(s, g_ebur_s, sizeof(s));
                    if (g_ebur_i[0]) memcpy(i, g_ebur_i, sizeof(i));
                    if (g_ebur_lra[0]) memcpy(lra, g_ebur_lra, sizeof(lra));
                    updated = 1;
                }
                pthread_mutex_unlock(&g_ebur_mutex);
                if (updated) {
                    fprintf(stderr,
                            "\r[EBU R128] M: %6s LUFS | S: %6s LUFS | "
                            "I: %6s "
                            "LUFS | LRA: %5s LU   ",
                            m, s, i, lra);
                    fflush(stderr);
                }
            }
        }
        usleep(100000);
    }
    if (g_ebur) {
        fprintf(stderr, "\n");
    }

    fprintf(stderr, "DEBUG: Exiting main loop, flushing buffers...\n");
    atomic_store(&quit_flag, 1);
    pthread_join(pw_thread, NULL);
    fprintf(stderr, "DEBUG: PipeWire stopped, draining worker...\n");

    atomic_store_explicit(&worker_quit_flag, 1, memory_order_release);
    pthread_cond_signal(&g_worker_cond);
    pthread_join(worker_thread, NULL);
    fprintf(stderr, "DEBUG: worker finished\n");

    teardown_metadata_pipes();

    flush_audio_encoder();

    fprintf(stderr, "DEBUG: writing trailer...\n");
    close_output_file();
    fsync_thread_stop();
    fprintf(stderr, "DEBUG: output finalized, exiting\n");

    avcodec_free_context(&ffmpeg_out.audio_enc_ctx);
    if (filter_graph) avfilter_graph_free(&filter_graph);

    pw_main_loop_destroy(data.mainloop);
    pw_deinit();

    if (g_swr_ctx) swr_free(&g_swr_ctx);
    if (g_fifo) av_audio_fifo_free(g_fifo);
    if (g_pre_filter_fifo) av_audio_fifo_free(g_pre_filter_fifo);
    if (g_post_filter_fifo) av_audio_fifo_free(g_post_filter_fifo);
    av_frame_free(&g_worker_resampled_frame);
    av_channel_layout_uninit(&g_capture_ch_layout);

    for (int i = 0; i < g_kept_count; i++) free(g_kept_files[i]);
    free(g_kept_files);

    avformat_network_deinit();

    return 0;
}
