# pipewire_timestamp

A high-reliability, real-time PipeWire audio recorder that muxes audio and microsecond-accurate wall-clock timestamps into a Matroska (`.mkv`) container.

Timestamps are embedded as an active SubRip (`.srt`) subtitle stream updated every 20 ms. Subtitle timing is anchored to the physical capture instant rather than encoder output time, preserving accurate synchronization across resampling, complex audio filter graphs, and device reconnects.

---

## Key Features

- **Continuous 20 ms Timestamp Subtitles**: SubRip subtitle stream rendered on a 50 Hz grid (`YYYY-MM-DD HH:MM:SS.uuuuuu`), with support for IANA timezones (`--tz`) and pipeline latency compensation (`--latency`).
- **Real-Time Safe Audio Pipeline**:
  - Lock-free single-producer single-consumer float ring buffer (`16M` samples) isolating the PipeWire real-time callback from the FFmpeg worker thread.
  - Page-locked memory via `mlockall()` to eliminate page fault stalls on RT threads.
  - Dynamic capture format/sample rate transitions drained and resampled losslessly without dropping buffered samples.
- **Robust Hardware Watchdog**:
  - Registry monitoring on PipeWire nodes tracks physical capture devices via `object.serial`.
  - Automatic stream reconnection on device unplug/replug while preventing fallback sink zombie links.
- **Lossless Audio Segmentation**:
  - Split audio streams on packet boundaries with zero dropped samples (`--segment_time`).
  - Wall-clock alignment (`--segment-clock`) for exact top-of-the-hour splits (`XX:00:00`).
  - Native `strftime` filename templating (`--strftime`) with automatic collision suffixing.
  - Rolling buffer retention (`--keep-segments N`) to prune old segments automatically.
- **Dynamic Subtitle Telemetry**:
  - Up to 16 external named pipes (`--metadata-pipe`) for real-time text injection (sensor readings, track info, transcription).
  - Built-in Whisper filter metadata extraction (`lavfi.whisper.text`).
  - Live console and subtitle EBU R128 loudness metrics (`--ebur`).
- **Durability & System Integration**:
  - Non-blocking periodic `fsync()` worker thread (`--fsync-ms`) to ensure coherent replayable files during unexpected power loss.
  - Dependency-free `systemd` watchdog and status notification (`sd_notify`).

---

## Dependencies & Compilation

### Dependencies

Ensure you have the PipeWire development libraries and FFmpeg (version 7.0+ recommended) installed:

- **Debian / Ubuntu**:
  ```bash
  sudo apt install build-essential pkg-config libpipewire-0.3-dev \
    libavformat-dev libavcodec-dev libavfilter-dev libavutil-dev libswresample-dev
  ```

- **Fedora / RHEL**:
  ```bash
  sudo dnf install gcc pkgconf-pkg-config pipewire-devel \
    ffmpeg-free-devel
  ```
  *(If using RPM Fusion, `ffmpeg-devel` may be used instead of `ffmpeg-free-devel`.)*

- **Arch Linux**:
  ```bash
  sudo pacman -S base-devel pkgconf pipewire ffmpeg
  ```

### Build

Compile using `gcc` and `pkg-config`:

```bash
gcc -Og -ggdb3 -fno-omit-frame-pointer -o pipewire_timestamp \
  pipewire_timestamp.c -lavformat -lavcodec -lavfilter -lavutil -lswresample \
  -lm -lz -lpthread \
  $(pkg-config --cflags --libs libpipewire-0.3)
```

---

## Usage Synopsis

```bash
./pipewire_timestamp -o output.mkv [options]
```

### Options Reference

| Option | Short | Argument | Description |
| :--- | :---: | :--- | :--- |
| `--output` | `-o` | `<file>` | Output file path or format template (**required**). |
| `--codec` | `-c` | `<name>` | FFmpeg audio encoder name (defaults to Opus). |
| `--bitrate` | `-b` | `<bps>` | Target audio bitrate in bits/sec (default: `64000`). |
| `--ac` | | `<N>` | Force output audio channel count (down-/up-mixed by resampler). |
| `--af` | `-a` | `<filter>` | FFmpeg audio filter string (evaluated before encoding). |
| `--pw-target` | `-p` | `<target>` | PipeWire source name, serial, or description to capture. |
| `--time` | `-t` | `<sec>` | Total runtime limit in seconds before clean exit. |
| `--segment_time` | `-s` | `<sec>` | Split output every `N` seconds on encoded packet boundaries. |
| `--segment-clock` | | | Align segment splits to wall-clock multiples of `segment_time`. |
| `--strftime` | `-f` | | Expand `strftime` specifiers in output filename using capture start time. |
| `--keep-segments`| | `<N>` | Keep only the `N` most recent segments on disk (requires `--segment_time`). |
| `--fsync-ms` | | `<ms>` | Flush muxer and invoke background `fsync()` every `N` milliseconds. |
| `--latency` | | `<ms>` | Offset subtitle timestamps by `±N` ms to compensate for filter graph delay. |
| `--tz` | | `<zone>` | Subtitle timezone formatted as IANA name (e.g. `Europe/Helsinki`). |
| `--title` | | `<title>` | Set container title metadata. |
| `--metadata-pipe`| | `<fifo>` | Read named pipe and append line to subtitle (repeatable up to 16). |
| `--ebur` | | | Measure and print live EBU R128 loudness metrics (`M`, `S`, `I`, `LRA`). |
| `--codecs` | | | Probe and list system audio encoders verified to encode at 48000 Hz. |

---

## Examples

### 1. High-Quality Recording with Local Timezone
Record default capture input to Opus at 128 kbps with timestamps formatted for `Europe/Helsinki`:

```bash
./pipewire_timestamp -o mic-session.mkv -b 128000 --tz Europe/Helsinki
```

### 2. Unattended 24/7 Rolling Archive
Record continuous hourly segments split exactly on the top of the hour (`XX:00:00`), keeping only the last 24 hours of audio:

```bash
./pipewire_timestamp \
  -o "archive-%Y%m%d-%H%M%S.mkv" \
  --strftime \
  --segment_time 3600 \
  --segment-clock \
  --keep-segments 24 \
  --fsync-ms 5000 \
  --tz UTC
```

### 3. Record Specific Hardware Interface with Noise Reduction
Bind directly to a USB interface using an FFmpeg noise filter, downmixing to mono:

```bash
./pipewire_timestamp \
  -p "alsa_input.usb-Shure_Inc_Shure_MV7-00.mono-fallback" \
  -o podcast-clean.mkv \
  --ac 1 \
  --af "arnndn=m=models/bd.rnnn" \
  --ebur
```

### 4. Real-Time Telemetry via Dynamic Subtitle Pipes
Inject external runtime data (such as sensor values or live speech-to-text) into the subtitle track:

```bash
# Terminal 1: Launch recorder listening to two FIFOs
./pipewire_timestamp -o flight-audio.mkv \
  --metadata-pipe /tmp/altitude_pipe \
  --metadata-pipe /tmp/speed_pipe

# Terminal 2: Push telemetry data
echo "ALT: 3200 ft" > /tmp/altitude_pipe
echo "SPD: 140 kts" > /tmp/speed_pipe
```

---

## Playback

Play recordings with `mpv`. To ensure subtitle overlays are clearly readable, specify a monospaced font and disable SDH stripping:

```bash
mpv --sub-filter-sdh=no --force-window=yes --sid=1 --sub-font="Iosevka Term" out.mkv
```

---

## Pipeline Architecture

```text
 ┌──────────────────────────────────────────────┐
 │             PipeWire Audio Node              │
 └──────────────────────┬───────────────────────┘
                        │
                        ▼ (Real-Time Callback, mlockall)
 ┌──────────────────────────────────────────────┐
 │ Lock-Free Float Ring Buffer (16M samples)    │
 │ Capture Anchor Queue (Capture Time <-> Pts)  │
 └──────────────────────┬───────────────────────┘
                        │
                        ▼ (Audio Worker Thread)
 ┌──────────────────────────────────────────────┐
 │ libswresample (Format/Channel Adaptation)    │
 └──────────────────────┬───────────────────────┘
                        │
                        ▼
 ┌──────────────────────────────────────────────┐
 │ Optional Filter Graph (--af / --ebur)        │
 └──────────────────────┬───────────────────────┘
                        │
                        ▼
 ┌──────────────────────────────────────────────┐
 │ Audio Encoder (Opus / AAC / FLAC)            │
 └──────────────────────┬───────────────────────┘
                        │
                        ├──────────────────────────┐
                        │                          │
                        ▼ (Audio Packets)          ▼ (20 ms Timestamps + Pipes)
 ┌──────────────────────────────────────────────┐  │
 │ Matroska Container Muxer (avformat)          │ ◄┘
 │ - Packet-boundary segmentation               │
 │ - strftime template expansion                │
 └──────────────────────┬───────────────────────┘
                        │
                        ▼ (Async Flush Queue)
 ┌──────────────────────────────────────────────┐
 │ Dedicated Background fsync() Worker          │
 └──────────────────────────────────────────────┘
```

---

## License

This project is open-source and distributed under the terms of the [MIT License](LICENSE).