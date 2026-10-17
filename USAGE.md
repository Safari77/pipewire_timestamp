# `pipewire_timestamp` Usage

## Synopsis

```bash
./pipewire_timestamp -o output.mkv [--codec audio_encoder] [--af "audio_filters"] \
  [-p pipewire_target] [-t runtime_in_seconds] [-b audio_bitrate] \
  [--segment_time seconds] [--segment-clock] [--strftime] [--ac channels] \
  [--fsync-ms ms] [--latency ms] [--ebur] [--codecs]
```

## Options

* **`--bitrate N`** (or **`-b N`**)
  Target audio bitrate in bits/sec (default `64000`), e.g., `"-b 128000"`.

* **`--ac N`**
  Force the number of audio channels in the output file. The capture layout is down-/up-mixed to `N` channels by the resampler, so e.g., `"--ac 1"` produces a mono file from a stereo source. Defaults to the encoder's preferred layout when omitted.

* **`--fsync-ms N`**
  Periodically flush the muxer and `fsync()` the output file to disk roughly every `N` milliseconds of recorded audio (e.g., `"--fsync-ms 1500"`). The interleaver is drained and the Matroska cluster is finalized before each `fsync()`, so what reaches the disk is always a coherent, replayable prefix. The `fsync()` itself runs on a dedicated thread, so a slow disk never stalls the audio path.

* **`--latency N`**
  Subtract `N` milliseconds (float, may be negative) from every subtitle timestamp to compensate for pipeline latency the capture-time anchor cannot see, e.g., buffering inside an `--af` filter graph or device-side latency PipeWire does not report.

* **`--segment_time N`**
  Start a new output file roughly every `N` seconds (no dropped packets). Without `--strftime`, the output name must contain a `printf` integer specifier for the segment index, e.g., `"out-%03d.mkv"`.

* **`--segment-clock`**
  Align segment boundaries to local clock time multiples of `segment_time`. For example, `"--segment_time 3600 --segment-clock"` forces segment splits exactly at the top of the hour (`XX:00:00`). Boundaries are evaluated on the capture clock (the same one the subtitles print), so a cut lands within one encoded packet of the boundary rather than one pipeline latency past it. Requires `--segment_time`.

* **`--strftime`**
  Expand `strftime()` specifiers in the output filename using the local capture time at which each file starts (not the moment the encoder caught up), so the name describes the audio the file actually contains, e.g., `"rec-%Y%m%d-%H%M%S.mkv"`. Use `'%%'` to put a literal `'%'` in the name. If two consecutive files expand to the same name (e.g., one-second time resolution with sub-second segments), a `"-N"` suffix is appended before the extension so the new file never truncates the previous one.

* **`--keep-segments N`**
  Together with `--segment_time`, keep only the `N` most recent segment files on disk; older ones are deleted as new segments open. Ideal for continuous, unattended recording of the recent past.

* **`--tz ZONE`**
  Write subtitle timestamps in the given IANA timezone (e.g., `"Europe/Helsinki"`) instead of UTC, appending the zone's UTC offset (`+HH:MM`) to every line. DST is resolved per timestamp, so summer/winter transitions are handled correctly.

* **`--title TITLE`**
  Set the Matroska container title metadata. (The `--pw-target` name, if given, is written as the audio track's comment.)

* **`--metadata-pipe P`**
  Read a named pipe (FIFO) and show its latest line as an extra subtitle line beneath the timestamp. Repeatable: each `--metadata-pipe` adds one more line, in order. A background thread reads the pipes; the 20 ms subtitle loop snapshots them with a try-lock and reuses the previous text if the reader is mid-update, so audio never stalls. Each line is capped at 40 UTF-8 characters; pushing an empty line clears that line.

* **`--ebur`**
  Measure and display live EBU R128 loudness stats (`M`, `S`, `I`, `LRA`) while recording using FFmpeg's `ebur128` filter.

* **`--codecs`**
  List available audio encoding codecs verified at 48000 Hz.

## Compilation

Compile with (adjust `pkg-config` as needed):

```bash
gcc -Og -ggdb3 -fno-omit-frame-pointer -o pipewire_timestamp \
  pipewire_timestamp.c -lavformat -lavcodec -lavfilter -lavutil -lswresample \
  -lm -lz -lpthread \
  $(pkg-config --cflags --libs libpipewire-0.3)
```

## Playback

Play with `mpv`:

```bash
mpv --sub-filter-sdh=no --force-window=yes --sid=1 --sub-font="Iosevka Term" out.mkv
```
