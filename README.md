# Stream.place conference capture (resilient segment recorder)

This recorder uses the Stream.place live segment websocket API directly:

- `place.stream.live.subscribeSegments` (binary MP4 segment stream)

It is designed to avoid corruption by writing each segment atomically and assembling output progressively.

## Why this is safer

- Each segment is saved as its own standalone `.mp4` file first.
- Writes are atomic (`.tmp` then rename), so crashes do not damage existing segment files.
- Progressive chunk assembly creates/updates `*.live.mp4`; if assemble fails, raw segments and finished chunks remain intact.
- Always-on websocket worker per stream with keepalive and exponential reconnect.
- Real-time heartbeat state file (`logs/state.json`) for external monitoring.

## Capture mode (no missed stream starts)

- This recorder is **always-on per configured DID**.
- It does not wait for polling to discover "live" transitions.
- As soon as a stream starts emitting segments, the websocket worker records them.

## Targets configured

- `@stream1.atmosphereconf.org` -> `did:plc:7tattzlorncahxgtdiuci7x7`
- `@stream2.atmosphereconf.org` -> `did:plc:djb6ssvz5wvuuqpdihlgh3xa`
- `@stream3.atmosphereconf.org` -> `did:plc:jcahd7fl7h23c24ftxuhkhiw`

## Quick start (Windows)

1. Clone repo.
2. Install ffmpeg and ensure `ffmpeg` is in `PATH`.
3. Run setup:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/setup.ps1
```

4. Edit config if needed:

- `config/streams.json` (created from example)

Recommended values for long conference runs:

- `reconnect_delay_seconds`: `4`
- `max_reconnect_delay_seconds`: `30`
- `ping_interval_seconds`: `15`
- `websocket_idle_timeout_seconds`: `45`
- `remux_interval_seconds`: `120`

5. Start recorder:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/run.ps1
```

This launches a modernized desktop dashboard (Tkinter) showing per-stream:

- live/offline indicator from Stream.place
- capture status and reconnects
- last segment freshness and total captured size
- thumbnail preview (live card image)

Storage efficiency defaults are now enabled:

- `quality_preset: high` (H.264 720p)
- raw segment pruning after successful finalize (keeps only a short recent tail)

This keeps crash-safety from segment capture while reducing disk usage significantly.

Stop action is guarded with a double confirmation (Yes/No + type `STOP`).

`Stop` now performs a graceful finalize:

- remuxes latest output per stream
- writes a timestamped checkpoint archive
- shows a dialog listing exactly where files were saved
- closes the app after finalize completes

The window close button (`X`) now runs the same graceful stop flow.

You can change where files are saved from the GUI via **Change Save Folder** (Windows folder picker).
The app shows current save paths for raw segments, final videos, and logs, and can open each folder.

Quality can be changed in GUI (dropdown) or CLI.
- GUI shows estimated GB/hour per stream for each preset.
- **Build Preview** makes a short test clip using recent segments so you can inspect quality before committing.

6. If needed, rebuild merged files from already-saved segments:

```powershell
.\.venv\Scripts\python.exe -m stream_place_capture --config config/streams.json --rebuild-final
```

## Run as startup service (Task Scheduler)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/register-task.ps1
Start-ScheduledTask -TaskName StreamPlaceCapture
```

## Build a single EXE (on Windows)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/build-exe.ps1
```

Then run:

```powershell
dist\stream-place-capture.exe --config config/streams.json --gui
```

## Quick start (Ubuntu)

1. Install dependencies:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv ffmpeg
```

2. Setup:

```bash
./scripts/linux/setup.sh
```

3. Run headless recorder:

```bash
./scripts/linux/run.sh
```

Use `tmux` or `systemd` for long-running conference capture.

### JSON status endpoint (Ubuntu)

Expose current recorder stats on port `11456`:

```bash
./scripts/linux/install-status-service.sh
curl http://127.0.0.1:11456/status
```

Returns JSON with:

- service state
- per-stream status/segments/last-segment age
- recording duration
- disk usage and free space

## CLI quality controls

```powershell
python -m stream_place_capture --list-quality
python -m stream_place_capture --quality high --config config/streams.json
python -m stream_place_capture --quality balanced --build-preview --config config/streams.json
```

## Output layout

- Raw segments: `captures/segments/<stream-name>/segments/*.mp4`
- Segment metadata: `captures/segments/<stream-name>/quality/*.json`
- Progressive merged file: `captures/final/<stream-name>/<stream-name>.live.mp4`
- Logs: `logs/capture.log`
- State heartbeat: `logs/state.json`

## 1080p30 handling

The recorder parses MP4 track metadata (resolution/fps) for every segment.

- `require_1080p30=true`: reject segments that are not approximately 1920x1080 @ 30fps.
- `require_1080p30=false`: capture everything and keep metadata so you can filter later.

For conference capture, `require_1080p30=false` is recommended to avoid accidental drops.

## Runtime checks

- Check `logs/state.json` regularly.
- Healthy stream should show `status: "recording"` and `last_segment_epoch_ms` advancing.
- Reconnect events increment `reconnects` so you can detect instability.

## About the "lossless remux WebRTC hack"

The Stream.place server code path for `subscribeSegments` upgrades to websocket and sends binary MP4 segments directly. This tool captures those bytes first, then either:

- keeps source quality (`lossless` preset), or
- transcodes to storage-efficient presets (`high`, `balanced`, `efficient`).

Assembly is chunk-based so outputs keep progressing without relying on one giant rewrite.

## Notes

- If stream privacy rules change, set `access_jwt` in config and use a valid bearer token.
- On abrupt shutdown, existing segments remain valid and can always be re-remuxed later.
