from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StreamTarget:
    name: str
    handle: str
    did: str


@dataclass(frozen=True)
class CaptureConfig:
    endpoint: str
    reconnect_delay_seconds: int
    max_reconnect_delay_seconds: int
    ping_interval_seconds: int
    websocket_idle_timeout_seconds: int
    remux_every_segments: int
    remux_interval_seconds: int
    enable_realtime_remux: bool
    ffmpeg_path: str
    require_1080p30: bool
    access_jwt: str | None
    capture_root: Path
    final_root: Path
    log_root: Path
    stream_targets: list[StreamTarget]


def _validate_target(raw: dict[str, Any]) -> StreamTarget:
    name = str(raw.get("name", "")).strip()
    handle = str(raw.get("handle", "")).strip().lstrip("@")
    did = str(raw.get("did", "")).strip()
    if not name or not handle or not did:
        raise ValueError(f"invalid stream target: {raw}")
    return StreamTarget(name=name, handle=handle, did=did)


def load_config(config_path: Path) -> CaptureConfig:
    raw = json.loads(config_path.read_text(encoding="utf-8"))

    endpoint = str(raw.get("endpoint", "https://stream.place")).rstrip("/")
    reconnect_delay_seconds = int(raw.get("reconnect_delay_seconds", 4))
    max_reconnect_delay_seconds = int(raw.get("max_reconnect_delay_seconds", 30))
    ping_interval_seconds = int(raw.get("ping_interval_seconds", 15))
    websocket_idle_timeout_seconds = int(raw.get("websocket_idle_timeout_seconds", 45))
    remux_every_segments = int(raw.get("remux_every_segments", 30))
    remux_interval_seconds = int(raw.get("remux_interval_seconds", 120))
    enable_realtime_remux = bool(raw.get("enable_realtime_remux", True))
    ffmpeg_path = str(raw.get("ffmpeg_path", "ffmpeg"))
    require_1080p30 = bool(raw.get("require_1080p30", False))
    access_jwt_raw = raw.get("access_jwt")
    access_jwt = str(access_jwt_raw).strip() if access_jwt_raw else None

    capture_root = Path(str(raw.get("capture_root", "captures/segments")))
    final_root = Path(str(raw.get("final_root", "captures/final")))
    log_root = Path(str(raw.get("log_root", "logs")))

    stream_targets_raw = raw.get("streams", [])
    if not isinstance(stream_targets_raw, list) or not stream_targets_raw:
        raise ValueError("streams must be a non-empty list")

    stream_targets = [_validate_target(item) for item in stream_targets_raw]

    return CaptureConfig(
        endpoint=endpoint,
        reconnect_delay_seconds=reconnect_delay_seconds,
        max_reconnect_delay_seconds=max_reconnect_delay_seconds,
        ping_interval_seconds=ping_interval_seconds,
        websocket_idle_timeout_seconds=websocket_idle_timeout_seconds,
        remux_every_segments=remux_every_segments,
        remux_interval_seconds=remux_interval_seconds,
        enable_realtime_remux=enable_realtime_remux,
        ffmpeg_path=ffmpeg_path,
        require_1080p30=require_1080p30,
        access_jwt=access_jwt,
        capture_root=capture_root,
        final_root=final_root,
        log_root=log_root,
        stream_targets=stream_targets,
    )


def config_to_dict(cfg: CaptureConfig) -> dict[str, Any]:
    return {
        "endpoint": cfg.endpoint,
        "reconnect_delay_seconds": cfg.reconnect_delay_seconds,
        "max_reconnect_delay_seconds": cfg.max_reconnect_delay_seconds,
        "ping_interval_seconds": cfg.ping_interval_seconds,
        "websocket_idle_timeout_seconds": cfg.websocket_idle_timeout_seconds,
        "enable_realtime_remux": cfg.enable_realtime_remux,
        "remux_every_segments": cfg.remux_every_segments,
        "remux_interval_seconds": cfg.remux_interval_seconds,
        "ffmpeg_path": cfg.ffmpeg_path,
        "require_1080p30": cfg.require_1080p30,
        "access_jwt": cfg.access_jwt,
        "capture_root": str(cfg.capture_root),
        "final_root": str(cfg.final_root),
        "log_root": str(cfg.log_root),
        "streams": [asdict(s) for s in cfg.stream_targets],
    }


def save_config(config_path: Path, cfg: CaptureConfig) -> None:
    payload = config_to_dict(cfg)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = config_path.with_suffix(config_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(config_path)
