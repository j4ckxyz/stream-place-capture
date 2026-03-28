from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import TypedDict


class QualityInfo(TypedDict):
    name: str
    label: str
    mode: str
    video_bitrate_k: int | None
    audio_bitrate_k: int | None
    estimated_mbps: float
    description: str


QUALITY_PRESETS: dict[str, QualityInfo] = {
    "lossless": {
        "name": "lossless",
        "label": "Source (Lossless)",
        "mode": "copy",
        "video_bitrate_k": None,
        "audio_bitrate_k": None,
        "estimated_mbps": 8.0,
        "description": "Best fidelity, largest files",
    },
    "high": {
        "name": "high",
        "label": "High (H.264)",
        "mode": "h264",
        "video_bitrate_k": 3600,
        "audio_bitrate_k": 128,
        "estimated_mbps": 3.8,
        "description": "High quality with much lower storage",
    },
    "balanced": {
        "name": "balanced",
        "label": "Balanced (H.264)",
        "mode": "h264",
        "video_bitrate_k": 2400,
        "audio_bitrate_k": 112,
        "estimated_mbps": 2.5,
        "description": "Good quality and strong compression",
    },
    "efficient": {
        "name": "efficient",
        "label": "Efficient (H.264)",
        "mode": "h264",
        "video_bitrate_k": 1500,
        "audio_bitrate_k": 96,
        "estimated_mbps": 1.6,
        "description": "Best storage savings",
    },
}


def list_quality_presets() -> list[QualityInfo]:
    order = ["lossless", "high", "balanced", "efficient"]
    return [quality_preset_info(k) for k in order]


def quality_preset_info(name: str) -> QualityInfo:
    key = (name or "").strip().lower()
    if key not in QUALITY_PRESETS:
        key = "high"
    p = QUALITY_PRESETS[key]
    return {
        "name": p["name"],
        "label": p["label"],
        "mode": p["mode"],
        "video_bitrate_k": p["video_bitrate_k"],
        "audio_bitrate_k": p["audio_bitrate_k"],
        "estimated_mbps": p["estimated_mbps"],
        "description": p["description"],
    }


def estimated_gb_per_hour(name: str) -> float:
    return quality_preset_info(name)["estimated_mbps"] * 0.45


class Remuxer:
    def __init__(self, ffmpeg_path: str, stream_name: str, segment_dir: Path, output_dir: Path, quality_preset: str) -> None:
        self.ffmpeg_path = ffmpeg_path
        self.stream_name = stream_name
        self.segment_dir = segment_dir
        self.output_dir = output_dir
        self.quality_preset = quality_preset
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(f"remuxer.{stream_name}")

    def live_output_path(self) -> Path:
        return self.output_dir / f"{self.stream_name}.live.mp4"

    def stream_state_path(self) -> Path:
        return self.output_dir / f"{self.stream_name}.state.json"

    def load_processed_state(self) -> dict[str, list[str]]:
        path = self.stream_state_path()
        if not path.exists():
            return {"processed": []}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                raw = data.get("processed")
                if isinstance(raw, list):
                    return {"processed": [str(x) for x in raw]}
        except Exception:
            pass
        return {"processed": []}

    def save_processed_state(self, state: dict[str, list[str]]) -> None:
        path = self.stream_state_path()
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(path)

    def prune_processed_raw_segments(self, keep_recent: int) -> int:
        state = self.load_processed_state()
        processed = set(state.get("processed", []))
        if not processed:
            return 0
        files = sorted(self.segment_dir.glob("*.mp4"))
        if len(files) <= keep_recent:
            return 0
        to_consider = files[: max(0, len(files) - keep_recent)]
        removed = 0
        for p in to_consider:
            if p.name not in processed:
                continue
            try:
                p.unlink(missing_ok=True)
                removed += 1
            except Exception:
                continue
        return removed

    def _concat_entry(self, path: Path) -> str:
        abs_path = path.resolve().as_posix().replace("'", "'\\''")
        return f"file '{abs_path}'"

    def _write_concat_list(self, paths: list[Path], list_file: Path) -> None:
        lines = [self._concat_entry(path) for path in paths]
        list_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _build_transcode_args(self, info: QualityInfo) -> list[str]:
        if info["mode"] == "copy":
            return ["-c", "copy"]
        v_k = int(info["video_bitrate_k"] or 2400)
        a_k = int(info["audio_bitrate_k"] or 112)
        return [
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "high",
            "-b:v",
            f"{v_k}k",
            "-maxrate",
            f"{int(v_k * 1.35)}k",
            "-bufsize",
            f"{int(v_k * 2)}k",
            "-g",
            "60",
            "-keyint_min",
            "60",
            "-c:a",
            "aac",
            "-b:a",
            f"{a_k}k",
        ]

    def _run_ffmpeg(self, cmd: list[str]) -> bool:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            self.log.warning("ffmpeg failed: %s", proc.stderr.strip()[:700])
            return False
        return True

    def _concat_copy(self, inputs: list[Path], out_tmp: Path) -> bool:
        list_file = self.output_dir / f"{self.stream_name}.concat.txt"
        self._write_concat_list(inputs, list_file)
        cmd = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_file),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            "-y",
            str(out_tmp),
        ]
        return self._run_ffmpeg(cmd)

    def _encode_transcode(self, inputs: list[Path], out_tmp: Path, info: QualityInfo, list_name: str) -> bool:
        list_file = self.output_dir / list_name
        self._write_concat_list(inputs, list_file)
        cmd = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_file),
        ]
        cmd.extend(self._build_transcode_args(info))
        cmd.extend(["-movflags", "+faststart", "-y", str(out_tmp)])
        return self._run_ffmpeg(cmd)

    def remux_progressive(self) -> Path | None:
        segment_files = sorted(self.segment_dir.glob("*.mp4"))
        if len(segment_files) < 2:
            return None

        output_path = self.live_output_path()
        info = quality_preset_info(self.quality_preset)

        state = self.load_processed_state()
        processed_names = set(state.get("processed", []))
        new_segments = [p for p in segment_files if p.name not in processed_names]
        if output_path.exists() and not new_segments:
            return output_path

        temp_path = self.output_dir / f"{self.stream_name}.live.tmp.mp4"

        if not output_path.exists():
            if info["mode"] == "copy":
                ok = self._concat_copy(segment_files, temp_path)
            else:
                ok = self._encode_transcode(segment_files, temp_path, info, f"{self.stream_name}.transcode.concat.txt")
            if not ok:
                return None
            temp_path.replace(output_path)
            self.save_processed_state({"processed": [p.name for p in segment_files]})
            self.log.info("updated remux output %s preset=%s", output_path, info["name"])
            return output_path

        if info["mode"] == "copy":
            ok = self._concat_copy([output_path] + new_segments, temp_path)
            if not ok:
                return None
            temp_path.replace(output_path)
            self.save_processed_state({"processed": [p.name for p in segment_files]})
            self.log.info("updated remux output %s preset=%s", output_path, info["name"])
            return output_path

        append_tmp = self.output_dir / f"{self.stream_name}.append.tmp.mp4"
        ok = self._encode_transcode(new_segments, append_tmp, info, f"{self.stream_name}.append.concat.txt")
        if not ok:
            return None

        ok = self._concat_copy([output_path, append_tmp], temp_path)
        try:
            append_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        if not ok:
            return None

        temp_path.replace(output_path)
        self.save_processed_state({"processed": [p.name for p in segment_files]})
        self.log.info("updated remux output %s preset=%s", output_path, info["name"])
        return output_path

    def force_full_rebuild(self) -> Path | None:
        segment_files = sorted(self.segment_dir.glob("*.mp4"))
        if len(segment_files) < 2:
            return None

        output_path = self.live_output_path()
        temp_path = self.output_dir / f"{self.stream_name}.live.tmp.mp4"
        info = quality_preset_info(self.quality_preset)

        if info["mode"] == "copy":
            ok = self._concat_copy(segment_files, temp_path)
        else:
            ok = self._encode_transcode(segment_files, temp_path, info, f"{self.stream_name}.full.concat.txt")
        if not ok:
            return None

        temp_path.replace(output_path)
        self.save_processed_state({"processed": [p.name for p in segment_files]})
        self.log.info("force rebuilt output %s preset=%s", output_path, info["name"])
        return output_path

    def build_preview_sample(self, preset_name: str, max_segments: int = 8) -> Path | None:
        segment_files = sorted(self.segment_dir.glob("*.mp4"))
        if not segment_files:
            return None
        sample = segment_files[-max_segments:]
        list_file = self.output_dir / f"{self.stream_name}.preview.concat.txt"
        self._write_concat_list(sample, list_file)

        info = quality_preset_info(preset_name)
        preview_dir = self.output_dir / "preview"
        preview_dir.mkdir(parents=True, exist_ok=True)
        out = preview_dir / f"{self.stream_name}.{info['name']}.preview.mp4"
        tmp = preview_dir / f"{self.stream_name}.{info['name']}.preview.tmp.mp4"

        cmd = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_file),
        ]
        cmd.extend(self._build_transcode_args(info))
        cmd.extend(["-movflags", "+faststart", "-y", str(tmp)])

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            self.log.warning("preview build failed: %s", proc.stderr.strip()[:700])
            return None

        tmp.replace(out)
        return out

    def archive_live_output(self) -> Path | None:
        source = self.live_output_path()
        if not source.exists():
            return None
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        archive = self.output_dir / f"{self.stream_name}.{stamp}.checkpoint.mp4"
        shutil.copy2(source, archive)
        self.log.info("wrote checkpoint archive %s", archive)
        return archive
