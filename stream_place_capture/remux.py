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
    max_width: int | None
    max_height: int | None
    target_fps: int | None
    estimated_mbps: float
    description: str


QUALITY_PRESETS: dict[str, QualityInfo] = {
    "lossless": {
        "name": "lossless",
        "label": "Source (Lossless)",
        "mode": "copy",
        "video_bitrate_k": None,
        "audio_bitrate_k": None,
        "max_width": None,
        "max_height": None,
        "target_fps": None,
        "estimated_mbps": 8.0,
        "description": "Best fidelity, largest files",
    },
    "high": {
        "name": "high",
        "label": "High 720p (H.264)",
        "mode": "h264",
        "video_bitrate_k": 2200,
        "audio_bitrate_k": 112,
        "max_width": 1280,
        "max_height": 720,
        "target_fps": 30,
        "estimated_mbps": 2.3,
        "description": "Recommended default, visibly smaller files",
    },
    "balanced": {
        "name": "balanced",
        "label": "Balanced 540p (H.264)",
        "mode": "h264",
        "video_bitrate_k": 1400,
        "audio_bitrate_k": 96,
        "max_width": 960,
        "max_height": 540,
        "target_fps": 30,
        "estimated_mbps": 1.5,
        "description": "Good quality and strong savings",
    },
    "efficient": {
        "name": "efficient",
        "label": "Efficient 480p (H.264)",
        "mode": "h264",
        "video_bitrate_k": 900,
        "audio_bitrate_k": 96,
        "max_width": 854,
        "max_height": 480,
        "target_fps": 30,
        "estimated_mbps": 1.0,
        "description": "Smallest files, usable quality",
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
        "max_width": p["max_width"],
        "max_height": p["max_height"],
        "target_fps": p["target_fps"],
        "estimated_mbps": p["estimated_mbps"],
        "description": p["description"],
    }


def estimated_gb_per_hour(name: str) -> float:
    return quality_preset_info(name)["estimated_mbps"] * 0.45


class Remuxer:
    def __init__(
        self,
        ffmpeg_path: str,
        stream_name: str,
        segment_dir: Path,
        output_dir: Path,
        quality_preset: str,
        chunk_segments: int = 12,
    ) -> None:
        self.ffmpeg_path = ffmpeg_path
        self.stream_name = stream_name
        self.segment_dir = segment_dir
        self.output_dir = output_dir
        self.quality_preset = quality_preset
        self.chunk_segments = max(3, int(chunk_segments))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(f"remuxer.{stream_name}")

    def live_output_path(self) -> Path:
        return self.output_dir / f"{self.stream_name}.live.mp4"

    def stream_state_path(self) -> Path:
        return self.output_dir / f"{self.stream_name}.state.json"

    def load_processed_state(self) -> dict[str, object]:
        path = self.stream_state_path()
        if not path.exists():
            return {"processed": []}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                raw = data.get("processed")
                processed: list[str] = []
                if isinstance(raw, list):
                    processed = [str(x) for x in raw]
                chunks_raw = data.get("chunks")
                chunks: list[str] = []
                if isinstance(chunks_raw, list):
                    chunks = [str(x) for x in chunks_raw]
                next_chunk_index_raw = data.get("next_chunk_index")
                next_chunk_index = int(next_chunk_index_raw) if isinstance(next_chunk_index_raw, int) else 1
                return {
                    "processed": processed,
                    "chunks": chunks,
                    "next_chunk_index": next_chunk_index,
                }
        except Exception:
            pass
        return {"processed": []}

    def save_processed_state(self, state: dict[str, object]) -> None:
        path = self.stream_state_path()
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(path)

    def prune_processed_raw_segments(self, keep_recent: int) -> int:
        state = self.load_processed_state()
        raw_processed = state.get("processed")
        processed_list = raw_processed if isinstance(raw_processed, list) else []
        processed = set(str(x) for x in processed_list)
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
        args = [
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

        mw = info["max_width"]
        mh = info["max_height"]
        fps = info["target_fps"]
        filters: list[str] = []
        if mw and mh:
            filters.append(f"scale={mw}:{mh}:force_original_aspect_ratio=decrease")
        if fps:
            filters.append(f"fps={fps}")
        if filters:
            args.extend(["-vf", ",".join(filters)])

        return args

    def _run_ffmpeg(self, cmd: list[str]) -> bool:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            self.log.warning("ffmpeg failed: %s", proc.stderr.strip()[:700])
            return False
        return True

    def _finalize_output_file(self, temp_path: Path, output_path: Path) -> Path:
        for _ in range(8):
            try:
                temp_path.replace(output_path)
                return output_path
            except PermissionError:
                time.sleep(0.15)

        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        fallback = output_path.with_name(f"{self.stream_name}.live.{stamp}.mp4")
        temp_path.replace(fallback)
        self.log.warning(
            "could not replace active output file %s (likely in use), wrote fallback %s",
            output_path,
            fallback,
        )
        return fallback

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
            "-f",
            "mp4",
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
        cmd.extend(["-movflags", "+faststart", "-f", "mp4", "-y", str(out_tmp)])
        return self._run_ffmpeg(cmd)

    def _compose_live_from_chunks(self, chunk_paths: list[Path]) -> Path | None:
        if not chunk_paths:
            return None
        output_path = self.live_output_path()
        temp_path = self.output_dir / f"{self.stream_name}.live.tmp.mp4"
        ok = self._concat_copy(chunk_paths, temp_path)
        if not ok:
            return None
        return self._finalize_output_file(temp_path, output_path)

    def _chunk_dir(self) -> Path:
        d = self.output_dir / "chunks"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _process_new_segments(self, force_partial: bool) -> list[Path]:
        info = quality_preset_info(self.quality_preset)
        state = self.load_processed_state()
        raw_processed = state.get("processed")
        processed_names: set[str] = set()
        if isinstance(raw_processed, list):
            for item in raw_processed:
                processed_names.add(str(item))
        raw_chunk_names = state.get("chunks")
        chunk_names = [str(x) for x in raw_chunk_names] if isinstance(raw_chunk_names, list) else []
        raw_next = state.get("next_chunk_index")
        next_index = int(raw_next) if isinstance(raw_next, int) else 1

        segment_files = sorted(self.segment_dir.glob("*.mp4"))
        pending = [p for p in segment_files if p.name not in processed_names]
        created_chunks: list[Path] = []
        chunk_dir = self._chunk_dir()

        while pending and (force_partial or len(pending) >= self.chunk_segments):
            if force_partial and len(pending) < self.chunk_segments:
                batch = pending
            else:
                batch = pending[: self.chunk_segments]

            chunk_name = f"{self.stream_name}.chunk.{next_index:08d}.mp4"
            chunk_tmp = chunk_dir / f"{self.stream_name}.chunk.{next_index:08d}.tmp.mp4"
            chunk_out = chunk_dir / chunk_name

            if info["mode"] == "copy":
                ok = self._concat_copy(batch, chunk_tmp)
            else:
                ok = self._encode_transcode(batch, chunk_tmp, info, f"{self.stream_name}.chunk.{next_index:08d}.concat.txt")
            if not ok:
                break

            chunk_tmp.replace(chunk_out)
            created_chunks.append(chunk_out)

            for seg in batch:
                processed_names.add(seg.name)
            chunk_names.append(chunk_name)
            next_index += 1

            state_to_save = {
                "processed": sorted(processed_names),
                "chunks": chunk_names,
                "next_chunk_index": next_index,
            }
            self.save_processed_state(state_to_save)

            pending = pending[len(batch) :]

        return created_chunks

    def remux_progressive(self) -> Path | None:
        self._process_new_segments(force_partial=False)
        state = self.load_processed_state()
        raw_chunk_names = state.get("chunks")
        chunk_names = [str(x) for x in raw_chunk_names] if isinstance(raw_chunk_names, list) else []
        chunk_dir = self._chunk_dir()
        chunk_paths = [chunk_dir / name for name in chunk_names if (chunk_dir / name).exists()]
        out = self._compose_live_from_chunks(chunk_paths)
        if out is not None:
            self.log.info("updated remux output %s preset=%s", out, quality_preset_info(self.quality_preset)["name"])
        return out

    def force_full_rebuild(self) -> Path | None:
        self._process_new_segments(force_partial=True)
        state = self.load_processed_state()
        raw_chunk_names = state.get("chunks")
        chunk_names = [str(x) for x in raw_chunk_names] if isinstance(raw_chunk_names, list) else []
        chunk_dir = self._chunk_dir()
        chunk_paths = [chunk_dir / name for name in chunk_names if (chunk_dir / name).exists()]
        out = self._compose_live_from_chunks(chunk_paths)
        if out is not None:
            self.log.info("force rebuilt output %s preset=%s", out, quality_preset_info(self.quality_preset)["name"])
        return out

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
