from __future__ import annotations

import logging
import subprocess
from pathlib import Path


class Remuxer:
    def __init__(self, ffmpeg_path: str, stream_name: str, segment_dir: Path, output_dir: Path) -> None:
        self.ffmpeg_path = ffmpeg_path
        self.stream_name = stream_name
        self.segment_dir = segment_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(f"remuxer.{stream_name}")

    def remux_progressive(self) -> Path | None:
        segment_files = sorted(self.segment_dir.glob("*.mp4"))
        if len(segment_files) < 2:
            return None

        list_file = self.output_dir / f"{self.stream_name}.concat.txt"
        lines = [f"file '{path.as_posix()}'" for path in segment_files]
        list_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        output_path = self.output_dir / f"{self.stream_name}.live.mp4"
        temp_path = self.output_dir / f"{self.stream_name}.live.mp4.tmp"

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
            str(temp_path),
        ]

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            self.log.warning("ffmpeg remux failed: %s", proc.stderr.strip()[:500])
            return None

        temp_path.replace(output_path)
        self.log.info("updated remux output %s", output_path)
        return output_path
