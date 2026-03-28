from __future__ import annotations

import struct
from dataclasses import dataclass


@dataclass(frozen=True)
class VideoTrackInfo:
    width: int | None
    height: int | None
    frame_rate: float | None


def _iter_boxes(data: bytes, start: int = 0, end: int | None = None):
    pos = start
    bound = len(data) if end is None else min(end, len(data))
    while pos + 8 <= bound:
        size = struct.unpack(">I", data[pos : pos + 4])[0]
        btype = data[pos + 4 : pos + 8]
        if size == 0:
            size = bound - pos
        elif size == 1:
            if pos + 16 > bound:
                break
            size = struct.unpack(">Q", data[pos + 8 : pos + 16])[0]
            header = 16
            content_start = pos + header
            content_end = pos + size
            if content_end > bound or size < header:
                break
            yield btype, content_start, content_end
            pos = content_end
            continue
        header = 8
        content_start = pos + header
        content_end = pos + size
        if size < header or content_end > bound:
            break
        yield btype, content_start, content_end
        pos = content_end


def parse_video_info(mp4_bytes: bytes) -> VideoTrackInfo:
    width: int | None = None
    height: int | None = None
    frame_rate: float | None = None

    for btype, start, end in _iter_boxes(mp4_bytes):
        if btype != b"moov":
            continue
        for t_type, t_start, t_end in _iter_boxes(mp4_bytes, start, end):
            if t_type != b"trak":
                continue
            tkhd_w = None
            tkhd_h = None
            for inner_type, inner_start, inner_end in _iter_boxes(mp4_bytes, t_start, t_end):
                if inner_type == b"tkhd":
                    version = mp4_bytes[inner_start]
                    if version == 1:
                        if inner_start + 104 <= inner_end:
                            tkhd_w = struct.unpack(">I", mp4_bytes[inner_start + 96 : inner_start + 100])[0] >> 16
                            tkhd_h = struct.unpack(">I", mp4_bytes[inner_start + 100 : inner_start + 104])[0] >> 16
                    else:
                        if inner_start + 92 <= inner_end:
                            tkhd_w = struct.unpack(">I", mp4_bytes[inner_start + 84 : inner_start + 88])[0] >> 16
                            tkhd_h = struct.unpack(">I", mp4_bytes[inner_start + 88 : inner_start + 92])[0] >> 16

                if inner_type != b"mdia":
                    continue

                timescale = None
                sample_count = 0
                sample_delta_sum = 0

                for mdia_type, mdia_start, mdia_end in _iter_boxes(mp4_bytes, inner_start, inner_end):
                    if mdia_type == b"mdhd":
                        version = mp4_bytes[mdia_start]
                        if version == 1:
                            if mdia_start + 32 <= mdia_end:
                                timescale = struct.unpack(">I", mp4_bytes[mdia_start + 20 : mdia_start + 24])[0]
                        else:
                            if mdia_start + 20 <= mdia_end:
                                timescale = struct.unpack(">I", mp4_bytes[mdia_start + 12 : mdia_start + 16])[0]

                    if mdia_type != b"minf":
                        continue

                    for minf_type, minf_start, minf_end in _iter_boxes(mp4_bytes, mdia_start, mdia_end):
                        if minf_type != b"stbl":
                            continue
                        for stbl_type, stbl_start, stbl_end in _iter_boxes(mp4_bytes, minf_start, minf_end):
                            if stbl_type == b"stsd":
                                if stbl_start + 16 <= stbl_end:
                                    entry_count = struct.unpack(">I", mp4_bytes[stbl_start + 4 : stbl_start + 8])[0]
                                    if entry_count > 0 and stbl_start + 8 + 8 + 78 <= stbl_end:
                                        sample_entry_start = stbl_start + 8
                                        sample_type = mp4_bytes[sample_entry_start + 4 : sample_entry_start + 8]
                                        if sample_type in {b"avc1", b"hvc1", b"hev1", b"av01"}:
                                            w = struct.unpack(">H", mp4_bytes[sample_entry_start + 32 : sample_entry_start + 34])[0]
                                            h = struct.unpack(">H", mp4_bytes[sample_entry_start + 34 : sample_entry_start + 36])[0]
                                            width = width or w
                                            height = height or h

                            if stbl_type == b"stts":
                                if stbl_start + 8 <= stbl_end:
                                    entry_count = struct.unpack(">I", mp4_bytes[stbl_start + 4 : stbl_start + 8])[0]
                                    cur = stbl_start + 8
                                    for _ in range(entry_count):
                                        if cur + 8 > stbl_end:
                                            break
                                        count = struct.unpack(">I", mp4_bytes[cur : cur + 4])[0]
                                        delta = struct.unpack(">I", mp4_bytes[cur + 4 : cur + 8])[0]
                                        sample_count += count
                                        sample_delta_sum += count * delta
                                        cur += 8

                if tkhd_w and tkhd_h:
                    width = width or tkhd_w
                    height = height or tkhd_h
                if timescale and sample_count > 0 and sample_delta_sum > 0:
                    fps = timescale * sample_count / sample_delta_sum
                    frame_rate = frame_rate or fps

    return VideoTrackInfo(width=width, height=height, frame_rate=frame_rate)


def is_mp4_segment(data: bytes) -> bool:
    return b"ftyp" in data[:256] and b"moov" in data


def is_roughly_1080p30(info: VideoTrackInfo) -> bool:
    if info.width is None or info.height is None or info.frame_rate is None:
        return False
    in_resolution = info.width >= 1900 and info.height >= 1060
    in_fps = 29.0 <= info.frame_rate <= 31.1
    return in_resolution and in_fps
