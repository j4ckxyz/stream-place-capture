from __future__ import annotations

from dataclasses import dataclass

import aiohttp


@dataclass(frozen=True)
class LiveStream:
    did: str
    handle: str
    title: str


async def resolve_handle(session: aiohttp.ClientSession, endpoint: str, handle: str) -> str:
    url = f"{endpoint}/xrpc/com.atproto.identity.resolveHandle"
    async with session.get(url, params={"handle": handle}, timeout=20) as resp:
        resp.raise_for_status()
        data = await resp.json()
    did = str(data.get("did", "")).strip()
    if not did:
        raise RuntimeError(f"resolveHandle returned no did for {handle}")
    return did


async def get_live_users(session: aiohttp.ClientSession, endpoint: str) -> list[LiveStream]:
    url = f"{endpoint}/xrpc/place.stream.live.getLiveUsers"
    async with session.get(url, params={"limit": 100}, timeout=20) as resp:
        resp.raise_for_status()
        data = await resp.json()

    out: list[LiveStream] = []
    for item in data.get("streams", []) or []:
        author = item.get("author") or {}
        record = item.get("record") or {}
        did = str(author.get("did", "")).strip()
        handle = str(author.get("handle", "")).strip()
        title = str(record.get("title", "")).strip()
        if did:
            out.append(LiveStream(did=did, handle=handle, title=title))
    return out
