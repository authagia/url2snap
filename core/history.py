import asyncio
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.models import PlaybackResult, TrackRef
from core.persistence import atomic_write_json, read_json, unwrap_versioned


@dataclass(frozen=True)
class HistoryEntry:
    id: str
    track: TrackRef
    result: PlaybackResult
    started_at: str
    ended_at: str


class HistoryRepository:
    async def add(self, entry: HistoryEntry) -> HistoryEntry:
        raise NotImplementedError

    async def list(self, limit: int = 100) -> list[HistoryEntry]:
        raise NotImplementedError

    async def get(self, entry_id: str) -> HistoryEntry | None:
        raise NotImplementedError


class JsonHistoryRepository(HistoryRepository):
    """Small JSON-backed history store.

    The repository boundary intentionally hides storage details so this can be
    replaced by SQLite later without changing Controller/API semantics.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    async def _read(self) -> list[HistoryEntry]:
        raw = await read_json(self.path)
        raw = unwrap_versioned(raw, key="entries", path=self.path)
        return [
            HistoryEntry(
                id=item["id"],
                track=TrackRef(original_url=item["track"]["original_url"]),
                result=PlaybackResult(item["result"]),
                started_at=item["started_at"],
                ended_at=item["ended_at"],
            )
            for item in raw
        ]

    async def _write(self, entries: list[HistoryEntry]):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = []
        for entry in entries:
            item = asdict(entry)
            item["result"] = entry.result.value
            payload.append(item)
        await atomic_write_json(self.path, {"version": 1, "entries": payload})

    async def add(self, entry: HistoryEntry) -> HistoryEntry:
        async with self._lock:
            entries = await self._read()
            entries.append(entry)
            await self._write(entries)
        return entry

    async def list(self, limit: int = 100) -> list[HistoryEntry]:
        if limit < 1:
            return []
        async with self._lock:
            entries = await self._read()
        return list(reversed(entries[-limit:]))

    async def get(self, entry_id: str) -> HistoryEntry | None:
        async with self._lock:
            entries = await self._read()
        return next((entry for entry in entries if entry.id == entry_id), None)


def new_history_entry(
    track: TrackRef,
    result: PlaybackResult,
    started_at: str,
    ended_at: str | None = None,
) -> HistoryEntry:
    return HistoryEntry(
        id=uuid.uuid4().hex[:12],
        track=track,
        result=result,
        started_at=started_at,
        ended_at=ended_at or datetime.now(timezone.utc).isoformat(),
    )
