from __future__ import annotations

import asyncio
from pathlib import Path

from collections import deque

from core.models import QueueItem, TrackRef
from core.persistence import atomic_write_json, read_json, unwrap_versioned
from core.tracklist import TrackList, TrackListItemNotFound


class QueueItemNotFound(TrackListItemNotFound):
    pass


class QueueRepository:
    async def load(self) -> list[QueueItem]:
        raise NotImplementedError

    async def save(self, items: list[QueueItem]) -> None:
        raise NotImplementedError


class JsonQueueRepository(QueueRepository):
    """JSON-backed waiting-queue store."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    @staticmethod
    def _from_dict(item: dict) -> QueueItem:
        return QueueItem(
            id=item["id"],
            track=TrackRef(original_url=item["track"]["original_url"]),
        )

    @staticmethod
    def _to_dict(item: QueueItem) -> dict:
        return {
            "id": item.id,
            "track": {"original_url": item.track.original_url},
        }

    async def load(self) -> list[QueueItem]:
        async with self._lock:
            raw = await read_json(self.path)
        return [self._from_dict(item) for item in unwrap_versioned(raw, key="items", path=self.path)]

    async def save(self, items: list[QueueItem]) -> None:
        payload = {
            "version": 1,
            "items": [self._to_dict(item) for item in items],
        }
        async with self._lock:
            await atomic_write_json(self.path, payload)


class PlaybackQueue(TrackList):
    """Active playback queue built on the generic TrackList abstraction."""

    item_cls = QueueItem

    def __init__(self, repository: QueueRepository | None = None):
        super().__init__()
        self.repository = repository

    async def load(self) -> list[QueueItem]:
        if self.repository is None:
            return await self.snapshot()
        items = await self.repository.load()
        async with self._lock:
            self._items.clear()
            self._items.extend(items)
        return list(items)

    async def _persist_items_locked(self, items: list[QueueItem]) -> None:
        if self.repository is not None:
            await self.repository.save(items)

    async def add(self, url: str, *, index: int | None = None) -> QueueItem:
        item = self._new_item(TrackRef(original_url=url))
        async with self._lock:
            items = list(self._items)
            if index is None:
                items.append(item)
            else:
                if index < 0 or index > len(items):
                    raise IndexError("track list index out of range")
                items.insert(index, item)
            await self._persist_items_locked(items)
            self._items = deque(items)
        return item

    async def replace(self, urls: list[str]) -> list[QueueItem]:
        items = [self._new_item(TrackRef(original_url=url)) for url in urls]
        async with self._lock:
            await self._persist_items_locked(items)
            self._items = deque(items)
        return items

    async def remove(self, item_id: str) -> QueueItem:
        async with self._lock:
            items = list(self._items)
            for i, item in enumerate(items):
                if item.id == item_id:
                    removed = items.pop(i)
                    await self._persist_items_locked(items)
                    self._items = deque(items)
                    return removed
        raise QueueItemNotFound(item_id)

    async def move(self, item_id: str, to_index: int) -> QueueItem:
        async with self._lock:
            items = list(self._items)
            old_index = next((i for i, x in enumerate(items) if x.id == item_id), None)
            if old_index is None:
                raise QueueItemNotFound(item_id)
            if to_index < 0 or to_index >= len(items):
                raise IndexError("track list index out of range")
            item = items.pop(old_index)
            items.insert(to_index, item)
            await self._persist_items_locked(items)
            self._items = deque(items)
            return item

    async def pop_next(self) -> QueueItem | None:
        async with self._lock:
            if not self._items:
                return None
            item = self._items[0]
            items = list(self._items)[1:]
            await self._persist_items_locked(items)
            self._items = deque(items)
            return item

    async def clear(self):
        async with self._lock:
            await self._persist_items_locked([])
            self._items.clear()
