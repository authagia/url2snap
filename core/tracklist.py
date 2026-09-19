import asyncio
import uuid
from collections import deque
from collections.abc import Iterable

from core.models import Playlist, TrackListItem, TrackRef


class TrackListItemNotFound(KeyError):
    pass


class TrackList:
    """Ordered collection of stable TrackRefs.

    Persistent/shared lists use this abstraction and its async lock. Playback
    does not depend on a concrete queue or playlist implementation.
    """

    item_cls = TrackListItem

    def __init__(self):
        self._items: deque[TrackListItem] = deque()
        self._lock = asyncio.Lock()

    def _new_item(self, track: TrackRef) -> TrackListItem:
        return self.item_cls(id=uuid.uuid4().hex[:12], track=track)

    async def add_track(self, track: TrackRef, *, index: int | None = None) -> TrackListItem:
        item = self._new_item(track)
        async with self._lock:
            if index is None:
                self._items.append(item)
            else:
                if index < 0 or index > len(self._items):
                    raise IndexError("track list index out of range")
                items = list(self._items)
                items.insert(index, item)
                self._items = deque(items)
        return item

    async def pop_next(self) -> TrackListItem | None:
        async with self._lock:
            return self._items.popleft() if self._items else None

    async def snapshot(self) -> list[TrackListItem]:
        async with self._lock:
            return list(self._items)

    async def remove(self, item_id: str) -> TrackListItem:
        async with self._lock:
            items = list(self._items)
            for i, item in enumerate(items):
                if item.id == item_id:
                    removed = items.pop(i)
                    self._items = deque(items)
                    return removed
        raise TrackListItemNotFound(item_id)

    async def move(self, item_id: str, to_index: int) -> TrackListItem:
        async with self._lock:
            items = list(self._items)
            old_index = next((i for i, x in enumerate(items) if x.id == item_id), None)
            if old_index is None:
                raise TrackListItemNotFound(item_id)
            if to_index < 0 or to_index >= len(items):
                raise IndexError("track list index out of range")
            item = items.pop(old_index)
            items.insert(to_index, item)
            self._items = deque(items)
            return item

    async def replace_tracks(self, tracks: list[TrackRef]) -> list[TrackListItem]:
        items = [self._new_item(track) for track in tracks]
        async with self._lock:
            self._items = deque(items)
        return items

    async def clear(self):
        async with self._lock:
            self._items.clear()


class ActiveTrackList:
    """Transient playback list owned by the playback coordinator.

    Unlike a persistent TrackList, this object is consumed serially by the
    coordinator, so it intentionally does not use asyncio locks. Items are
    snapshots of TrackListItems and therefore remain stable even if the source
    playlist is edited or deleted while playback is active.
    """

    def __init__(
        self,
        items: Iterable[TrackListItem],
        *,
        source_type: str,
        source_id: str | None = None,
        source_name: str | None = None,
    ):
        self._items: deque[TrackListItem] = deque(items)
        self.source_type = source_type
        self.source_id = source_id
        self.source_name = source_name

    @classmethod
    def from_playlist(cls, playlist: Playlist) -> "ActiveTrackList":
        # Make fresh TrackListItems so the active snapshot cannot share
        # mutable item objects with the repository's Playlist instance.
        items = [
            TrackListItem(id=item.id, track=item.track)
            for item in playlist.items
        ]
        return cls(
            items,
            source_type="playlist",
            source_id=playlist.id,
            source_name=playlist.name,
        )

    def pop_next(self) -> TrackListItem | None:
        return self._items.popleft() if self._items else None

    def append(self, item: TrackListItem) -> None:
        self._items.append(item)

    def snapshot(self) -> list[TrackListItem]:
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    @property
    def exhausted(self) -> bool:
        return not self._items

    def info(self) -> dict[str, str]:
        data = {"type": self.source_type}
        if self.source_id is not None:
            data["id"] = self.source_id
        if self.source_name is not None:
            data["name"] = self.source_name
        return data
