import asyncio
import uuid
from pathlib import Path

from core.models import Playlist, PlaylistItem, TrackRef
from core.persistence import atomic_write_json, read_json, unwrap_versioned


class PlaylistNotFound(KeyError):
    pass


class PlaylistItemNotFound(KeyError):
    pass


class PlaylistRepository:
    """Persistence boundary for named playlists."""

    async def create(self, name: str, tracks: list[TrackRef] | None = None) -> Playlist:
        raise NotImplementedError

    async def list(self) -> list[Playlist]:
        raise NotImplementedError

    async def get(self, playlist_id: str) -> Playlist | None:
        raise NotImplementedError

    async def save(self, playlist: Playlist) -> Playlist:
        raise NotImplementedError

    async def delete(self, playlist_id: str) -> Playlist:
        raise NotImplementedError


class JsonPlaylistRepository(PlaylistRepository):
    """JSON-backed playlist store. Replaceable by SQLite later."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    @staticmethod
    def _from_dict(item: dict) -> Playlist:
        return Playlist(
            id=item["id"],
            name=item["name"],
            items=[
                PlaylistItem(
                    id=track["id"],
                    track=TrackRef(original_url=track["track"]["original_url"]),
                )
                for track in item.get("items", [])
            ],
        )

    @staticmethod
    def _to_dict(playlist: Playlist) -> dict:
        return {
            "id": playlist.id,
            "name": playlist.name,
            "items": [
                {
                    "id": item.id,
                    "track": {"original_url": item.track.original_url},
                }
                for item in playlist.items
            ],
        }

    async def _read(self) -> list[Playlist]:
        raw = await read_json(self.path)
        return [self._from_dict(item) for item in unwrap_versioned(raw, key="playlists", path=self.path)]

    async def _write(self, playlists: list[Playlist]):
        payload = {
            "version": 1,
            "playlists": [self._to_dict(playlist) for playlist in playlists],
        }
        await atomic_write_json(self.path, payload)

    async def create(self, name: str, tracks: list[TrackRef] | None = None) -> Playlist:
        playlist = Playlist(
            id=uuid.uuid4().hex[:12],
            name=name,
            items=[
                PlaylistItem(id=uuid.uuid4().hex[:12], track=track)
                for track in (tracks or [])
            ],
        )
        async with self._lock:
            playlists = await self._read()
            playlists.append(playlist)
            await self._write(playlists)
        return playlist

    async def list(self) -> list[Playlist]:
        async with self._lock:
            return await self._read()

    async def get(self, playlist_id: str) -> Playlist | None:
        async with self._lock:
            playlists = await self._read()
        return next((p for p in playlists if p.id == playlist_id), None)

    async def save(self, playlist: Playlist) -> Playlist:
        async with self._lock:
            playlists = await self._read()
            for i, existing in enumerate(playlists):
                if existing.id == playlist.id:
                    playlists[i] = playlist
                    await self._write(playlists)
                    return playlist
        raise PlaylistNotFound(playlist.id)

    async def delete(self, playlist_id: str) -> Playlist:
        async with self._lock:
            playlists = await self._read()
            for i, playlist in enumerate(playlists):
                if playlist.id == playlist_id:
                    removed = playlists.pop(i)
                    await self._write(playlists)
                    return removed
        raise PlaylistNotFound(playlist_id)
