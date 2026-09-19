import asyncio
import tempfile
import unittest
from pathlib import Path

from core.controller import Controller
from core.history import HistoryRepository, JsonHistoryRepository, new_history_entry
from core.models import PlaybackResult, PlaybackState, TrackRef
from core.queue import PlaybackQueue, QueueItemNotFound


class InMemoryHistoryRepository(HistoryRepository):
    def __init__(self):
        self.entries = []

    async def add(self, entry):
        self.entries.append(entry)
        return entry

    async def list(self, limit=100):
        return list(reversed(self.entries[-limit:]))

    async def get(self, entry_id):
        return next((x for x in self.entries if x.id == entry_id), None)


class DoneSession:
    state = PlaybackState.IDLE
    current = None
    last_result = PlaybackResult.COMPLETED

    async def stop(self, result=PlaybackResult.STOPPED, silence=False):
        raise AssertionError("a terminal playback task must not be stopped again")


class TestPhase8(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.controller = None

    async def asyncTearDown(self):
        if self.controller is not None:
            self.controller._worker.cancel()
            await asyncio.gather(self.controller._worker, return_exceptions=True)

    async def test_queue_uses_tracklist_and_preserves_queue_item_type(self):
        queue = PlaybackQueue()
        a = await queue.add("http://example.test/a")
        b = await queue.add("http://example.test/b")
        await queue.move(b.id, 0)
        snapshot = await queue.snapshot()

        self.assertEqual([item.url for item in snapshot], [b.url, a.url])
        self.assertEqual(type(snapshot[0]).__name__, "QueueItem")

        await queue.remove(a.id)
        with self.assertRaises(QueueItemNotFound):
            await queue.remove(a.id)

    async def test_history_repository_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = JsonHistoryRepository(Path(tmp) / "history.json")
            entry = new_history_entry(
                TrackRef("http://example.test/song"),
                PlaybackResult.ERROR,
                "2026-09-19T00:00:00+00:00",
            )
            await repo.add(entry)
            loaded = await repo.get(entry.id)
            self.assertEqual(loaded, entry)

    async def test_enqueue_history_uses_original_url(self):
        history = InMemoryHistoryRepository()
        source = new_history_entry(
            TrackRef("https://example.test/original"),
            PlaybackResult.COMPLETED,
            "2026-09-19T00:00:00+00:00",
        )
        await history.add(source)

        self.controller = Controller(object(), "/tmp/unused-snap", history=history)
        self.controller._worker.cancel()
        await asyncio.gather(self.controller._worker, return_exceptions=True)
        item = await self.controller.enqueue_history(source.id)
        queued = await self.controller.queue.snapshot()

        self.assertEqual(item["url"], "https://example.test/original")
        self.assertEqual(queued[-1].track, source.track)

    async def test_done_playback_is_not_mislabeled_stopped(self):
        history = InMemoryHistoryRepository()
        self.controller = Controller(object(), "/tmp/unused-snap", history=history)
        self.controller.session = DoneSession()
        self.controller._current_track = TrackRef("https://example.test/finished")
        self.controller._current_started_at = "2026-09-19T00:00:00+00:00"
        self.controller._playback_task = asyncio.create_task(asyncio.sleep(0))
        await self.controller._playback_task

        await self.controller._stop_current(PlaybackResult.STOPPED, silence=False)

        self.assertEqual(len(history.entries), 1)
        self.assertEqual(history.entries[0].result, PlaybackResult.COMPLETED)

from core.models import PlaylistItem
from core.playlists import JsonPlaylistRepository


class TestPlaylistPersistence(unittest.IsolatedAsyncioTestCase):
    async def test_playlist_crud_and_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = JsonPlaylistRepository(Path(tmp) / "playlists.json")
            playlist = await repo.create(
                "Favorites",
                [TrackRef("https://example.test/a"), TrackRef("https://example.test/b")],
            )
            self.assertEqual(playlist.name, "Favorites")
            self.assertEqual([x.track.original_url for x in playlist.items], [
                "https://example.test/a",
                "https://example.test/b",
            ])

            loaded = await repo.get(playlist.id)
            self.assertEqual(loaded, playlist)

            loaded.items.append(PlaylistItem("extra", TrackRef("https://example.test/c")))
            await repo.save(loaded)
            loaded_again = await repo.get(playlist.id)
            self.assertEqual(loaded_again.items[-1].id, "extra")

            removed = await repo.delete(playlist.id)
            self.assertEqual(removed.id, playlist.id)
            self.assertIsNone(await repo.get(playlist.id))


from core.models import Playlist, PlaylistItem, TrackListItem
from core.tracklist import ActiveTrackList


class InMemoryPlaylistRepository:
    def __init__(self, playlists):
        self.playlists = {p.id: p for p in playlists}

    async def get(self, playlist_id):
        return self.playlists.get(playlist_id)


class TestPlaylistActivation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.controller = None

    async def asyncTearDown(self):
        if self.controller is not None:
            self.controller._worker.cancel()
            await asyncio.gather(self.controller._worker, return_exceptions=True)

    async def test_activation_preserves_queue_and_snapshots_playlist(self):
        playlist = Playlist(
            id="pl1",
            name="Favorites",
            items=[
                PlaylistItem("p1", TrackRef("https://example.test/p1")),
                PlaylistItem("p2", TrackRef("https://example.test/p2")),
            ],
        )
        self.controller = Controller(
            object(), "/tmp/unused-snap",
            playlists=InMemoryPlaylistRepository([playlist]),
        )
        self.controller._worker.cancel()
        await asyncio.gather(self.controller._worker, return_exceptions=True)

        await self.controller.queue.add("https://example.test/q1")
        await self.controller.queue.add("https://example.test/q2")
        calls = []

        async def fake_start_track(track, *, source="direct"):
            calls.append((track.original_url, source))

        self.controller._start_track = fake_start_track
        await self.controller._do_play_playlist(playlist)

        queued = await self.controller.queue.snapshot()
        self.assertEqual([x.url for x in queued], [
            "https://example.test/q1", "https://example.test/q2"
        ])
        self.assertEqual(calls, [("https://example.test/p1", "playlist")])
        self.assertEqual(
            [x.track.original_url for x in self.controller._active_tracklist.snapshot()],
            ["https://example.test/p2"],
        )
        self.assertEqual(
            self.controller._active_tracklist.info(),
            {"type": "playlist", "id": "pl1", "name": "Favorites"},
        )

    async def test_playlist_exhaustion_resumes_preserved_queue(self):
        self.controller = Controller(object(), "/tmp/unused-pcm", "/tmp/unused-snap")
        self.controller._worker.cancel()
        await asyncio.gather(self.controller._worker, return_exceptions=True)

        await self.controller.queue.add("https://example.test/q1")
        self.controller._active_tracklist = ActiveTrackList(
            [TrackListItem("p2", TrackRef("https://example.test/p2"))],
            source_type="playlist",
            source_id="pl1",
            source_name="Favorites",
        )
        calls = []

        async def fake_start_track(track, *, source="direct"):
            calls.append((track.original_url, source))

        self.controller._start_track = fake_start_track

        await self.controller._start_next_if_available()
        self.assertEqual(calls[-1], ("https://example.test/p2", "playlist"))
        self.assertIsNotNone(self.controller._active_tracklist)

        self.controller._current_track = None
        self.controller._playback_task = None
        await self.controller._start_next_if_available()
        self.assertEqual(calls[-1], ("https://example.test/q1", "queue"))
        self.assertIsNone(self.controller._active_tracklist)
        self.assertIsNone(self.controller._active_tracklist)
