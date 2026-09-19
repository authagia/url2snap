import asyncio
import unittest

from core.controller import Controller
from core.models import PlaybackResult, PlaybackState, Playlist, PlaylistItem, TrackRef, TrackListItem
from core.tracklist import ActiveTrackList


class InMemoryPlaylistRepository:
    def __init__(self, playlists):
        self.playlists = {p.id: p for p in playlists}

    async def get(self, playlist_id):
        return self.playlists.get(playlist_id)


class TestPhase14(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        controller = getattr(self, "controller", None)
        if controller is not None:
            controller._worker.cancel()
            await asyncio.gather(controller._worker, return_exceptions=True)

    def test_active_tracklist_snapshots_playlist_items(self):
        playlist = Playlist(
            id="pl1",
            name="Favorites",
            items=[
                PlaylistItem("p1", TrackRef("https://example.test/p1")),
                PlaylistItem("p2", TrackRef("https://example.test/p2")),
            ],
        )
        active = ActiveTrackList.from_playlist(playlist)

        playlist.items.clear()

        first = active.pop_next()
        second = active.pop_next()
        assert first == TrackListItem("p1", TrackRef("https://example.test/p1"))
        assert second == TrackListItem("p2", TrackRef("https://example.test/p2"))
        assert active.pop_next() is None
        assert active.info() == {"type": "playlist", "id": "pl1", "name": "Favorites"}

    async def test_playlist_activation_uses_active_tracklist_and_preserves_queue(self):
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
        assert [item.url for item in queued] == [
            "https://example.test/q1", "https://example.test/q2"
        ]
        assert calls == [("https://example.test/p1", "playlist")]
        assert isinstance(self.controller._active_tracklist, ActiveTrackList)
        assert [item.url for item in self.controller._active_tracklist.snapshot()] == [
            "https://example.test/p2"
        ]
        assert self.controller.status is not None

    async def test_no_next_after_playback_error_returns_to_idle(self):
        class FakeSession:
            # A terminal mpv failure leaves PlaybackSession idle after it
            # returns ERROR; this fake models that public post-completion state.
            state = PlaybackState.IDLE
            current = None
            last_result = PlaybackResult.ERROR

            async def stop(self, result=PlaybackResult.STOPPED, silence=False):
                raise AssertionError("terminal playback task must not be stopped")

        self.controller = Controller(object(), "/tmp/unused-snap")
        self.controller.session = FakeSession()
        self.controller._worker.cancel()
        await asyncio.gather(self.controller._worker, return_exceptions=True)

        self.controller._current_track = TrackRef("https://example.test/bad")
        self.controller._current_source = "queue"
        self.controller._current_started_at = "2026-09-19T00:00:00+00:00"
        self.controller._playback_task = asyncio.create_task(asyncio.sleep(0))
        await self.controller._playback_task

        await self.controller._do_finished(self.controller._generation)

        assert self.controller._current_track is None
        assert self.controller._playback_task is None
        assert self.controller.session.state == PlaybackState.IDLE
