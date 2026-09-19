import asyncio
import os
import stat
import tempfile
import unittest
from pathlib import Path

from core.controller import Controller
from core.models import PlaybackResult, PlaybackState, TrackRef
from output.fifo import ensure_fifo


class TestPhase13(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        # Tests that create a Controller explicitly shut down its worker.
        controller = getattr(self, "controller", None)
        if controller is not None:
            await controller.close()

    async def test_controller_command_dispatch_is_coordinator_owned(self):
        self.controller = Controller(object(), "/tmp/unused-snap")
        self.assertIs(self.controller._worker, self.controller._coordinator.worker)
        self.assertIs(self.controller._commands, self.controller._coordinator.commands)

    async def test_controller_close_stops_active_playback(self):
        class FakeSession:
            state = PlaybackState.PLAYING
            current = object()
            last_result = PlaybackResult.COMPLETED

            async def stop(self, result=PlaybackResult.STOPPED, silence=False):
                self.stopped = (result, silence)
                self.state = PlaybackState.IDLE
                self.current = None

        class FakeTask:
            done = True

        self.controller = Controller(object(), "/tmp/unused-snap")
        self.controller.session = FakeSession()
        self.controller._current_track = TrackRef("https://example.test/a")
        self.controller._playback_task = asyncio.create_task(asyncio.sleep(0))
        await self.controller._playback_task

        recorded = []

        async def record(track, result, started_at=None):
            recorded.append((track.original_url, result))

        self.controller._record_history = record
        await self.controller.close()

        self.assertEqual(recorded, [("https://example.test/a", PlaybackResult.COMPLETED)])
        self.assertTrue(self.controller._worker.done())


def test_ensure_fifo_creates_parent_and_fifo():
    with tempfile.TemporaryDirectory() as tmp:
        fifo = Path(tmp) / "runtime" / "snapfifo"
        ensure_fifo(str(fifo))
        assert fifo.exists()
        assert stat.S_ISFIFO(os.stat(fifo).st_mode)


def test_ensure_fifo_rejects_regular_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "snapfifo"
        path.write_text("not a fifo")
        try:
            ensure_fifo(str(path))
        except RuntimeError as exc:
            assert "not a FIFO" in str(exc)
        else:
            raise AssertionError("regular file should be rejected")
