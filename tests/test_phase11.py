import asyncio

import pytest

from core.models import PlaybackResult, PlaybackState, ResolvedSource, TrackRef
from core.session import PlaybackSession
from player.mpv import MpvPlayer


class FakeProcess:
    def __init__(self, returncode: int):
        self.returncode = returncode
        self.stderr = asyncio.StreamReader()
        self.stderr.feed_eof()
        self.terminated = False

    async def wait(self):
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.terminated = True


@pytest.mark.asyncio
async def test_mpv_writes_directly_to_snap_fifo(monkeypatch):
    calls = {}
    process = FakeProcess(0)

    async def fake_create(*cmd, **kwargs):
        calls["cmd"] = cmd
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    player = MpvPlayer("/tmp/snapfifo", "mpv-test")

    await player.start(ResolvedSource("https://example.test/audio"))
    rc = await player.wait()

    assert rc == 0
    assert "--ao=pcm" in calls["cmd"]
    assert "--ao-pcm-waveheader=no" in calls["cmd"]
    assert "--ao-pcm-file=/tmp/snapfifo" in calls["cmd"]
    assert "--audio-format=s16" in calls["cmd"]
    assert "--audio-samplerate=48000" in calls["cmd"]
    assert "--audio-channels=stereo" in calls["cmd"]


@pytest.mark.asyncio
async def test_session_error_does_not_wait_for_pcm_pipeline(monkeypatch):
    class FakePlayer:
        def __init__(self, snap_fifo, mpv_bin):
            pass

        async def start(self, media):
            return None

        async def wait(self):
            return 1

        async def get_stderr(self):
            return "bad url"

        async def stop(self):
            return None

    silence_calls = []

    class FakeFifo:
        async def write_silence(self, duration_ms):
            silence_calls.append(duration_ms)

    monkeypatch.setattr("core.session.MpvPlayer", FakePlayer)

    session = PlaybackSession("/tmp/snapfifo")
    session._fifo = FakeFifo()

    result = await asyncio.wait_for(
        session.play(ResolvedSource("https://example.test/bad")),
        timeout=1,
    )

    assert result == PlaybackResult.ERROR
    assert session.last_result == PlaybackResult.ERROR
    assert session.current is None
    assert session.state == PlaybackState.IDLE
    assert silence_calls == [200]


class ErrorSession:
    state = PlaybackState.ERROR
    current = None
    last_result = PlaybackResult.ERROR

    async def stop(self, result=PlaybackResult.STOPPED, silence=False):
        raise AssertionError("terminal playback task must not be stopped")


@pytest.mark.asyncio
async def test_controller_error_path_still_skips_to_next_item():
    from core.controller import Controller
    controller = Controller(object(), "/tmp/unused-snap")
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    controller.session = ErrorSession()
    controller._current_track = TrackRef("https://example.test/bad")
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-19T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task

    await controller.queue.add("https://example.test/next")
    started = []

    async def fake_start_next():
        item = await controller.queue.pop_next()
        if item is not None:
            started.append(item.track.original_url)

    controller._start_next_if_available = fake_start_next
    history = []

    async def fake_record(track, result, started_at=None):
        history.append((track.original_url, result))

    controller._record_history = fake_record

    await controller._do_finished(controller._generation)

    assert history == [("https://example.test/bad", PlaybackResult.ERROR)]
    assert started == ["https://example.test/next"]
