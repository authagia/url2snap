import asyncio
import json
from contextlib import suppress

import pytest

from core.events import EventBus
from core.metadata import MediaMetadata, MetadataWorker, YtDlpMetadataProvider


class FakeProcess:
    def __init__(self, payload: dict, returncode: int = 0, stderr: bytes = b""):
        self.returncode = returncode
        self._payload = json.dumps(payload).encode()
        self._stderr = stderr
        self.killed = False
        self.waited = False

    async def communicate(self):
        return self._payload, self._stderr

    def kill(self):
        self.killed = True

    async def wait(self):
        self.waited = True


@pytest.mark.asyncio
async def test_ytdlp_metadata_provider_never_requests_media_url(monkeypatch):
    calls = {}
    process = FakeProcess(
        {
            "title": "Song",
            "artist": "Artist",
            "album": "Album",
            "duration": 123.4,
            "thumbnail": "https://img.example/cover.jpg",
            "is_live": False,
        }
    )

    async def fake_create(*args, **kwargs):
        calls["args"] = args
        return process

    monkeypatch.setattr("core.metadata.shutil.which", lambda _: "/usr/bin/yt-dlp")
    monkeypatch.setattr("core.metadata.asyncio.create_subprocess_exec", fake_create)

    provider = YtDlpMetadataProvider(executable="yt-dlp", timeout=5)
    result = await provider.fetch("https://www.youtube.com/watch?v=abc")

    assert calls["args"] == (
        "yt-dlp",
        "--dump-single-json",
        "--skip-download",
        "--no-playlist",
        "--",
        "https://www.youtube.com/watch?v=abc",
    )
    assert result == MediaMetadata(
        url="https://www.youtube.com/watch?v=abc",
        title="Song",
        artist="Artist",
        album="Album",
        duration=123.4,
        artwork_url="https://img.example/cover.jpg",
        is_live=False,
    )


@pytest.mark.asyncio
async def test_metadata_worker_deduplicates_and_emits_update():
    calls = []

    class Provider:
        async def fetch(self, url):
            calls.append(url)
            await asyncio.sleep(0)
            return MediaMetadata(url=url, title="Song")

    bus = EventBus()
    subscriber = await bus.subscribe()
    worker = MetadataWorker(Provider(), bus, worker_count=1)
    await worker.start()
    try:
        assert worker.request("https://example.test/a")
        assert not worker.request("https://example.test/a")

        while worker.get("https://example.test/a") is None:
            await asyncio.sleep(0)

        assert calls == ["https://example.test/a"]
        event = await asyncio.wait_for(subscriber.get(), timeout=1)
        assert event.type == "metadata.updated"
        assert event.data["metadata"]["title"] == "Song"
    finally:
        await worker.close()
        await bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_metadata_worker_is_best_effort_and_reports_errors():
    class Provider:
        async def fetch(self, url):
            raise RuntimeError("boom")

    bus = EventBus()
    subscriber = await bus.subscribe()
    worker = MetadataWorker(Provider(), bus, worker_count=1)
    await worker.start()
    try:
        assert worker.request("https://example.test/fail")
        event = await asyncio.wait_for(subscriber.get(), timeout=1)
        assert event.type == "metadata.error"
        assert event.data == {
            "url": "https://example.test/fail",
            "error": "boom",
        }
        assert worker.get("https://example.test/fail") is None
    finally:
        await worker.close()
        await bus.unsubscribe(subscriber)


@pytest.mark.asyncio
async def test_metadata_worker_can_be_closed_cleanly():
    class Provider:
        async def fetch(self, url):
            await asyncio.sleep(60)
            return MediaMetadata(url=url)

    bus = EventBus()
    worker = MetadataWorker(Provider(), bus, worker_count=2)
    await worker.start()
    worker.request("https://example.test/a")
    worker.request("https://example.test/b")
    await worker.close()
    assert worker._workers == []

    with suppress(Exception):
        await bus.unsubscribe(await bus.subscribe())


@pytest.mark.asyncio
async def test_controller_prefetches_enqueued_url_and_exposes_metadata():
    from core.controller import Controller
    from core.models import TrackRef

    class FakeMetadataWorker:
        def __init__(self):
            self.started_urls = None
            self.requested = []
            self.metadata = {}

        async def start(self, urls=None):
            self.started_urls = list(urls or [])

        async def close(self):
            return None

        def request(self, url, *, force=False):
            self.requested.append((url, force))
            return True

        def get(self, url):
            return self.metadata.get(url)

    worker = FakeMetadataWorker()
    controller = Controller(object(), '/tmp/unused-snap', metadata_worker=worker)
    try:
        await controller.startup()
        item = await controller.enqueue('https://example.test/song')
        assert item['metadata'] is None
        assert worker.requested == [('https://example.test/song', False)]

        worker.metadata['https://example.test/song'] = MediaMetadata(
            url='https://example.test/song',
            title='Song',
            artist='Artist',
            duration=10.0,
        )
        queue = await controller.queue_snapshot()
        assert queue[0]['metadata']['title'] == 'Song'
        assert queue[0]['metadata']['artist'] == 'Artist'

        controller._current_track = TrackRef('https://example.test/song')
        controller._current_started_at = '2026-09-20T12:00:00+00:00'
        status = await controller.status()
        assert status['current']['title'] == 'Song'
        assert status['current']['duration'] == 10.0
    finally:
        controller._worker.cancel()
        await asyncio.gather(controller._worker, return_exceptions=True)
        await controller.close()
