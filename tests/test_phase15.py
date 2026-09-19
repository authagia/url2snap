import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from core.controller import Controller
from core.history import JsonHistoryRepository, new_history_entry
from core.models import PlaybackResult, TrackRef
from core.persistence import PersistenceError, atomic_write_json
from core.playlists import JsonPlaylistRepository
from core.queue import JsonQueueRepository, PlaybackQueue


@pytest.mark.asyncio
async def test_queue_persists_ids_and_order_and_legacy_array_is_accepted():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "queue.json"
        repo = JsonQueueRepository(path)
        queue = PlaybackQueue(repo)
        first = await queue.add("https://example.test/a")
        second = await queue.add("https://example.test/b")
        await queue.move(second.id, 0)

        loaded = PlaybackQueue(JsonQueueRepository(path))
        items = await loaded.load()
        assert [(x.id, x.url) for x in items] == [
            (second.id, "https://example.test/b"),
            (first.id, "https://example.test/a"),
        ]

        legacy = [
            {"id": "legacy", "track": {"original_url": "https://example.test/legacy"}}
        ]
        path.write_text(json.dumps(legacy), encoding="utf-8")
        migrated = PlaybackQueue(JsonQueueRepository(path))
        await migrated.load()
        assert (await migrated.snapshot())[0].id == "legacy"


@pytest.mark.asyncio
async def test_all_json_repositories_write_versioned_atomic_documents():
    with tempfile.TemporaryDirectory() as tmp:
        history_path = Path(tmp) / "history.json"
        playlist_path = Path(tmp) / "playlists.json"
        queue_path = Path(tmp) / "queue.json"

        history = JsonHistoryRepository(history_path)
        entry = new_history_entry(
            TrackRef("https://example.test/a"),
            PlaybackResult.COMPLETED,
            "2026-09-19T00:00:00+00:00",
        )
        await history.add(entry)
        playlist = JsonPlaylistRepository(playlist_path)
        await playlist.create("Favorites", [TrackRef("https://example.test/b")])
        queue = PlaybackQueue(JsonQueueRepository(queue_path))
        await queue.add("https://example.test/c")

        assert json.loads(history_path.read_text()) ["version"] == 1
        assert json.loads(playlist_path.read_text()) ["version"] == 1
        assert json.loads(queue_path.read_text()) ["version"] == 1
        assert not list(Path(tmp).glob(".*.tmp"))


@pytest.mark.asyncio
async def test_corrupt_persistence_fails_loudly_without_becoming_empty():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "queue.json"
        path.write_text('{"version": 1, "items": [', encoding="utf-8")
        with pytest.raises(PersistenceError):
            await JsonQueueRepository(path).load()

        assert path.read_text(encoding="utf-8") == '{"version": 1, "items": ['


@pytest.mark.asyncio
async def test_controller_startup_restores_waiting_queue_but_does_not_autoplay():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "queue.json"
        writer = PlaybackQueue(JsonQueueRepository(path))
        await writer.add("https://example.test/a")
        await writer.add("https://example.test/b")

        controller = Controller(
            object(),
            "/tmp/unused-snap",
            queue_repository=JsonQueueRepository(path),
        )
        try:
            # The command worker is intentionally not needed for startup.
            controller._worker.cancel()
            await asyncio.gather(controller._worker, return_exceptions=True)
            await controller.startup()

            snapshot = await controller.queue.snapshot()
            assert [item.url for item in snapshot] == [
                "https://example.test/a", "https://example.test/b"
            ]
            assert controller._current_track is None
            assert controller._playback_task is None
        finally:
            if not controller._coordinator.worker.done():
                controller._coordinator.worker.cancel()
            await asyncio.gather(controller._coordinator.worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_atomic_write_replaces_previous_document():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "document.json"
        await atomic_write_json(path, {"version": 1, "value": "old"})
        await atomic_write_json(path, {"version": 1, "value": "new"})
        assert json.loads(path.read_text()) == {"version": 1, "value": "new"}
