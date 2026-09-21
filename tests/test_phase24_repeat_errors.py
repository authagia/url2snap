import asyncio

import pytest

from core.controller import Controller
from core.error_policy import SkipErrorPolicy
from core.models import PlaybackResult, PlaybackState, RepeatMode, TrackRef


async def _stop_controller_worker(controller):
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_repeat_queue_requeues_resolver_failure_and_starts_next_item():
    controller = Controller(object(), "/tmp/unused-snap", error_policy=SkipErrorPolicy())
    await _stop_controller_worker(controller)
    await controller.set_repeat_mode(RepeatMode.REPEAT_QUEUE)
    await controller.queue.add("https://example.test/bad")
    await controller.queue.add("https://example.test/good")

    starts = []

    async def fake_start_track(track, *, source="direct"):
        starts.append((track.original_url, source))
        if track.original_url.endswith("/bad"):
            raise ValueError("resolver failure")

    controller._start_track = fake_start_track
    try:
        await controller._start_next_if_available()

        assert starts == [
            ("https://example.test/bad", "queue"),
            ("https://example.test/good", "queue"),
        ]
        queued = await controller.queue.snapshot()
        assert [item.url for item in queued] == ["https://example.test/bad"]
    finally:
        await controller.metadata.close()


@pytest.mark.asyncio
async def test_repeat_queue_requeues_playback_error_after_skip_policy():
    controller = Controller(object(), "/tmp/unused-snap", error_policy=SkipErrorPolicy())
    await _stop_controller_worker(controller)
    await controller.set_repeat_mode(RepeatMode.REPEAT_QUEUE)

    track = TrackRef("https://example.test/bad")
    controller._current_track = track
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-20T12:00:00+00:00"
    controller.session.last_result = PlaybackResult.ERROR

    async def fake_start_next():
        return None

    controller._start_next_if_available = fake_start_next
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    generation = controller._generation

    try:
        await controller._do_finished(generation)
        queued = await controller.queue.snapshot()
        assert [item.url for item in queued] == [track.original_url]
        assert controller._current_track is None
    finally:
        await controller.metadata.close()


@pytest.mark.asyncio
async def test_repeat_queue_requeues_explicitly_skipped_queue_item():
    controller = Controller(object(), "/tmp/unused-snap")
    await _stop_controller_worker(controller)
    await controller.set_repeat_mode(RepeatMode.REPEAT_QUEUE)

    track = TrackRef("https://example.test/skipped")
    controller._current_track = track
    controller._current_source = "queue"
    controller.session.last_result = PlaybackResult.SKIPPED

    starts = []

    async def fake_start_next():
        starts.append(True)

    controller._start_next_if_available = fake_start_next

    try:
        await controller._do_skip()
        queued = await controller.queue.snapshot()
        assert [item.url for item in queued] == [track.original_url]
        assert starts == [True]
    finally:
        await controller.metadata.close()
