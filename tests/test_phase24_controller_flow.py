import asyncio

import pytest

from core.controller import Controller
from core.error_policy import RetryErrorPolicy, SkipErrorPolicy
from core.models import PlaybackResult, PlaybackState, RepeatMode, TrackRef


async def stopped_controller(**kwargs):
    controller = Controller(object(), "/tmp/unused-snap", **kwargs)
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    return controller


@pytest.mark.asyncio
async def test_manual_skip_repeat_queue_moves_current_track_to_tail():
    controller = await stopped_controller()
    controller._repeat_mode = RepeatMode.REPEAT_QUEUE

    current = TrackRef("https://example.test/current")
    controller._current_track = current
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-21T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task

    await controller.queue.add("https://example.test/next")
    started = []

    async def fake_start_next():
        item = await controller.queue.pop_next()
        if item is not None:
            started.append(item.url)

    controller._start_next_if_available = fake_start_next

    async def fake_record(*args, **kwargs):
        return None
    controller._record_history = fake_record

    await controller._do_skip()

    queued = await controller.queue.snapshot()
    assert [item.url for item in queued] == ["https://example.test/current"]
    assert started == ["https://example.test/next"]


@pytest.mark.asyncio
async def test_playback_error_skip_repeat_queue_requeues_current_track():
    controller = await stopped_controller(error_policy=SkipErrorPolicy())
    controller._repeat_mode = RepeatMode.REPEAT_QUEUE

    current = TrackRef("https://example.test/current")
    controller._current_track = current
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-21T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.ERROR

    await controller.queue.add("https://example.test/next")
    started = []

    async def fake_start_next():
        item = await controller.queue.pop_next()
        if item is not None:
            started.append(item.url)

    controller._start_next_if_available = fake_start_next

    async def fake_record(*args, **kwargs):
        return None
    controller._record_history = fake_record

    await controller._do_finished(controller._generation)

    queued = await controller.queue.snapshot()
    assert [item.url for item in queued] == ["https://example.test/current"]
    assert started == ["https://example.test/next"]


@pytest.mark.asyncio
async def test_repeat_one_error_starts_same_track_again_without_nested_handler():
    controller = await stopped_controller(error_policy=SkipErrorPolicy())
    controller._repeat_mode = RepeatMode.REPEAT_ONE

    current = TrackRef("https://example.test/current")
    controller._current_track = current
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-21T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.ERROR

    starts = []

    async def fake_start_track(track, *, source="direct"):
        starts.append((track.original_url, source))
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-21T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    controller._start_track = fake_start_track

    async def fake_record(*args, **kwargs):
        return None
    controller._record_history = fake_record

    await controller._do_finished(controller._generation)

    assert starts == [(current.original_url, "queue")]
    assert controller._current_track == current
    assert controller._current_retry_count == 0


@pytest.mark.asyncio
async def test_completed_repeat_queue_requeues_current_track():
    controller = await stopped_controller()
    controller._repeat_mode = RepeatMode.REPEAT_QUEUE

    current = TrackRef("https://example.test/current")
    controller._current_track = current
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-21T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.COMPLETED

    await controller.queue.add("https://example.test/next")
    started = []

    async def fake_start_next():
        item = await controller.queue.pop_next()
        if item is not None:
            started.append(item.url)

    controller._start_next_if_available = fake_start_next

    async def fake_record(*args, **kwargs):
        return None
    controller._record_history = fake_record

    await controller._do_finished(controller._generation)

    queued = await controller.queue.snapshot()
    assert [item.url for item in queued] == ["https://example.test/current"]
    assert started == ["https://example.test/next"]


@pytest.mark.asyncio
async def test_completed_repeat_one_restarts_current_track():
    controller = await stopped_controller()
    controller._repeat_mode = RepeatMode.REPEAT_ONE

    current = TrackRef("https://example.test/current")
    controller._current_track = current
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-21T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.COMPLETED

    starts = []

    async def fake_start_track(track, *, source="direct"):
        starts.append((track.original_url, source))
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-21T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    controller._start_track = fake_start_track

    async def fake_record(*args, **kwargs):
        return None
    controller._record_history = fake_record

    await controller._do_finished(controller._generation)

    assert starts == [(current.original_url, "queue")]
    assert controller._current_track == current
