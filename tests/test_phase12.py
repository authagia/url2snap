import asyncio

import pytest

from core.controller import Controller
from core.error_policy import ErrorAction, ErrorContext, RetryErrorPolicy, SkipErrorPolicy
from core.models import PlaybackResult, RepeatMode, TrackRef


@pytest.mark.asyncio
async def test_skip_error_policy_preserves_initial_behavior():
    policy = SkipErrorPolicy()
    action = await policy.decide(
        ErrorContext(track=TrackRef("https://example.test/a"), source="queue", attempt=0)
    )
    assert action == ErrorAction.SKIP


@pytest.mark.asyncio
async def test_retry_error_policy_retries_then_skips():
    policy = RetryErrorPolicy(max_retries=2)
    track = TrackRef("https://example.test/a")

    actions = [
        await policy.decide(ErrorContext(track=track, source="queue", attempt=0)),
        await policy.decide(ErrorContext(track=track, source="queue", attempt=1)),
        await policy.decide(ErrorContext(track=track, source="queue", attempt=2)),
    ]

    assert actions == [ErrorAction.RETRY, ErrorAction.RETRY, ErrorAction.SKIP]


@pytest.mark.asyncio
async def test_controller_retries_resolver_failure_before_skipping():
    controller = Controller(object(), "/tmp/unused", error_policy=RetryErrorPolicy(max_retries=2))
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)

    await controller.queue.add("https://example.test/a")
    await controller.queue.add("https://example.test/b")

    attempts = 0
    started = []
    history = []

    async def fake_start_track(track, *, source="direct"):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("temporary resolver failure")
        started.append((track.original_url, source))

    controller._start_track = fake_start_track
    controller._record_history = lambda track, result, started_at=None: history.append(
        (track.original_url, result)
    )

    await controller._start_next_if_available()

    assert attempts == 3
    assert started == [("https://example.test/a", "queue")]
    assert history == []


@pytest.mark.asyncio
async def test_controller_playback_error_retries_same_track_then_skips():
    controller = Controller(object(), "/tmp/unused", error_policy=RetryErrorPolicy(max_retries=1))
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)

    track = TrackRef("https://example.test/a")
    controller._current_track = track
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-19T00:00:00+00:00"

    starts = []
    history = []

    async def fake_start_track(track, *, source="direct"):
        starts.append((track.original_url, source))
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-19T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    async def fake_record(track, result, started_at=None):
        history.append((track.original_url, result))

    controller._start_track = fake_start_track
    controller._record_history = fake_record

    # First failure: retry the same TrackRef.
    controller.session.last_result = PlaybackResult.ERROR
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    gen = controller._generation
    await controller._do_finished(gen)

    assert starts == [("https://example.test/a", "queue")]
    assert history == [("https://example.test/a", PlaybackResult.ERROR)]
    assert controller._current_track == track
    assert controller._current_retry_count == 1

    # Second failure: retry budget is exhausted, so the item is skipped.
    controller.session.last_result = PlaybackResult.ERROR
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    gen = controller._generation
    started_next = []

    async def fake_start_next():
        started_next.append(True)

    controller._start_next_if_available = fake_start_next
    await controller._do_finished(gen)

    assert history[-1] == ("https://example.test/a", PlaybackResult.ERROR)
    assert started_next == [True]
    assert controller._current_track is None

@pytest.mark.asyncio
async def test_controller_resolver_skip_uses_repeat_one_as_unbounded_retry():
    controller = Controller(object(), "/tmp/unused", error_policy=SkipErrorPolicy())
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    await controller.set_repeat_mode(RepeatMode.REPEAT_ONE)

    track = TrackRef("https://example.test/a")
    starts = 0

    async def fake_start_track(track, *, source="direct"):
        nonlocal starts
        starts += 1
        if starts < 3:
            raise ValueError("temporary resolver failure")
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-20T00:00:00+00:00"

    controller._start_track = fake_start_track
    await controller.queue.add(track.original_url)

    await controller._start_next_if_available()

    assert starts == 3
    assert controller._current_track == track


@pytest.mark.asyncio
async def test_controller_resolver_skip_uses_repeat_queue_and_continues():
    controller = Controller(object(), "/tmp/unused", error_policy=SkipErrorPolicy())
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    await controller.set_repeat_mode(RepeatMode.REPEAT_QUEUE)

    await controller.queue.add("https://example.test/a")
    await controller.queue.add("https://example.test/b")
    starts = []

    async def fake_start_track(track, *, source="direct"):
        starts.append(track.original_url)
        if track.original_url.endswith("/a"):
            raise ValueError("temporary resolver failure")
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-20T00:00:00+00:00"

    controller._start_track = fake_start_track

    await controller._start_next_if_available()

    assert starts == ["https://example.test/a", "https://example.test/b"]
    assert [item.url for item in await controller.queue.snapshot()] == [
        "https://example.test/a"
    ]
    assert controller._current_track == TrackRef("https://example.test/b")


@pytest.mark.asyncio
async def test_controller_playback_error_repeat_one_retries_until_start_succeeds():
    controller = Controller(object(), "/tmp/unused", error_policy=SkipErrorPolicy())
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    await controller.set_repeat_mode(RepeatMode.REPEAT_ONE)

    track = TrackRef("https://example.test/a")
    controller._current_track = track
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-20T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.ERROR

    starts = 0

    async def fake_start_track(track, *, source="direct"):
        nonlocal starts
        starts += 1
        if starts == 1:
            raise ValueError("temporary resolver failure")
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-20T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    controller._start_track = fake_start_track

    async def fake_record_history(*args, **kwargs):
        return None

    controller._record_history = fake_record_history

    await controller._do_finished(controller._generation)

    assert starts == 2
    assert controller._current_track == track


@pytest.mark.asyncio
async def test_controller_playback_error_repeat_queue_requeues_failed_track():
    controller = Controller(object(), "/tmp/unused", error_policy=SkipErrorPolicy())
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)
    await controller.set_repeat_mode(RepeatMode.REPEAT_QUEUE)

    await controller.queue.add("https://example.test/b")
    track = TrackRef("https://example.test/a")
    controller._current_track = track
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-20T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.ERROR

    starts = []

    async def fake_start_track(track, *, source="direct"):
        starts.append(track.original_url)
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-20T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    controller._start_track = fake_start_track

    async def fake_record_history(*args, **kwargs):
        return None

    controller._record_history = fake_record_history

    await controller._do_finished(controller._generation)

    assert starts == ["https://example.test/b"]
    assert [item.url for item in await controller.queue.snapshot()] == [
        "https://example.test/a"
    ]
    assert controller._current_track == TrackRef("https://example.test/b")


@pytest.mark.asyncio
async def test_controller_playback_error_retry_start_failure_reenters_error_policy():
    controller = Controller(object(), "/tmp/unused", error_policy=RetryErrorPolicy(max_retries=2))
    controller._worker.cancel()
    await asyncio.gather(controller._worker, return_exceptions=True)

    track = TrackRef("https://example.test/a")
    controller._current_track = track
    controller._current_source = "queue"
    controller._current_started_at = "2026-09-20T00:00:00+00:00"
    controller._playback_task = asyncio.create_task(asyncio.sleep(0))
    await controller._playback_task
    controller.session.last_result = PlaybackResult.ERROR

    policy_attempts = []
    real_policy = controller.error_policy

    async def decide(context):
        policy_attempts.append(context.attempt)
        return await real_policy.decide(context)

    controller.error_policy = type("CountingPolicy", (), {"decide": staticmethod(decide)})()

    starts = 0

    async def fake_start_track(track, *, source="direct"):
        nonlocal starts
        starts += 1
        if starts <= 1:
            raise ValueError("temporary resolver failure")
        controller._current_track = track
        controller._current_source = source
        controller._current_started_at = "2026-09-20T00:01:00+00:00"
        controller._generation += 1
        controller._playback_task = asyncio.create_task(asyncio.sleep(0))

    controller._start_track = fake_start_track

    await controller._do_finished(controller._generation)

    assert starts == 2
    assert policy_attempts == [0, 1]
    assert controller._current_track == track
    assert controller._current_retry_count == 2
