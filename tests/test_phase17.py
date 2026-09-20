import asyncio

import pytest
from fastapi.testclient import TestClient

from api.routes import create_app
from core.controller import Controller
from core.events import EventBus
from core.models import PlaybackState, TrackRef


@pytest.mark.asyncio
async def test_event_bus_drops_oldest_event_for_slow_subscriber():
    bus = EventBus(max_queue_size=2)
    queue = await bus.subscribe()
    try:
        await bus.publish("state.changed", {"n": 1})
        await bus.publish("state.changed", {"n": 2})
        await bus.publish("state.changed", {"n": 3})

        first = await queue.get()
        second = await queue.get()
        assert first.data == {"n": 2}
        assert second.data == {"n": 3}
    finally:
        await bus.unsubscribe(queue)


@pytest.mark.asyncio
async def test_controller_status_exposes_playback_started_at():
    controller = Controller(object(), "/tmp/unused-snap")
    try:
        controller.session.state = PlaybackState.PLAYING
        controller._current_track = TrackRef("https://example.test/a")
        controller._current_source = "direct"
        controller._current_started_at = "2026-09-20T12:34:56.000000+00:00"

        body = await controller.status()
        assert body["playback_started_at"] == "2026-09-20T12:34:56.000000+00:00"
        assert body["current"]["url"] == "https://example.test/a"
    finally:
        await controller.close()


