from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AppEvent:
    """A lightweight state-change notification.

    Events are hints that let clients invalidate their snapshot and fetch the
    current state. They are deliberately not a durable event log.
    """

    type: str
    data: dict[str, Any]


class EventBus:
    """In-process pub/sub for transient SSE notifications."""

    def __init__(self, *, max_queue_size: int = 100):
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be > 0")
        self.max_queue_size = max_queue_size
        self._subscribers: set[asyncio.Queue[AppEvent]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue[AppEvent]:
        queue: asyncio.Queue[AppEvent] = asyncio.Queue(maxsize=self.max_queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[AppEvent]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def publish(self, event_type: str, data: dict[str, Any] | None = None) -> None:
        event = AppEvent(event_type, data or {})
        async with self._lock:
            subscribers = tuple(self._subscribers)

        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # SSE is only an invalidation channel. Drop the oldest event so
                # a slow consumer eventually receives the latest notification.
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    pass
