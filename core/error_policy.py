from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from core.models import PlaybackResult, TrackRef


class ErrorAction(str, Enum):
    RETRY = "retry"
    SKIP = "skip"
    STOP = "stop"


@dataclass(frozen=True)
class ErrorContext:
    track: TrackRef
    source: str
    attempt: int
    result: PlaybackResult = PlaybackResult.ERROR
    error: BaseException | None = None


class ErrorPolicy(Protocol):
    async def decide(self, context: ErrorContext) -> ErrorAction:
        """Choose what to do with one failed playback attempt."""


class SkipErrorPolicy:
    """Initial/default policy: every error is skipped."""

    name = "skip"

    async def decide(self, context: ErrorContext) -> ErrorAction:
        return ErrorAction.SKIP


class RetryErrorPolicy:
    """Retry failed attempts up to max_retries, then skip."""

    name = "retry"

    def __init__(self, max_retries: int = 2, retry_delay: float = 0.0):
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_delay < 0:
            raise ValueError("retry_delay must be >= 0")
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def decide(self, context: ErrorContext) -> ErrorAction:
        if context.attempt < self.max_retries:
            if self.retry_delay:
                await asyncio.sleep(self.retry_delay)
            return ErrorAction.RETRY
        return ErrorAction.SKIP
