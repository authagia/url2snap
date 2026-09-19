from abc import ABC, abstractmethod

from core.models import ResolvedSource, TrackRef


class Resolver(ABC):
    """Turn a stable TrackRef into an ephemeral playable source."""

    @abstractmethod
    def can_handle(self, track: TrackRef) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def resolve(self, track: TrackRef) -> ResolvedSource:
        raise NotImplementedError
