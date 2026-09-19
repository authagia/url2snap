from core.models import ResolvedSource, TrackRef
from resolver.base import Resolver


class ResolverChain:
    def __init__(self, resolvers: list[Resolver]):
        self._resolvers = resolvers

    async def resolve(self, track: TrackRef) -> ResolvedSource:
        for resolver in self._resolvers:
            if resolver.can_handle(track):
                return await resolver.resolve(track)
        raise ValueError(f"no resolver for URL: {track.original_url}")
