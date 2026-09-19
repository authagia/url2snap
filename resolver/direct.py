from core.models import ResolvedSource, TrackRef
from resolver.base import Resolver


class DirectResolver(Resolver):
    """Fallback resolver: let mpv decide whether the URL is playable."""

    def can_handle(self, track: TrackRef) -> bool:
        return True

    async def resolve(self, track: TrackRef) -> ResolvedSource:
        return ResolvedSource(url=track.original_url)
