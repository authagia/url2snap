from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class PlaybackState(str, Enum):
    IDLE = "idle"
    STARTING = "starting"
    PLAYING = "playing"
    STOPPING = "stopping"
    ERROR = "error"


class RepeatMode(str, Enum):
    NORMAL = "normal"
    REPEAT_ONE = "repeat_one"
    REPEAT_QUEUE = "repeat_queue"


class PlaybackResult(str, Enum):
    COMPLETED = "completed"
    STOPPED = "stopped"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass(frozen=True)
class TrackRef:
    """Stable input identity.

    This is what should be queued/persisted. It deliberately contains the
    original user-facing URL, not a resolver-produced URL that may expire.
    """

    original_url: str


@dataclass(frozen=True)
class ResolvedSource:
    """Ephemeral source handed to the player.

    A resolver may produce a temporary/expiring URL here. Do not persist this
    object as the identity of a queue item; re-resolve TrackRef when needed.
    """

    url: str
    headers: dict[str, str] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    title: Optional[str] = None
    duration: Optional[float] = None
    is_live: bool = False


@dataclass
class TrackListItem:
    """An ordered list item backed by a stable TrackRef."""

    id: str
    track: TrackRef

    @property
    def url(self) -> str:
        return self.track.original_url


@dataclass
class QueueItem(TrackListItem):
    """Queue-specific view of a TrackList item."""


@dataclass
class PlaylistItem(TrackListItem):
    """Playlist-specific view of a TrackList item."""


@dataclass
class Playlist:
    """Persistable named collection of ordered PlaylistItems."""

    id: str
    name: str
    items: list[PlaylistItem] = field(default_factory=list)
