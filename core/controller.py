import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from core.error_policy import ErrorAction, ErrorContext, ErrorPolicy, SkipErrorPolicy
from core.events import EventBus
from core.history import HistoryRepository, new_history_entry
from core.metadata import MetadataWorker, YtDlpMetadataProvider
from core.models import PlaybackResult, PlaybackState, Playlist, PlaylistItem, RepeatMode, ResolvedSource, TrackRef, TrackListItem
from core.playlists import PlaylistNotFound, PlaylistRepository
from core.queue import PlaybackQueue, QueueRepository
from core.tracklist import ActiveTrackList
from core.session import PlaybackSession
from resolver.chain import ResolverChain
from core.playback import PlaybackCoordinator

log = logging.getLogger(__name__)


@dataclass
class _PlaybackContext:
    """Mutable state for the currently owned playback attempt.

    The controller exposes compatibility properties for these fields because
    tests and a few internal helpers historically accessed them directly. The
    context itself owns the state transitions so clearing/replacing an attempt
    cannot accidentally reset only half of the fields.
    """

    generation: int = 0
    task: asyncio.Task | None = None
    track: TrackRef | None = None
    media: ResolvedSource | None = None
    source: str | None = None
    started_at: str | None = None
    retry_count: int = 0

    def has_current(self) -> bool:
        return self.track is not None or self.task is not None

    def invalidate(self) -> int:
        self.generation += 1
        return self.generation

    def begin(
        self,
        track: TrackRef,
        media: ResolvedSource,
        source: str,
        started_at: str,
    ) -> int:
        self.generation += 1
        self.track = track
        self.media = media
        self.source = source
        self.started_at = started_at
        self.retry_count = 0
        self.task = None
        return self.generation

    def clear(self) -> tuple[TrackRef | None, int]:
        previous_track = self.track
        previous_generation = self.generation
        self.task = None
        self.track = None
        self.media = None
        self.source = None
        self.started_at = None
        self.retry_count = 0
        return previous_track, previous_generation


class Controller:
    """Serializes commands and owns queue/playback lifecycle decisions."""

    def __init__(
        self,
        resolver: ResolverChain,
        snap_fifo: str,
        mpv_bin: str = "mpv",
        history: HistoryRepository | None = None,
        playlists: PlaylistRepository | None = None,
        queue_repository: QueueRepository | None = None,
        error_policy: ErrorPolicy | None = None,
        metadata_worker: MetadataWorker | None = None,
        ytdlp_bin: str = "yt-dlp",
        ytdlp_metadata_timeout: float = 20.0,
        metadata_workers: int = 2,
        metadata_error_cooldown: float = 30.0,
        metadata_request_interval: float = 1.0,
    ):
        self.resolver = resolver
        self.queue = PlaybackQueue(repository=queue_repository)
        self.session = PlaybackSession(snap_fifo, mpv_bin)
        self.history = history
        self.playlists = playlists
        self.error_policy = error_policy or SkipErrorPolicy()

        self.events = EventBus()
        self.metadata = metadata_worker or MetadataWorker(
            YtDlpMetadataProvider(executable=ytdlp_bin, timeout=ytdlp_metadata_timeout),
            self.events,
            worker_count=metadata_workers,
            error_cooldown=metadata_error_cooldown,
            request_interval=metadata_request_interval,
        )

        self._coordinator = PlaybackCoordinator(self)
        self._commands = self._coordinator.commands
        self._worker = self._coordinator.worker
        self._playback = _PlaybackContext()
        self._repeat_mode = RepeatMode.NORMAL

        # A transient playback TrackList. It is owned and consumed by
        # the coordinator; persistent Queue/Playlist state is kept separate.
        self._active_tracklist: ActiveTrackList | None = None
        self._started = False

    # Compatibility accessors keep the existing private test seam while the
    # playback state itself now has one owner.
    @property
    def _playback_task(self):
        return self._playback.task

    @_playback_task.setter
    def _playback_task(self, value):
        self._playback.task = value

    @property
    def _generation(self):
        return self._playback.generation

    @_generation.setter
    def _generation(self, value):
        self._playback.generation = value

    @property
    def _current_track(self):
        return self._playback.track

    @_current_track.setter
    def _current_track(self, value):
        self._playback.track = value

    @property
    def _current_media(self):
        return self._playback.media

    @_current_media.setter
    def _current_media(self, value):
        self._playback.media = value

    @property
    def _current_source(self):
        return self._playback.source

    @_current_source.setter
    def _current_source(self, value):
        self._playback.source = value

    @property
    def _current_started_at(self):
        return self._playback.started_at

    @_current_started_at.setter
    def _current_started_at(self, value):
        self._playback.started_at = value

    @property
    def _current_retry_count(self):
        return self._playback.retry_count

    @_current_retry_count.setter
    def _current_retry_count(self, value):
        self._playback.retry_count = value

    async def startup(self):
        """Load persisted waiting state without automatically resuming playback."""
        if self._started:
            return
        await self.queue.load()
        await self.metadata.start([item.url for item in await self.queue.snapshot()])
        self._started = True

    async def _submit(self, command: str, payload=None) -> str:
        return await self._coordinator.submit(command, payload)

    async def close(self):
        """Stop active playback and then shut down the command worker."""
        try:
            if self._current_track is not None or self._playback_task is not None:
                await self._do_stop()
        finally:
            await self.metadata.close()
            await self._coordinator.close()

    async def _record_history(
        self,
        track: TrackRef | None,
        result: PlaybackResult,
        started_at: str | None = None,
    ):
        if self.history is None or track is None:
            return
        entry = new_history_entry(
            track=track,
            result=result,
            started_at=started_at or datetime.now(timezone.utc).isoformat(),
        )
        try:
            await self.history.add(entry)
            await self.events.publish("state.changed", {"resources": ["history"]})
        except Exception:
            log.exception("failed to record history")

    def _has_current_playback(self) -> bool:
        return self._playback.has_current()

    def _request_metadata(self, urls):
        for url in urls:
            self.metadata.request(url)

    def _clear_active_tracklist(self):
        self._active_tracklist = None

    async def _clear_current(self):
        previous_track, previous_generation = self._playback.clear()
        if previous_track is not None:
            await self.events.publish(
                "playback.stopped",
                {"url": previous_track.original_url, "generation": previous_generation},
            )
        await self.events.publish("state.changed", {"resources": ["status"]})

    async def _stop_current(self, result: PlaybackResult, *, silence: bool):
        """End the current attempt and record the terminal result before clearing it."""
        track = self._current_track
        started_at = self._current_started_at
        task = self._playback_task

        self._playback.invalidate()
        if task is not None and not task.done():
            await self.session.stop(result, silence=silence)
            await asyncio.gather(task, return_exceptions=True)
            actual_result = result
        else:
            # The playback task may have reached a terminal state just before
            # this command was processed. Do not mislabel a completed/error
            # attempt as stopped merely because its finished event is pending.
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            actual_result = self.session.last_result or result

        await self._record_history(track, actual_result, started_at)
        await self._clear_current()

    async def _start_track(self, track: TrackRef, *, source: str = "direct"):
        """Resolve one stable TrackRef and launch one playback attempt.

        This is intentionally policy-free. Callers that need retry/skip
        semantics use _start_track_with_policy() around it.
        """
        self.metadata.request(
            track.original_url,
            force=self.metadata.get(track.original_url) is None,
        )
        media = await self.resolver.resolve(track)

        if self._has_current_playback():
            await self._stop_current(PlaybackResult.STOPPED, silence=True)

        started_at = datetime.now(timezone.utc).isoformat()
        generation = self._playback.begin(track, media, source, started_at)
        metadata = self.metadata.get(track.original_url)
        await self.events.publish(
            "playback.started",
            {
                "url": track.original_url,
                "generation": generation,
                "started_at": started_at,
                "metadata": metadata.to_dict() if metadata is not None else None,
            },
        )
        await self.events.publish("state.changed", {"resources": ["status"]})

        task = asyncio.create_task(self.session.play(media))
        self._playback.task = task
        self._watch_playback_task(task, generation)

    def _watch_playback_task(self, task: asyncio.Task, generation: int):
        """Convert a finished player task into the serialized controller command."""

        def finished(_done_task: asyncio.Task):
            asyncio.create_task(self._commands.put(("finished", generation, None)))

        task.add_done_callback(finished)

    async def _decide_error_action(
        self,
        track: TrackRef,
        source: str,
        attempt: int,
        *,
        error: BaseException | None = None,
    ) -> ErrorAction:
        action = await self.error_policy.decide(
            ErrorContext(
                track=track,
                source=source,
                attempt=attempt,
                result=PlaybackResult.ERROR,
                error=error,
            )
        )
        log.warning(
            "playback failed for %s (attempt=%s, action=%s)",
            track.original_url,
            attempt + 1,
            action.value,
        )
        return action

    async def _start_track_with_policy(
        self,
        track: TrackRef,
        *,
        source: str,
        attempt: int = 0,
    ) -> ErrorAction | None:
        """Start a track, retrying start/resolve failures per ErrorPolicy.

        Returns None when playback was launched. Otherwise returns the
        terminal ErrorAction selected by the policy.
        """
        while True:
            try:
                await self._start_track(track, source=source)
                self._current_retry_count = attempt
                return None
            except Exception as exc:
                action = await self._decide_error_action(
                    track,
                    source,
                    attempt,
                    error=exc,
                )
                if action == ErrorAction.RETRY:
                    attempt += 1
                    continue
                await self._record_history(track, PlaybackResult.ERROR)
                return action

    async def _repeat_current(self, track: TrackRef, source: str):
        await self._clear_current()
        await self._start_track(track, source=source)

    async def _requeue_for_repeat_queue(self, track: TrackRef, source: str):
        if source == "playlist" and self._active_tracklist is not None:
            self._active_tracklist.append(
                TrackListItem(id=uuid.uuid4().hex[:12], track=track)
            )
        elif source == "queue":
            await self.queue.add(track.original_url)
            await self.events.publish("state.changed", {"resources": ["queue"]})

    async def _continue_after_skip(
        self,
        track: TrackRef,
        source: str,
        *,
        repeat_one: bool = False,
    ):
        """Apply repeat semantics after an item has been skipped."""
        if repeat_one and self._repeat_mode == RepeatMode.REPEAT_ONE:
            await self._restart_repeat_one(track, source)
            return
        if self._repeat_mode == RepeatMode.REPEAT_QUEUE:
            await self._requeue_for_repeat_queue(track, source)
        await self._start_next_if_available()

    async def _restart_repeat_one(self, track: TrackRef, source: str):
        """Keep retrying a failed item under repeat-one semantics.

        The caller has already cleared the terminal playback state. ErrorPolicy
        still applies to each individual start attempt; once its retry budget
        is exhausted, repeat-one starts another policy cycle for the same
        TrackRef, making the overall behavior unbounded while keeping the
        policy itself bounded.
        """
        while True:
            action = await self._start_track_with_policy(track, source=source)
            if action is None:
                return
            if action == ErrorAction.STOP:
                self._clear_active_tracklist()
                return
            await self._clear_current()

    async def _next_playback_item(self):
        """Pop the next item from the active plan, then the waiting queue."""
        while True:
            if self._active_tracklist is not None:
                item = self._active_tracklist.pop_next()
                if item is not None:
                    return item.track, self._active_tracklist.source_type, None
                self._clear_active_tracklist()
                continue

            item = await self.queue.pop_next()
            if item is None:
                return None
            await self.events.publish("state.changed", {"resources": ["queue"]})
            return item.track, "queue", item.id

    async def _start_next_if_available(self):
        """Start the next TrackRef, applying ErrorPolicy and repeat policy."""
        while True:
            next_item = await self._next_playback_item()
            if next_item is None:
                return
            track, source, item_id = next_item

            action = await self._start_track_with_policy(track, source=source)
            if action is None:
                return
            if action == ErrorAction.STOP:
                self._clear_active_tracklist()
                return

            # A start-time SKIP is a terminal skip. Repeat policy is handled at
            # this boundary instead of being duplicated inside the retry loop.
            if self._repeat_mode == RepeatMode.REPEAT_ONE:
                await self._restart_repeat_one(track, source)
                return
            if self._repeat_mode == RepeatMode.REPEAT_QUEUE:
                await self._requeue_for_repeat_queue(track, source)
            log.info("skipping unplayable item %s", item_id or track.original_url)
            # Continue this loop so repeat_queue can immediately revisit the
            # item at the tail without duplicating queue-advance logic.

    async def _finish_error(self, track: TrackRef, source: str):
        action = await self._decide_error_action(
            track,
            source,
            self._current_retry_count,
        )
        if action == ErrorAction.RETRY:
            attempt = self._current_retry_count + 1
            await self._clear_current()
            action = await self._start_track_with_policy(
                track,
                source=source,
                attempt=attempt,
            )
            if action is None:
                return
            if action == ErrorAction.STOP:
                self._clear_active_tracklist()
                return
            # Retry-start failure reached a terminal SKIP. Continue through
            # the same repeat semantics as a playback ERROR -> SKIP.
            await self._continue_after_skip(track, source, repeat_one=True)
            return

        await self._clear_current()
        if action == ErrorAction.STOP:
            self._clear_active_tracklist()
            return

        # ErrorAction.SKIP is terminal for this attempt. Repeat policy decides
        # whether the same track should be retried or moved to the tail.
        await self._continue_after_skip(track, source, repeat_one=True)

    async def _finish_completed(self, track: TrackRef, source: str):
        if self._repeat_mode == RepeatMode.REPEAT_ONE:
            await self._repeat_current(track, source)
            return

        await self._clear_current()
        if self._repeat_mode == RepeatMode.REPEAT_QUEUE:
            await self._requeue_for_repeat_queue(track, source)
        await self._start_next_if_available()

    async def _do_play(self, url: str):
        await self._start_track(TrackRef(original_url=url), source="direct")
        # A direct play is a one-off override; do not resume a previously
        # activated playlist after it ends. The resolver is run before the
        # current playback is stopped, so a bad direct URL does not destroy
        # the active playback plan.
        self._clear_active_tracklist()

    async def _do_play_playlist(self, playlist: Playlist):
        # Resolve before replacing current playback. If the first item cannot
        # be resolved, the current track and existing active plan stay intact.
        active = ActiveTrackList.from_playlist(playlist)
        self._request_metadata(item.url for item in active.snapshot())
        first = active.pop_next()
        assert first is not None
        await self._start_track(first.track, source="playlist")
        self._active_tracklist = active

    async def _do_stop(self):
        if not self._has_current_playback():
            self._clear_active_tracklist()
            return
        await self._stop_current(PlaybackResult.STOPPED, silence=False)
        self._clear_active_tracklist()

    async def _do_skip(self):
        if not self._has_current_playback():
            await self._start_next_if_available()
            return
        track = self._current_track
        source = self._current_source or "direct"
        assert track is not None
        await self._stop_current(PlaybackResult.SKIPPED, silence=True)
        await self._continue_after_skip(track, source)

    async def _do_enqueue(self):
        if self.session.state == PlaybackState.IDLE and not self.session.current and not self._has_current_playback():
            await self._start_next_if_available()

    async def _handle_playback_result(
        self,
        track: TrackRef,
        source: str,
        result: PlaybackResult,
    ):
        """Route one terminal playback result to its policy handler."""
        if result == PlaybackResult.ERROR:
            await self._finish_error(track, source)
            return
        if result == PlaybackResult.COMPLETED:
            await self._finish_completed(track, source)
            return

        # STOPPED and SKIPPED are both terminal, user-visible omissions.
        # Their caller already decided to end the current attempt, so only
        # advance through the configured repeat/queue semantics here.
        await self._clear_current()
        await self._continue_after_skip(track, source)

    async def _do_finished(self, generation: int):
        if generation != self._generation:
            return

        result = self.session.last_result
        if self._playback_task is not None:
            await asyncio.gather(self._playback_task, return_exceptions=True)
        self._playback_task = None

        track = self._current_track
        source = self._current_source or "direct"
        started_at = self._current_started_at
        if track is None or result is None:
            return

        await self._record_history(track, result, started_at)
        await self._handle_playback_result(track, source, result)

    async def play_playlist(self, playlist_id: str) -> str:
        playlist = await self._get_playlist_or_raise(playlist_id)
        if not playlist.items:
            raise ValueError("playlist is empty")
        return await self._submit("play_playlist", playlist)

    async def play(self, url: str) -> str:
        return await self._submit("play", url)

    async def stop(self) -> str:
        return await self._submit("stop")

    async def skip(self) -> str:
        return await self._submit("skip")

    async def set_repeat_mode(self, mode: RepeatMode):
        self._repeat_mode = mode
        await self.events.publish("state.changed", {"resources": ["repeat", "status"]})
        return mode

    async def repeat_mode(self):
        return self._repeat_mode

    async def enqueue(self, url: str):
        item = await self.queue.add(url)
        self._request_metadata([item.url])
        await self.events.publish("state.changed", {"resources": ["queue"]})
        await self._submit("queue")
        return self._queue_item_dict(item)

    async def enqueue_history(self, entry_id: str):
        if self.history is None:
            raise KeyError(entry_id)
        entry = await self.history.get(entry_id)
        if entry is None:
            raise KeyError(entry_id)
        item = await self.queue.add(entry.track.original_url)
        self._request_metadata([item.url])
        await self.events.publish("state.changed", {"resources": ["queue"]})
        await self._submit("queue")
        return self._queue_item_dict(item)

    @staticmethod
    def _history_entry_dict(entry):
        return {
            "id": entry.id,
            "url": entry.track.original_url,
            "result": entry.result.value,
            "started_at": entry.started_at,
            "ended_at": entry.ended_at,
        }

    async def history_snapshot(self, limit: int = 100):
        entries = await self.history.list(limit) if self.history is not None else []
        return [self._history_entry_dict(entry) for entry in entries]

    async def replay_history(self, entry_id: str):
        if self.history is None:
            raise KeyError(entry_id)
        entry = await self.history.get(entry_id)
        if entry is None:
            raise KeyError(entry_id)
        await self.play(entry.track.original_url)
        return self._history_entry_dict(entry)

    async def status(self):
        if self._active_tracklist is not None:
            playback_source = self._active_tracklist.info()
        elif self._current_source == "queue":
            playback_source = {"type": "queue"}
        elif self._current_source == "direct":
            playback_source = {"type": "direct"}
        else:
            playback_source = None

        active_remaining = len(self._active_tracklist) if self._active_tracklist is not None else 0

        current_track = self._current_track
        current_media = self._current_media
        current = None
        if current_track is not None:
            metadata = self.metadata.get(current_track.original_url)
            current = {
                "url": current_track.original_url,
                "title": (metadata.title if metadata is not None else None)
                or (current_media.title if current_media is not None else None),
                "artist": metadata.artist if metadata is not None else None,
                "album": metadata.album if metadata is not None else None,
                "duration": (metadata.duration if metadata is not None else None)
                if (metadata is not None and metadata.duration is not None)
                else (current_media.duration if current_media is not None else None),
                "artwork_url": metadata.artwork_url if metadata is not None else None,
                "is_live": metadata.is_live if metadata is not None else False,
            }

        return {
            "state": self.session.state.value,
            "current": current,
            "queue_length": len(await self.queue.snapshot()),
            "repeat_mode": self._repeat_mode.value,
            "playback_started_at": self._current_started_at,
            "playback_source": playback_source,
            "playback_list_remaining": active_remaining,
            "error_policy": getattr(self.error_policy, "name", self.error_policy.__class__.__name__),
        }


    @staticmethod
    def _playlist_dict(playlist):
        return {
            "id": playlist.id,
            "name": playlist.name,
            "items": [
                {"id": item.id, "url": item.track.original_url}
                for item in playlist.items
            ],
        }

    async def playlist_snapshot(self):
        if self.playlists is None:
            return []
        return [self._playlist_dict(p) for p in await self.playlists.list()]

    async def get_playlist(self, playlist_id: str):
        if self.playlists is None:
            raise KeyError(playlist_id)
        playlist = await self.playlists.get(playlist_id)
        if playlist is None:
            raise KeyError(playlist_id)
        return self._playlist_dict(playlist)

    async def create_playlist(self, name: str, urls: list[str] | None = None):
        if self.playlists is None:
            raise RuntimeError("playlist repository is not configured")
        tracks = [TrackRef(original_url=url) for url in (urls or [])]
        playlist = await self.playlists.create(name, tracks)
        await self.events.publish("state.changed", {"resources": ["playlists"]})
        return self._playlist_dict(playlist)

    async def delete_playlist(self, playlist_id: str):
        if self.playlists is None:
            raise KeyError(playlist_id)
        try:
            playlist = await self.playlists.delete(playlist_id)
        except PlaylistNotFound:
            raise KeyError(playlist_id)
        await self.events.publish("state.changed", {"resources": ["playlists"]})
        return self._playlist_dict(playlist)

    async def _get_playlist_or_raise(self, playlist_id: str):
        if self.playlists is None:
            raise KeyError(playlist_id)
        playlist = await self.playlists.get(playlist_id)
        if playlist is None:
            raise KeyError(playlist_id)
        return playlist

    async def add_playlist_track(self, playlist_id: str, url: str, index: int | None = None):
        playlist = await self._get_playlist_or_raise(playlist_id)
        item = PlaylistItem(id=uuid.uuid4().hex[:12], track=TrackRef(original_url=url))
        if index is None:
            playlist.items.append(item)
        else:
            if index < 0 or index > len(playlist.items):
                raise IndexError("playlist index out of range")
            playlist.items.insert(index, item)
        await self.playlists.save(playlist)
        self._request_metadata([item.url])
        await self.events.publish("state.changed", {"resources": ["playlists"]})
        return {"id": item.id, "url": item.track.original_url}

    async def remove_playlist_track(self, playlist_id: str, item_id: str):
        playlist = await self._get_playlist_or_raise(playlist_id)
        for i, item in enumerate(playlist.items):
            if item.id == item_id:
                removed = playlist.items.pop(i)
                await self.playlists.save(playlist)
                await self.events.publish("state.changed", {"resources": ["playlists"]})
                return {"id": removed.id, "url": removed.track.original_url}
        raise KeyError(item_id)

    async def move_playlist_track(self, playlist_id: str, item_id: str, index: int):
        playlist = await self._get_playlist_or_raise(playlist_id)
        if index < 0 or index >= len(playlist.items):
            raise IndexError("playlist index out of range")
        old_index = next((i for i, item in enumerate(playlist.items) if item.id == item_id), None)
        if old_index is None:
            raise KeyError(item_id)
        item = playlist.items.pop(old_index)
        playlist.items.insert(index, item)
        await self.playlists.save(playlist)
        await self.events.publish("state.changed", {"resources": ["playlists"]})
        return {"id": item.id, "url": item.track.original_url}

    async def replace_playlist(self, playlist_id: str, urls: list[str]):
        playlist = await self._get_playlist_or_raise(playlist_id)
        playlist.items = [
            PlaylistItem(id=uuid.uuid4().hex[:12], track=TrackRef(original_url=url))
            for url in urls
        ]
        await self.playlists.save(playlist)
        self._request_metadata(urls)
        await self.events.publish("state.changed", {"resources": ["playlists"]})
        return self._playlist_dict(playlist)

    def _queue_item_dict(self, item):
        metadata = self.metadata.get(item.url)
        return {
            "id": item.id,
            "url": item.url,
            "metadata": metadata.to_dict() if metadata is not None else None,
        }

    async def remove_queue_item(self, item_id: str):
        item = await self.queue.remove(item_id)
        await self.events.publish("state.changed", {"resources": ["queue"]})
        return self._queue_item_dict(item)

    async def move_queue_item(self, item_id: str, index: int):
        item = await self.queue.move(item_id, index)
        await self.events.publish("state.changed", {"resources": ["queue"]})
        return self._queue_item_dict(item)

    async def replace_queue(self, urls: list[str]):
        items = await self.queue.replace(urls)
        self._request_metadata(item.url for item in items)
        await self.events.publish("state.changed", {"resources": ["queue"]})
        if (
            self.session.state == PlaybackState.IDLE
            and self.session.current is None
            and self._current_track is None
            and self._playback_task is None
        ):
            await self._start_next_if_available()
        return [self._queue_item_dict(item) for item in items]

    async def queue_snapshot(self):
        return [self._queue_item_dict(item) for item in await self.queue.snapshot()]
