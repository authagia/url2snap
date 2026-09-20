from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from core.events import EventBus

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MediaMetadata:
    """Non-ephemeral metadata for a stable source URL."""

    url: str
    title: str | None = None
    artist: str | None = None
    album: str | None = None
    duration: float | None = None
    artwork_url: str | None = None
    is_live: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetadataProvider(Protocol):
    async def fetch(self, url: str) -> MediaMetadata:
        """Fetch metadata without resolving an expiring playback URL."""


class YtDlpMetadataProvider:
    """Fetch metadata with yt-dlp, but never ask it for a media URL."""

    def __init__(self, *, executable: str = "yt-dlp", timeout: float = 20.0):
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        self.executable = executable
        self.timeout = timeout

    async def fetch(self, url: str) -> MediaMetadata:
        if shutil.which(self.executable) is None:
            raise RuntimeError(f"yt-dlp executable not found: {self.executable!r}")

        command = [
            self.executable,
            "--dump-single-json",
            "--skip-download",
            "--no-playlist",
            "--",
            url,
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise RuntimeError(f"failed to start yt-dlp: {exc}") from exc

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            raise RuntimeError(
                f"yt-dlp metadata lookup timed out after {self.timeout:g}s"
            ) from exc

        stdout_text = stdout.decode("utf-8", errors="replace").strip()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()

        if process.returncode != 0:
            detail = stderr_text or stdout_text
            message = f"yt-dlp metadata lookup exited with code {process.returncode}"
            if detail:
                message += f": {detail[-2000:]}"
            raise RuntimeError(message)

        if not stdout_text:
            raise RuntimeError("yt-dlp returned no metadata")

        try:
            info = json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("yt-dlp returned invalid JSON") from exc

        if not isinstance(info, dict):
            raise RuntimeError("yt-dlp returned unexpected metadata")

        return self._metadata_from_info(url, info)

    @staticmethod
    def _metadata_from_info(url: str, info: dict[str, Any]) -> MediaMetadata:
        artist = info.get("artist")
        if not isinstance(artist, str) or not artist.strip():
            artists = info.get("artists")
            if isinstance(artists, list):
                names = [str(item).strip() for item in artists if str(item).strip()]
                artist = ", ".join(names) or None
            else:
                artist = None
        artist = artist.strip() if isinstance(artist, str) and artist.strip() else None

        if artist is None:
            for key in ("channel", "uploader"):
                candidate = info.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    artist = candidate.strip()
                    break

        album = info.get("album")
        album = album.strip() if isinstance(album, str) and album.strip() else None

        title = info.get("title")
        title = title.strip() if isinstance(title, str) and title.strip() else None

        duration = info.get("duration")
        if isinstance(duration, bool):
            duration = None
        elif duration is not None:
            try:
                duration = float(duration)
            except (TypeError, ValueError):
                duration = None

        artwork_url = info.get("thumbnail")
        if not isinstance(artwork_url, str) or not artwork_url.strip():
            thumbnails = info.get("thumbnails")
            if isinstance(thumbnails, list):
                for item in reversed(thumbnails):
                    if isinstance(item, dict):
                        candidate = item.get("url")
                        if isinstance(candidate, str) and candidate.strip():
                            artwork_url = candidate
                            break
        artwork_url = (
            artwork_url.strip()
            if isinstance(artwork_url, str) and artwork_url.strip()
            else None
        )

        return MediaMetadata(
            url=url,
            title=title,
            artist=artist,
            album=album,
            duration=duration,
            artwork_url=artwork_url,
            is_live=bool(info.get("is_live")),
        )


class MetadataWorker:
    """Background metadata prefetcher with in-flight deduplication.

    Metadata lookup is explicitly best-effort and never part of playback's
    critical path. A failed provider lookup is surfaced through the EventBus.
    """

    ERROR_COOLDOWN = 30.0

    def __init__(
        self,
        provider: MetadataProvider,
        events: EventBus,
        *,
        worker_count: int = 2,
        error_cooldown: float = ERROR_COOLDOWN,
    ):
        if worker_count < 1:
            raise ValueError("worker_count must be > 0")
        if error_cooldown < 0:
            raise ValueError("error_cooldown must be >= 0")

        self.provider = provider
        self.events = events
        self.worker_count = worker_count
        self.error_cooldown = error_cooldown
        self._jobs: asyncio.Queue[str] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._pending: set[str] = set()
        self._cache: dict[str, MediaMetadata] = {}
        self._errors: dict[str, float] = {}
        self._closed = False

    async def start(self, urls: list[str] | None = None) -> None:
        if self._workers:
            return
        self._closed = False
        self._workers = [
            asyncio.create_task(self._run_worker(i), name=f"metadata-worker-{i}")
            for i in range(self.worker_count)
        ]
        for url in urls or []:
            self.request(url)

    async def close(self) -> None:
        self._closed = True
        workers = self._workers
        self._workers = []
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._pending.clear()

    def request(self, url: str, *, force: bool = False) -> bool:
        """Schedule metadata retrieval. Returns True when newly queued."""
        if not url.strip() or self._closed or not self._workers:
            return False
        url = url.strip()
        if url in self._cache:
            return False
        if url in self._pending:
            return False
        failed_at = self._errors.get(url)
        if not force and failed_at is not None:
            if time.monotonic() - failed_at < self.error_cooldown:
                return False
            self._errors.pop(url, None)

        self._pending.add(url)
        self._jobs.put_nowait(url)
        return True

    def get(self, url: str) -> MediaMetadata | None:
        return self._cache.get(url)

    async def _run_worker(self, worker_id: int) -> None:
        log.debug("metadata worker %s started", worker_id)
        try:
            while True:
                url = await self._jobs.get()
                try:
                    await self._fetch_one(url)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("metadata worker %s failed for %s", worker_id, url)
                finally:
                    self._pending.discard(url)
                    self._jobs.task_done()
        except asyncio.CancelledError:
            log.debug("metadata worker %s stopped", worker_id)
            raise

    async def _fetch_one(self, url: str) -> None:
        try:
            metadata = await self.provider.fetch(url)
        except Exception as exc:
            self._errors[url] = time.monotonic()
            await self.events.publish(
                "metadata.error",
                {
                    "url": url,
                    "error": str(exc),
                },
            )
            await self.events.publish(
                "state.changed",
                {"resources": ["queue", "status"]},
            )
            return

        self._errors.pop(url, None)
        self._cache[url] = metadata
        await self.events.publish(
            "metadata.updated",
            {
                "url": url,
                "metadata": metadata.to_dict(),
            },
        )
        await self.events.publish(
            "state.changed",
            {"resources": ["queue", "status"]},
        )
