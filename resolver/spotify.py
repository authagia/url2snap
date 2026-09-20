from __future__ import annotations

import asyncio
import re
import shutil
from urllib.parse import urlsplit

from core.models import ResolvedSource, TrackRef
from resolver.base import Resolver


_SPOTIFY_TRACK_RE = re.compile(r"^/track/[A-Za-z0-9]+/?$")


class SpotifyResolver(Resolver):
    """Resolve a Spotify track URL to a YouTube URL via spotDL.

    spotDL performs the Spotify-track identification and external-source
    matching. We intentionally ask it for the original matched source URL
    rather than a direct media URL; mpv remains responsible for using yt-dlp
    to resolve the YouTube URL for playback.
    """

    def __init__(
        self,
        spotdl_bin: str = "spotdl",
        *,
        timeout: float = 30.0,
        audio_provider: str = "youtube",
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        if not audio_provider.strip():
            raise ValueError("audio_provider must not be empty")

        self.spotdl_bin = spotdl_bin
        self.timeout = timeout
        self.audio_provider = audio_provider

    def can_handle(self, track: TrackRef) -> bool:
        parsed = urlsplit(track.original_url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if parsed.hostname != "open.spotify.com":
            return False
        return bool(_SPOTIFY_TRACK_RE.fullmatch(parsed.path))

    async def resolve(self, track: TrackRef) -> ResolvedSource:
        if not self.can_handle(track):
            raise ValueError(f"unsupported Spotify URL: {track.original_url}")

        if shutil.which(self.spotdl_bin) is None:
            raise RuntimeError(
                f"spotDL executable not found: {self.spotdl_bin!r}"
            )

        command = [
            self.spotdl_bin,
            f"--audio={self.audio_provider}",
            "url",
            track.original_url,
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise RuntimeError(
                f"failed to start spotDL: {exc}"
            ) from exc

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            raise RuntimeError(
                f"spotDL timed out after {self.timeout:g}s"
            ) from exc

        stdout_text = stdout.decode("utf-8", errors="replace")
        stderr_text = stderr.decode("utf-8", errors="replace").strip()

        if process.returncode != 0:
            detail = stderr_text or stdout_text.strip()
            message = f"spotDL exited with code {process.returncode}"
            if detail:
                message += f": {detail}"
            raise RuntimeError(message)

        candidates = [
            line.strip()
            for line in stdout_text.splitlines()
            if line.strip().startswith(("http://", "https://"))
        ]
        if not candidates:
            detail = stderr_text or stdout_text.strip()
            message = "spotDL returned no playable source URL"
            if detail:
                message += f": {detail}"
            raise RuntimeError(message)

        source_url = candidates[0]
        source = urlsplit(source_url)
        if source.scheme not in {"http", "https"} or not source.netloc:
            raise RuntimeError(f"spotDL returned an invalid URL: {source_url!r}")

        return ResolvedSource(url=source_url)
