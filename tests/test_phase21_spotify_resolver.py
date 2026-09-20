import asyncio

import pytest

from core.models import TrackRef
from resolver.spotify import SpotifyResolver


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC", True),
        ("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC?si=abc", True),
        ("http://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC", True),
        ("https://open.spotify.com/album/4uLU6hMCjMI75M1A2tKUQC", False),
        ("https://www.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC", False),
        ("https://www.youtube.com/watch?v=abc", False),
    ],
)
def test_can_handle_track_url(url, expected):
    resolver = SpotifyResolver()
    assert resolver.can_handle(TrackRef(url)) is expected


@pytest.mark.asyncio
async def test_resolve_uses_spotdl_and_returns_youtube_url(monkeypatch):
    calls = {}

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return (
                b"https://www.youtube.com/watch?v=matched123\n",
                b"",
            )

        def kill(self):
            calls["killed"] = True

        async def wait(self):
            calls["waited"] = True

    async def fake_create_subprocess_exec(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr("resolver.spotify.shutil.which", lambda _: "/usr/bin/spotdl")
    monkeypatch.setattr(
        "resolver.spotify.asyncio.create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    resolver = SpotifyResolver(spotdl_bin="spotdl")
    result = await resolver.resolve(
        TrackRef("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC")
    )

    assert result.url == "https://www.youtube.com/watch?v=matched123"
    assert calls["args"] == (
        "spotdl",
        "--audio=youtube",
        "url",
        "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC",
    )
    assert calls["kwargs"]["stdin"] is asyncio.subprocess.DEVNULL


@pytest.mark.asyncio
async def test_resolve_rejects_non_spotify_url():
    resolver = SpotifyResolver()
    with pytest.raises(ValueError, match="unsupported Spotify URL"):
        await resolver.resolve(TrackRef("https://www.youtube.com/watch?v=abc"))


@pytest.mark.asyncio
async def test_resolve_fails_without_spotdl(monkeypatch):
    monkeypatch.setattr("resolver.spotify.shutil.which", lambda _: None)
    resolver = SpotifyResolver(spotdl_bin="missing-spotdl")

    with pytest.raises(RuntimeError, match="spotDL executable not found"):
        await resolver.resolve(
            TrackRef("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC")
        )
