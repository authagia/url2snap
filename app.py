import asyncio

from api.routes import create_app
from config import (
    ERROR_POLICY,
    HISTORY_FILE,
    HTTP_HOST,
    HTTP_PORT,
    MAX_RETRIES,
    MPV_BIN,
    PLAYLIST_FILE,
    QUEUE_FILE,
    RETRY_DELAY_MS,
    SNAP_FIFO,
    SPOTDL_BIN,
    SPOTDL_TIMEOUT,
    YTDLP_BIN,
    YTDLP_METADATA_TIMEOUT,
    METADATA_WORKERS,
    METADATA_ERROR_COOLDOWN,
    METADATA_REQUEST_INTERVAL,
)
from core.controller import Controller
from core.error_policy import RetryErrorPolicy, SkipErrorPolicy
from core.history import JsonHistoryRepository
from core.playlists import JsonPlaylistRepository
from core.queue import JsonQueueRepository
from output.fifo import ensure_fifo
from resolver.chain import ResolverChain
from resolver.direct import DirectResolver
from resolver.spotify import SpotifyResolver


async def main():
    ensure_fifo(SNAP_FIFO)

    resolver = ResolverChain([
        # SpotifyResolver(spotdl_bin=SPOTDL_BIN, timeout=SPOTDL_TIMEOUT), # TOO SLOW to Extract Youtube URL (≈ 1min)
        DirectResolver(),
    ])
    history = JsonHistoryRepository(HISTORY_FILE)
    playlists = JsonPlaylistRepository(PLAYLIST_FILE)
    queue_repository = JsonQueueRepository(QUEUE_FILE)

    if ERROR_POLICY == "skip":
        error_policy = SkipErrorPolicy()
    elif ERROR_POLICY == "retry":
        error_policy = RetryErrorPolicy(
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY_MS / 1000,
        )
    else:
        raise ValueError(f"unsupported AUDIO_SOURCE_ERROR_POLICY: {ERROR_POLICY!r}")

    controller = Controller(
        resolver=resolver,
        snap_fifo=SNAP_FIFO,
        mpv_bin=MPV_BIN,
        history=history,
        playlists=playlists,
        queue_repository=queue_repository,
        error_policy=error_policy,
        ytdlp_bin=YTDLP_BIN,
        ytdlp_metadata_timeout=YTDLP_METADATA_TIMEOUT,
        metadata_workers=METADATA_WORKERS,
        metadata_error_cooldown=METADATA_ERROR_COOLDOWN,
        metadata_request_interval=METADATA_REQUEST_INTERVAL,
    )

    await controller.startup()
    app = create_app(controller)

    import uvicorn
    server_config = uvicorn.Config(
        app,
        host=HTTP_HOST,
        port=HTTP_PORT,
        log_level="info",
    )
    server = uvicorn.Server(server_config)

    try:
        await server.serve()
    finally:
        await controller.close()


if __name__ == "__main__":
    asyncio.run(main())
