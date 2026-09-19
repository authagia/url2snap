import asyncio
import logging

from core.models import PlaybackResult, PlaybackState, ResolvedSource
from output.fifo import SnapFifo
from player.mpv import MpvPlayer

log = logging.getLogger(__name__)


class PlaybackSession:
    """Owns one mpv playback lifecycle.

    mpv writes raw S16LE 48 kHz stereo directly to Snapcast's FIFO. The
    session therefore decides completion from mpv's process result instead of
    waiting for a separate PCM reader/writer pipeline to reach EOF.

    Explicit stop/skip and playback errors may emit a short transition silence
    after mpv has released the FIFO writer.
    """

    SILENCE_MS = 200

    def __init__(self, snap_fifo: str, mpv_bin: str = "mpv"):
        self.snap_fifo = snap_fifo
        self.mpv_bin = mpv_bin

        self.state = PlaybackState.IDLE
        self.current: ResolvedSource | None = None
        self.last_result: PlaybackResult | None = None

        self._player: MpvPlayer | None = None
        self._fifo = SnapFifo(snap_fifo)
        self._requested_result: PlaybackResult | None = None

    async def play(self, media: ResolvedSource) -> PlaybackResult:
        await self.stop()

        self.current = media
        self.state = PlaybackState.STARTING
        self.last_result = None
        self._requested_result = None

        player = MpvPlayer(self.snap_fifo, self.mpv_bin)
        self._player = player
        result = PlaybackResult.ERROR

        try:
            await player.start(media)
            self.state = PlaybackState.PLAYING

            rc = await player.wait()
            stderr = await player.get_stderr()

            if self._requested_result is not None:
                result = self._requested_result
                log.info("mpv ended after %s request", result.value)
            elif rc:
                result = PlaybackResult.ERROR
                log.warning("mpv exited with code %s", rc)
                if stderr:
                    log.warning("mpv stderr:\n%s", stderr.rstrip())
                await self._write_transition_silence()
            else:
                result = PlaybackResult.COMPLETED
                log.info("mpv reached EOF normally")

            self.last_result = result
            # A completed mpv process, including an error exit, has ended the
            # session. ERROR is reserved for an unexpected session-level
            # exception; normal playback failures must not strand the service
            # in ERROR after the controller has handled the result.
            self.state = PlaybackState.IDLE
            return result

        except asyncio.CancelledError:
            raise
        except Exception:
            self.state = PlaybackState.ERROR
            self.last_result = PlaybackResult.ERROR
            log.exception("playback session failed")
            return PlaybackResult.ERROR
        finally:
            self._player = None
            self._requested_result = None
            self.current = None
            if self.state != PlaybackState.ERROR:
                self.state = PlaybackState.IDLE

    async def _write_transition_silence(self):
        try:
            await self._fifo.write_silence(self.SILENCE_MS)
        except Exception:
            log.exception("failed to write transition silence")

    async def stop(
        self,
        result: PlaybackResult = PlaybackResult.STOPPED,
        silence: bool = False,
    ):
        player = self._player
        if player is None:
            return

        self.state = PlaybackState.STOPPING
        self._requested_result = result

        await player.stop()

        if silence:
            await self._write_transition_silence()

        self._player = None
        self.current = None
        self.state = PlaybackState.IDLE
        # Keep the request visible until play() consumes it. The playback task
        # and stop() run concurrently, so clearing it here can race with the
        # player.wait() completion.
