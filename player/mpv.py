import asyncio

from core.models import ResolvedSource


class MpvPlayer:
    """Launch mpv with its raw PCM output connected directly to Snapcast."""

    def __init__(self, snap_fifo: str, mpv_bin: str = "mpv"):
        self.snap_fifo = snap_fifo
        self.mpv_bin = mpv_bin
        self.process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None
        self._stderr_data = b""

    async def start(self, media: ResolvedSource):
        if self.process is not None:
            raise RuntimeError("mpv is already running")

        cmd = [
            self.mpv_bin,
            "--no-video",
            "--no-terminal",
            "--really-quiet",
            "--ao=pcm",
            "--ao-pcm-waveheader=no",
            f"--ao-pcm-file={self.snap_fifo}",
            "--audio-format=s16",
            "--audio-samplerate=48000",
            "--audio-channels=stereo",
            media.url,
        ]

        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stderr_data = b""
        self._stderr_task = asyncio.create_task(self._read_stderr())

    async def _read_stderr(self):
        if self.process is None or self.process.stderr is None:
            return
        self._stderr_data = await self.process.stderr.read()

    async def wait(self) -> int:
        if self.process is None:
            raise RuntimeError("mpv is not running")
        rc = await self.process.wait()
        if self._stderr_task is not None:
            await self._stderr_task
        self._stderr_task = None
        self.process = None
        return rc

    async def get_stderr(self) -> str:
        return self._stderr_data.decode(errors="replace")

    async def stop(self):
        if self.process is None:
            return

        self.process.terminate()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=3)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()

        if self._stderr_task is not None:
            await self._stderr_task
        self._stderr_task = None
        self.process = None
