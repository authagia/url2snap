import asyncio
import errno
import os
import stat
from pathlib import Path


def ensure_fifo(path: str, mode: int = 0o660):
    """Ensure *path* is a FIFO and its parent directory exists.

    The default runtime path is /run/snapcast/snapfifo. On systems where the
    service user cannot create /run/snapcast, provision that directory at
    deployment time and grant the mpv user write access plus the snapserver
    user/group read/traverse access.
    """
    p = Path(path).expanduser().absolute()
    parent = p.parent

    try:
        parent.mkdir(parents=True, mode=0o770, exist_ok=True)
    except PermissionError as exc:
        raise PermissionError(
            f"cannot create FIFO directory {parent}; provision it for the "
            "url2snap service user before startup"
        ) from exc

    if p.exists():
        if not stat.S_ISFIFO(p.stat().st_mode):
            raise RuntimeError(f"{p} exists and is not a FIFO")
        return

    try:
        os.mkfifo(p, mode)
    except FileExistsError:
        if not stat.S_ISFIFO(p.stat().st_mode):
            raise RuntimeError(f"{p} exists and is not a FIFO")


class SnapFifo:
    """Control-side PCM writer for short transition silence.

    Media PCM is written by mpv directly; this helper is only used for the
    short transition block emitted after an interrupted or failed attempt.
    """

    def __init__(self, path: str):
        self.path = path
        self._write_lock = asyncio.Lock()

    async def write_silence(
        self,
        duration_ms: int,
        sample_rate: int = 48000,
        channels: int = 2,
        sample_width: int = 2,
    ):
        if duration_ms <= 0:
            return

        frame_bytes = channels * sample_width
        frames = round(sample_rate * duration_ms / 1000)
        data = b"\x00" * (frames * frame_bytes)

        async with self._write_lock:
            fd = None
            try:
                fd = await asyncio.to_thread(
                    os.open,
                    self.path,
                    os.O_WRONLY | os.O_NONBLOCK,
                )
            except OSError as exc:
                if exc.errno == errno.ENXIO:
                    return
                raise

            try:
                await asyncio.to_thread(self._write_all, fd, data)
            finally:
                os.close(fd)

    @staticmethod
    def _write_all(fd: int, data: bytes):
        view = memoryview(data)
        while view:
            try:
                n = os.write(fd, view)
            except BlockingIOError:
                import time
                time.sleep(0.001)
                continue
            view = view[n:]
