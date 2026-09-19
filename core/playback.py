from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.controller import Controller

log = logging.getLogger(__name__)


class PlaybackCoordinator:
    """Serialize playback-related commands for a Controller.

    The coordinator owns command ordering and dispatch only. Playback policy
    and domain state remain on Controller for now; this gives us a clean seam
    for further Controller decomposition without changing the public API.
    """

    def __init__(self, owner: Controller):
        self.owner = owner
        self.commands: asyncio.Queue[tuple[str, object, str | None]] = asyncio.Queue()
        self.worker = asyncio.create_task(self._worker_loop())

    async def submit(self, command: str, payload=None) -> str:
        command_id = uuid.uuid4().hex[:12]
        await self.commands.put((command, payload, command_id))
        return command_id

    async def _worker_loop(self):
        while True:
            command, payload, _command_id = await self.commands.get()
            try:
                if command == "play":
                    await self.owner._do_play(payload)
                elif command == "stop":
                    await self.owner._do_stop()
                elif command == "skip":
                    await self.owner._do_skip()
                elif command == "queue":
                    await self.owner._do_enqueue()
                elif command == "play_playlist":
                    await self.owner._do_play_playlist(payload)
                elif command == "finished":
                    await self.owner._do_finished(payload)
                else:
                    raise ValueError(f"unknown playback command: {command!r}")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("controller command failed: %s", command)
            finally:
                self.commands.task_done()

    async def close(self):
        self.worker.cancel()
        await asyncio.gather(self.worker, return_exceptions=True)
