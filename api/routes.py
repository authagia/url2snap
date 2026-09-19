from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from core.controller import Controller
from core.models import RepeatMode


class UrlRequest(BaseModel):
    url: str


class QueueReplaceRequest(BaseModel):
    urls: list[str]


class QueueMoveRequest(BaseModel):
    index: int


class PlaylistCreateRequest(BaseModel):
    name: str
    urls: list[str] = Field(default_factory=list)


class PlaylistTrackRequest(BaseModel):
    url: str
    index: int | None = None


class RepeatRequest(BaseModel):
    mode: RepeatMode


def create_app(controller: Controller) -> FastAPI:
    app = FastAPI(title="audio-source")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/play", status_code=status.HTTP_202_ACCEPTED)
    async def play(req: UrlRequest):
        if not req.url.strip():
            raise HTTPException(400, "url is empty")
        await controller.play(req.url)
        return {"ok": True}

    @app.post("/stop", status_code=status.HTTP_202_ACCEPTED)
    async def stop():
        await controller.stop()
        return {"ok": True}

    @app.post("/skip", status_code=status.HTTP_202_ACCEPTED)
    async def skip():
        await controller.skip()
        return {"ok": True}

    @app.get("/repeat")
    async def get_repeat():
        return {"mode": (await controller.repeat_mode()).value}

    @app.put("/repeat")
    async def set_repeat(req: RepeatRequest):
        mode = await controller.set_repeat_mode(req.mode)
        return {"ok": True, "mode": mode.value}

    @app.get("/status")
    async def get_status():
        return await controller.status()

    @app.get("/history")
    async def history(limit: int = 100):
        if limit < 1 or limit > 1000:
            raise HTTPException(400, "limit must be between 1 and 1000")
        return await controller.history_snapshot(limit)

    @app.post("/history/{entry_id}/play", status_code=status.HTTP_202_ACCEPTED)
    async def replay_history(entry_id: str):
        try:
            entry = await controller.replay_history(entry_id)
        except KeyError:
            raise HTTPException(404, "history entry not found")
        return {"ok": True, "entry": entry}

    @app.post("/history/{entry_id}/queue", status_code=status.HTTP_202_ACCEPTED)
    async def enqueue_history(entry_id: str):
        try:
            item = await controller.enqueue_history(entry_id)
        except KeyError:
            raise HTTPException(404, "history entry not found")
        return {"ok": True, "item": item}

    @app.post("/playlists/{playlist_id}/play", status_code=status.HTTP_202_ACCEPTED)
    async def play_playlist(playlist_id: str):
        try:
            await controller.play_playlist(playlist_id)
        except KeyError:
            raise HTTPException(404, "playlist not found")
        except ValueError:
            raise HTTPException(400, "playlist is empty")
        return {"ok": True}

    @app.get("/playlists")
    async def get_playlists():
        return await controller.playlist_snapshot()

    @app.post("/playlists")
    async def create_playlist(req: PlaylistCreateRequest):
        if not req.name.strip():
            raise HTTPException(400, "playlist name is empty")
        if any(not url.strip() for url in req.urls):
            raise HTTPException(400, "url is empty")
        try:
            playlist = await controller.create_playlist(req.name.strip(), req.urls)
        except RuntimeError:
            raise HTTPException(503, "playlists are not configured")
        return {"ok": True, "playlist": playlist}

    @app.get("/playlists/{playlist_id}")
    async def get_playlist(playlist_id: str):
        try:
            return await controller.get_playlist(playlist_id)
        except KeyError:
            raise HTTPException(404, "playlist not found")

    @app.delete("/playlists/{playlist_id}")
    async def delete_playlist(playlist_id: str):
        try:
            playlist = await controller.delete_playlist(playlist_id)
        except KeyError:
            raise HTTPException(404, "playlist not found")
        return {"ok": True, "playlist": playlist}

    @app.post("/playlists/{playlist_id}/tracks")
    async def add_playlist_track(playlist_id: str, req: PlaylistTrackRequest):
        if not req.url.strip():
            raise HTTPException(400, "url is empty")
        try:
            item = await controller.add_playlist_track(playlist_id, req.url, req.index)
        except KeyError:
            raise HTTPException(404, "playlist not found")
        except IndexError:
            raise HTTPException(400, "index out of range")
        return {"ok": True, "item": item}

    @app.delete("/playlists/{playlist_id}/tracks/{item_id}")
    async def remove_playlist_track(playlist_id: str, item_id: str):
        try:
            item = await controller.remove_playlist_track(playlist_id, item_id)
        except KeyError as exc:
            if str(exc).strip("'") == playlist_id:
                raise HTTPException(404, "playlist not found")
            raise HTTPException(404, "playlist item not found")
        return {"ok": True, "item": item}

    @app.post("/playlists/{playlist_id}/tracks/{item_id}/move")
    async def move_playlist_track(playlist_id: str, item_id: str, req: QueueMoveRequest):
        try:
            item = await controller.move_playlist_track(playlist_id, item_id, req.index)
        except KeyError as exc:
            if str(exc).strip("'") == playlist_id:
                raise HTTPException(404, "playlist not found")
            raise HTTPException(404, "playlist item not found")
        except IndexError:
            raise HTTPException(400, "index out of range")
        return {"ok": True, "item": item}

    @app.put("/playlists/{playlist_id}/tracks")
    async def replace_playlist(playlist_id: str, req: QueueReplaceRequest):
        if any(not url.strip() for url in req.urls):
            raise HTTPException(400, "url is empty")
        try:
            playlist = await controller.replace_playlist(playlist_id, req.urls)
        except KeyError:
            raise HTTPException(404, "playlist not found")
        return {"ok": True, "playlist": playlist}

    @app.get("/queue")
    async def queue():
        return await controller.queue_snapshot()

    @app.post("/queue")
    async def enqueue(req: UrlRequest):
        if not req.url.strip():
            raise HTTPException(400, "url is empty")
        item = await controller.enqueue(req.url)
        return {"ok": True, "item": item}

    @app.delete("/queue/{item_id}")
    async def remove_queue_item(item_id: str):
        try:
            item = await controller.remove_queue_item(item_id)
        except KeyError:
            raise HTTPException(404, "queue item not found")
        return {"ok": True, "item": item}

    @app.post("/queue/{item_id}/move")
    async def move_queue_item(item_id: str, req: QueueMoveRequest):
        try:
            item = await controller.move_queue_item(item_id, req.index)
        except KeyError:
            raise HTTPException(404, "queue item not found")
        except IndexError:
            raise HTTPException(400, "index out of range")
        return {"ok": True, "item": item}

    @app.put("/queue")
    async def replace_queue(req: QueueReplaceRequest):
        if any(not url.strip() for url in req.urls):
            raise HTTPException(400, "url is empty")
        items = await controller.replace_queue(req.urls)
        return {"ok": True, "items": items}

    return app
