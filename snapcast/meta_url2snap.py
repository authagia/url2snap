#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import threading
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger("meta_url2snap")


class SnapcastMetadataBridge:
    """Snapcast stream controlscript backed by url2snap's SSE events."""

    def __init__(self, host: str, port: int, reconnect_delay: float = 2.0):
        self.base_url = f"http://{host}:{port}"
        self.reconnect_delay = reconnect_delay
        self._stdout_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self._current_url: str | None = None
        self._properties: dict[str, Any] = self._stopped_properties()

    def emit(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._stdout_lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()

    @staticmethod
    def _stopped_properties() -> dict[str, Any]:
        return {
            "canControl": False,
            "canGoNext": False,
            "canGoPrevious": False,
            "canPause": False,
            "canPlay": False,
            "canSeek": False,
            "loopStatus": "none",
            "playbackStatus": "stopped",
            "shuffle": False,
        }

    @staticmethod
    def _track_id(url: str) -> str:
        return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

    @classmethod
    def _snap_metadata(cls, metadata: dict[str, Any], url: str) -> dict[str, Any]:
        result: dict[str, Any] = {"trackId": cls._track_id(url), "url": url}
        title = metadata.get("title")
        if isinstance(title, str) and title.strip():
            result["title"] = title.strip()
        artist = metadata.get("artist")
        if isinstance(artist, str) and artist.strip():
            result["artist"] = [artist.strip()]
        album = metadata.get("album")
        if isinstance(album, str) and album.strip():
            result["album"] = album.strip()
        duration = metadata.get("duration")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            result["duration"] = float(duration)
        artwork_url = metadata.get("artwork_url")
        if isinstance(artwork_url, str) and artwork_url.strip():
            result["artUrl"] = artwork_url.strip()
        return result

    def _set_current(self, url: str, metadata: dict[str, Any] | None) -> None:
        with self._state_lock:
            self._current_url = url
            props = self._stopped_properties()
            props["playbackStatus"] = "playing"
            if metadata:
                props["metadata"] = self._snap_metadata(metadata, url)
            self._properties = props
        self._emit_properties()

    def _update_metadata(self, url: str, metadata: dict[str, Any]) -> None:
        with self._state_lock:
            if self._current_url != url:
                return
            props = dict(self._properties)
            props["metadata"] = self._snap_metadata(metadata, url)
            self._properties = props
        self._emit_properties()

    def _clear_current(self, url: str | None = None) -> None:
        with self._state_lock:
            if url is not None and self._current_url != url:
                return
            self._current_url = None
            self._properties = self._stopped_properties()
        self._emit_properties()

    def _emit_properties(self) -> None:
        with self._state_lock:
            properties = dict(self._properties)
        self.emit(
            {
                "jsonrpc": "2.0",
                "method": "Plugin.Stream.Player.Properties",
                "params": properties,
            }
        )

    def sync_status(self) -> None:
        try:
            request = Request(
                f"{self.base_url}/status",
                headers={"Accept": "application/json"},
            )
            with urlopen(request, timeout=5) as response:
                payload = json.load(response)
        except Exception as exc:
            log.debug("status sync failed: %s", exc)
            return

        current = payload.get("current")
        state = payload.get("state")
        if isinstance(current, dict) and isinstance(current.get("url"), str):
            url = current["url"]
            metadata = {
                "title": current.get("title"),
                "artist": current.get("artist"),
                "album": current.get("album"),
                "duration": current.get("duration"),
                "artwork_url": current.get("artwork_url"),
            }
            if state in {"starting", "playing"}:
                self._set_current(url, metadata)
            else:
                self._clear_current(url)
        else:
            self._clear_current()

    def handle_event(self, event_type: str, data: dict[str, Any]) -> None:
        if event_type == "playback.started":
            url = data.get("url")
            if isinstance(url, str) and url:
                metadata = data.get("metadata")
                self._set_current(
                    url,
                    metadata if isinstance(metadata, dict) else None,
                )
        elif event_type == "metadata.updated":
            url = data.get("url")
            metadata = data.get("metadata")
            if isinstance(url, str) and isinstance(metadata, dict):
                self._update_metadata(url, metadata)
        elif event_type == "playback.stopped":
            url = data.get("url")
            self._clear_current(url if isinstance(url, str) else None)

    def sse_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.sync_status()
                request = Request(
                    f"{self.base_url}/events",
                    headers={
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache",
                    },
                )
                with urlopen(request, timeout=None) as response:
                    event_type: str | None = None
                    data_lines: list[str] = []
                    while not self._stop.is_set():
                        line = response.readline()
                        if not line:
                            break
                        text = line.decode("utf-8", errors="replace").rstrip("\r\n")
                        if text.startswith(":"):
                            continue
                        if not text:
                            if event_type and data_lines:
                                try:
                                    payload = json.loads("\n".join(data_lines))
                                except json.JSONDecodeError:
                                    payload = None
                                if isinstance(payload, dict):
                                    self.handle_event(event_type, payload)
                            event_type = None
                            data_lines = []
                            continue
                        field, _, value = text.partition(":")
                        if value.startswith(" "):
                            value = value[1:]
                        if field == "event":
                            event_type = value
                        elif field == "data":
                            data_lines.append(value)
            except (URLError, OSError, TimeoutError) as exc:
                log.warning("url2snap SSE connection failed: %s", exc)
            except Exception:
                log.exception("url2snap SSE loop failed")

            self._stop.wait(self.reconnect_delay)

    def run(self) -> None:
        self._stop.clear()
        self.emit({"jsonrpc": "2.0", "method": "Plugin.Stream.Ready"})
        thread = threading.Thread(target=self.sse_loop, name="url2snap-sse", daemon=True)
        thread.start()
        try:
            for raw_line in sys.stdin:
                if self._stop.is_set():
                    break
                self.handle_command(raw_line)
        finally:
            self._stop.set()
            thread.join(timeout=1.0)

    def handle_command(self, raw_line: str) -> None:
        try:
            request = json.loads(raw_line)
            request_id = request.get("id")
            method = request.get("method", "")
            if method == "Plugin.Stream.Player.GetProperties":
                with self._state_lock:
                    properties = dict(self._properties)
                self.emit({"jsonrpc": "2.0", "id": request_id, "result": properties})
                return
            if method == "Plugin.Stream.Player.SetProperty":
                self.emit({"jsonrpc": "2.0", "id": request_id, "result": "ok"})
                return
            if method == "Plugin.Stream.Player.Control":
                self.emit(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": "control is not supported"},
                    }
                )
                return
            self.emit(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": "method not found"},
                }
            )
        except Exception as exc:
            self.emit(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error", "data": str(exc)},
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url2snap-host", default="127.0.0.1")
    parser.add_argument("--url2snap-port", type=int, default=1790)
    parser.add_argument("--reconnect-delay", type=float, default=2.0)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_known_args()[0]


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    SnapcastMetadataBridge(
        args.url2snap_host,
        args.url2snap_port,
        reconnect_delay=max(0.1, args.reconnect_delay),
    ).run()


if __name__ == "__main__":
    main()
