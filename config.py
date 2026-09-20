import os

SNAP_FIFO = os.environ.get("AUDIO_SOURCE_SNAP_FIFO", "/run/snapcast/snapfifo")
MPV_BIN = os.environ.get("AUDIO_SOURCE_MPV_BIN", "mpv")
HTTP_HOST = os.environ.get("AUDIO_SOURCE_HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("AUDIO_SOURCE_HTTP_PORT", "8000"))

HISTORY_FILE = os.environ.get("AUDIO_SOURCE_HISTORY_FILE", "./data/history.json")
PLAYLIST_FILE = os.environ.get("AUDIO_SOURCE_PLAYLIST_FILE", "./data/playlists.json")
QUEUE_FILE = os.environ.get("AUDIO_SOURCE_QUEUE_FILE", "./data/queue.json")
ERROR_POLICY = os.environ.get("AUDIO_SOURCE_ERROR_POLICY", "skip").strip().lower()
MAX_RETRIES = int(os.environ.get("AUDIO_SOURCE_MAX_RETRIES", "2"))
RETRY_DELAY_MS = int(os.environ.get("AUDIO_SOURCE_RETRY_DELAY_MS", "0"))
SPOTDL_BIN = os.environ.get("AUDIO_SOURCE_SPOTDL_BIN", "spotdl")
SPOTDL_TIMEOUT = float(os.environ.get("AUDIO_SOURCE_SPOTDL_TIMEOUT", "30"))
