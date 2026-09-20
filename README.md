# audio-source

Minimal HTTP control service for URL-based multi-room audio playback via mpv and
Snapcast.

```text
HTTP API
  -> Controller / PlaybackCoordinator
  -> Queue / Playlist / History / Resolver
  -> mpv
  -> snapfifo
  -> Snapserver
  -> Snapclients
```

The project deliberately keeps the custom layer small. mpv owns media playback;
Snapcast owns synchronized multi-room delivery. The service owns playback
control, ordered TrackLists, persistence, repeat/error policy, and the HTTP API.

## Features

- Direct URL playback
- Persistent waiting queue with stable item IDs
- Queue add/remove/move/replace
- Repeat: `normal`, `repeat_one`, `repeat_queue`
- Persistent playlists
- Playlist playback without destroying the waiting queue
- Playback history with replay or queue-from-history
- Bounded retry policy with fresh URL resolution on every retry
- Spotify track URLs resolved to YouTube URLs via spotDL
- Automatic error skipping by default
- CORS for browser-based clients
- Versioned JSON persistence with atomic replacement
- Direct `mpv -> snapfifo` raw PCM output

## Design principles

### Stable references vs. resolved sources

Queue, playlists, and history store `TrackRef(original_url)`. A resolver creates a
short-lived `ResolvedSource` immediately before playback. Expiring media URLs
are never treated as persistent identity.

```text
TrackRef(original URL)
       |
       v
    Resolver
       |
       v
ResolvedSource (ephemeral)
       |
       v
      mpv
       |
       v
   Snapcast FIFO
```

This also means retrying a failed item runs the resolver again instead of trying
to reuse an expired media URL.

### TrackLists

`TrackList` is the common ordered-list abstraction. The waiting Queue and
persistent Playlist use it, while active playlist playback consumes a transient
snapshot so editing the stored playlist does not change what is already playing.

### Playback output

mpv writes raw S16 stereo PCM at 48 kHz directly to the Snapserver FIFO. No
custom PCM reader/ring buffer is involved in the media path.

Normal completion is based on the mpv process exit result. An mpv error does not
wait for a separate FIFO reader to drain or reach EOF.

## Error handling

The default policy is:

```text
ERROR -> SKIP
```

A retry policy can be enabled with environment variables:

```text
AUDIO_SOURCE_ERROR_POLICY=retry
AUDIO_SOURCE_MAX_RETRIES=2
AUDIO_SOURCE_RETRY_DELAY_MS=0
```

With `MAX_RETRIES=2`, a failing item gets up to three total attempts. Resolver
failures and mpv playback failures use the same policy.

## Runtime requirements

- Linux
- Python 3.11+
- `mpv`
- `spotdl` (installed by `requirements.txt`)
- Snapserver configured with a pipe/FIFO source
- Network access to any remote media source you intend to play

For sources such as YouTube, actual support depends on the local mpv setup and
its input helpers (for example yt-dlp availability). Spotify track URLs are
resolved to YouTube URLs by spotDL before playback. spotDL uses yt-dlp for its
YouTube provider; Deno is recommended by spotDL for some YouTube cases.

## Quick start

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run:

```bash
python app.py
```

Default HTTP endpoint:

```text
http://127.0.0.1:8000
```

Interactive API documentation is available at `/docs`.

## Snapcast setup

The default FIFO path is:

```text
/run/snapcast/snapfifo
```

The service creates the FIFO itself, so the runtime directory must be writable
by the audio-source service user and traversable/readable by the Snapserver
account.

Example host setup (replace the user/group with the actual accounts on your
machine):

```bash
sudo install -d -o <audio-user> -g <snapserver-group> -m 0770 /run/snapcast
```

Snapserver should read the existing FIFO, for example:

```ini
source = pipe:///run/snapcast/snapfifo?name=default&mode=read
```

The FIFO path can be overridden with `AUDIO_SOURCE_SNAP_FIFO`.

## Configuration

The application has no built-in secrets or API keys. Environment-specific
configuration is limited to paths, process names, HTTP binding, persistence
files, and retry behavior.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AUDIO_SOURCE_HTTP_HOST` | `0.0.0.0` | HTTP bind address |
| `AUDIO_SOURCE_HTTP_PORT` | `8000` | HTTP port |
| `AUDIO_SOURCE_MPV_BIN` | `mpv` | mpv executable |
| `AUDIO_SOURCE_SNAP_FIFO` | `/run/snapcast/snapfifo` | Snapcast FIFO |
| `AUDIO_SOURCE_HISTORY_FILE` | `./data/history.json` | History store |
| `AUDIO_SOURCE_PLAYLIST_FILE` | `./data/playlists.json` | Playlist store |
| `AUDIO_SOURCE_QUEUE_FILE` | `./data/queue.json` | Waiting queue store |
| `AUDIO_SOURCE_ERROR_POLICY` | `skip` | `skip` or `retry` |
| `AUDIO_SOURCE_MAX_RETRIES` | `2` | Retries when policy is `retry` |
| `AUDIO_SOURCE_RETRY_DELAY_MS` | `0` | Delay between retries |
| `AUDIO_SOURCE_SPOTDL_BIN` | `spotdl` | spotDL executable |
| `AUDIO_SOURCE_SPOTDL_TIMEOUT` | `30` | spotDL resolve timeout in seconds |

A starter file is provided as `.env.example`. The application currently reads
environment variables directly; loading `.env.example` requires your process
manager or shell tooling.

### Runtime files and GitHub

`data/*.json` contains local playback state and is ignored by Git. `.env` is
also ignored. Do not commit machine-specific persistence files or credentials.

## Persistence and restart behavior

Queue, Playlist, and History use repository interfaces with JSON-backed default
implementations. Writes use a temporary file followed by `fsync` and atomic
`os.replace`.

Pre-Phase-15 legacy array-shaped persistence files remain readable and are
migrated to the versioned document format on the next write.

On startup, only the waiting queue is restored. Active playback is not resumed.
A malformed persistence file causes startup to fail instead of silently
starting with an empty store.

## CORS

Browser clients are allowed through CORS with all origins, methods, and request
headers allowed; credentialed CORS is disabled. This is intended for a trusted
network / development setup. Restrict origins before exposing the API beyond a
trusted network or adding browser authentication.

## API

The concise endpoint reference is in [`docs/api.md`](docs/api.md).

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/play` | Play a URL |
| `POST` | `/stop` | Stop playback |
| `POST` | `/skip` | Skip current item |
| `GET` | `/status` | Playback status |
| `GET` / `PUT` | `/repeat` | Read/set repeat mode |
| `GET` / `POST` / `DELETE` / `PUT` | `/queue...` | Queue management |
| `GET` / `POST` / `DELETE` | `/playlists...` | Playlist management |
| `POST` | `/playlists/{id}/play` | Play a playlist snapshot |
| `GET` | `/history` | Playback history |
| `POST` | `/history/{id}/play` | Replay history entry |
| `POST` | `/history/{id}/queue` | Push history entry into queue |

The full interactive schema is generated by FastAPI at `/docs`.

## Development

Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

Run tests:

```bash
pytest -q
```

Compile check:

```bash
python -m compileall -q .
```

CI runs these checks on pushes and pull requests.

## Roadmap

See [`ROADMAP.md`](ROADMAP.md).

The next major milestones are real deployment hardening and then containerization.
Containerization is intentionally deferred until host-based mpv/Snapcast behavior
has been validated for long-running use.
