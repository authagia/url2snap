# API

Base URL: `http://<host>:8000`

The service also exposes FastAPI's interactive OpenAPI UI at `/docs` and the
OpenAPI schema at `/openapi.json`.

## Common rules

- `POST /play`, `/stop`, `/skip`, playlist playback, and history playback return `202 Accepted`.
- Queue and playlist URLs are stored as the original URL. Expiring resolver output is not persisted.
- `404` means the requested queue item, history entry, or playlist does not exist.
- Invalid request data returns `400`/`422` as appropriate.

## Playback

### `POST /play`

Start one URL immediately. This cancels an active playlist playback plan.

Request:

```json
{"url":"https://example.com/audio"}
```

Response: `202`

```json
{"ok":true}
```

### `POST /stop`

Stop the current playback and return to idle. Cancels an active playlist plan.

Response: `202`

```json
{"ok":true}
```

### `POST /skip`

Skip the current track and continue with the active TrackList or waiting queue.

Response: `202`

```json
{"ok":true}
```

### `GET /status`

Returns current playback state and queue information.

Example:

```json
{
  "state":"playing",
  "current":{
    "url":"https://example.com/audio",
    "title":"Example",
    "duration":123.4
  },
  "queue_length":2,
  "repeat_mode":"normal",
  "playback_source":{"type":"queue"},
  "playback_list_remaining":0,
  "error_policy":"skip"
}
```

### `GET /repeat`

Returns `normal`, `repeat_one`, or `repeat_queue`.

### `PUT /repeat`

Request:

```json
{"mode":"repeat_one"}
```

Response:

```json
{"ok":true,"mode":"repeat_one"}
```

## Queue

Queue items have stable IDs while queued.

### `GET /queue`

Returns the waiting queue:

```json
[
  {"id":"abc123","url":"https://example.com/a"},
  {"id":"def456","url":"https://example.com/b"}
]
```

### `POST /queue`

Request:

```json
{"url":"https://example.com/a"}
```

Response:

```json
{"ok":true,"item":{"id":"abc123","url":"https://example.com/a"}}
```

### `DELETE /queue/{item_id}`

Remove one waiting item.

### `POST /queue/{item_id}/move`

Request:

```json
{"index":0}
```

### `PUT /queue`

Replace the waiting queue without stopping the current track.

Request:

```json
{"urls":["https://example.com/a","https://example.com/b"]}
```

## History

### `GET /history?limit=100`

Returns newest entries first. `limit` is `1..1000`.

```json
[
  {
    "id":"abc123",
    "url":"https://example.com/a",
    "result":"completed",
    "started_at":"2026-09-19T10:00:00+00:00",
    "ended_at":"2026-09-19T10:03:12+00:00"
  }
]
```

`result` is one of `completed`, `error`, `skipped`, `stopped`.

### `POST /history/{entry_id}/play`

Replay the stored original URL as a new direct playback request.

### `POST /history/{entry_id}/queue`

Push the stored original URL into the waiting queue.

Response:

```json
{"ok":true,"item":{"id":"abc123","url":"https://example.com/a"}}
```

## Playlists

A playlist is a named persistent ordered list. Activating it snapshots the
playlist for playback; later edits to the stored playlist do not affect the
active snapshot.

### `GET /playlists`

### `POST /playlists`

Request:

```json
{"name":"Favorites","urls":["https://example.com/a"]}
```

### `GET /playlists/{playlist_id}`

### `DELETE /playlists/{playlist_id}`

### `POST /playlists/{playlist_id}/play`

Activate a playlist for playback. The waiting queue is preserved.

### `POST /playlists/{playlist_id}/tracks`

Request:

```json
{"url":"https://example.com/a","index":0}
```

`index` is optional; omitted means append.

### `DELETE /playlists/{playlist_id}/tracks/{item_id}`

### `POST /playlists/{playlist_id}/tracks/{item_id}/move`

Request:

```json
{"index":0}
```

### `PUT /playlists/{playlist_id}/tracks`

Request:

```json
{"urls":["https://example.com/a","https://example.com/b"]}
```

## CORS

CORS is currently enabled for browser clients with all origins, methods, and
request headers allowed and credentials disabled. This is suitable for a
trusted-network development setup; restrict allowed origins before adding
browser authentication or exposing the API publicly.


## Realtime events

`GET /events` provides a transient Server-Sent Events stream. It is an invalidation
channel, not a durable event log. Clients should fetch the latest snapshot after
receiving an event.

Current event:

```text
event: state.changed
data: {"resources":["status"]}
```

Possible resources are `status`, `queue`, `history`, `repeat`, and `playlists`.
A connection also emits a comment immediately and periodic keepalive comments.

## Status playback start

`GET /status` includes:

```json
"playback_started_at": "2026-09-20T12:34:56.123456+00:00"
```

The value is the controller's logical playback start timestamp for the current
track and is `null` when no track is active. It is intended for UI presentation
such as an approximate elapsed-time indicator, not for sample-accurate seeking.
