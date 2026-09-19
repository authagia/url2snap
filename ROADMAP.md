# Roadmap

The current design goal is a small HTTP control service that delegates media
handling to mpv and synchronized multi-room delivery to Snapcast.

## Phase 16 — Public release preparation

- GitHub-ready README and API documentation
- Explicit environment/configuration examples
- Runtime state excluded from source control
- Basic CI for tests and compile checks

## Phase 17 — Real deployment hardening

- End-to-end tests with real mpv + Snapserver + multiple Snapclients
- Long-running playback / queue / repeat tests
- mpv crash and resolver failure scenarios
- Snapserver restart / FIFO disappearance scenarios
- Graceful shutdown and restart behavior
- Review command serialization and controller boundaries under concurrency

## Phase 18 — Containerization milestone

Containerization is intentionally deferred until the host deployment behavior
is stable.

Target:

- Build a reproducible application image
- Keep Queue / Playlist / History under a mounted data volume
- Make mpv and yt-dlp availability explicit in the image
- Make the Snapcast integration explicit rather than hiding host assumptions
- Decide whether Snapserver is external to the container or part of a composed deployment
- Document FIFO/runtime handling for both host and container deployments

The first container milestone does **not** require moving Snapserver into the
same container as audio-source.

## Later / optional

- Configurable CORS origins
- Authentication / authorization
- SQLite repositories
- WebSocket or SSE status events
- Additional source-specific resolvers
- Web UI
