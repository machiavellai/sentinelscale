# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start production server
npm start

# Start with file-watching (auto-restart on changes)
npm run dev
```

No test suite or linter is configured. Redis must be running before starting the server.

## Environment Variables

Create a `.env` file at the project root:

```
PORT=
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
MAX_QUEUE_SIZE=5000      # max BullMQ waiting jobs before 503
WORKER_THREADS=          # defaults to os.cpus().length
```

## Architecture

SentinelScale is a media ingestion and processing server. The request flow is:

1. **HTTP upload** (`POST /api/upload`) — `src/routes/upload.route.js` streams the multipart file to `uploads/` via busboy, then enqueues a BullMQ job.
2. **BullMQ queue** (`src/queues/ingestion.queue.js`) — named `media-ingestion`, backed by Redis (ioredis). Jobs retry 3× with exponential backoff.
3. **BullMQ worker** (`src/queues/ingestion.worker.js`) — picks up jobs at concurrency 5. For images it copies the file into a `SharedArrayBuffer` and dispatches to the image Piscina pool; for videos it dispatches the file path to the video Piscina pool.
4. **Piscina thread pools** (`src/workers/pool.js`) — two pools: `imagePool` (full thread count) and `videoPool` (half thread count).
   - `src/workers/image.worker.js` — uses **sharp** to resize and convert to webp (or specified format).
   - `src/workers/video.worker.js` — uses **fluent-ffmpeg** (requires `ffmpeg` binary on PATH) to transcode to mp4 or specified format.
5. **Socket.IO** (`src/server.js`) — wired up but only logs connections; intended for pushing job progress to clients by `jobId`.
6. **Shared buffer util** (`src/utils/sharedBuffer.js`) — copies a Node Buffer into a `SharedArrayBuffer` with a spinlock so the image worker thread can safely read it without serialization overhead.

The `uploads/` directory must exist at the project root (the upload route writes there). The worker deletes the temp file after processing.

## Key Design Constraints

- Image data crosses the main-thread → worker boundary via `SharedArrayBuffer` + `Atomics` spinlock (zero-copy). Don't serialize image buffers through the BullMQ job payload.
- The `ingestion.worker.js` file in `src/queues/` is separate from the Piscina workers in `src/workers/` — the BullMQ worker is the coordinator; the Piscina workers do the CPU-bound work.
- `ffmpeg` must be installed and on PATH for video processing to work.
