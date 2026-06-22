# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start Redis (requires Docker)
docker-compose up -d

# Start production server
npm start

# Start with file-watching (auto-restart on changes)
npm run dev
```

No test suite or linter is configured. Redis must be running before starting the server.

## First-time setup

`sharp`, `piscina`, and `fluent-ffmpeg` are not in `package.json` — install them manually:

```bash
npm install express bullmq ioredis piscina socket.io sharp busboy dotenv fluent-ffmpeg --save
npm install nodemon --save-dev
mkdir uploads
```

`ffmpeg` binary must also be on PATH for video processing (`brew install ffmpeg` on Mac; add to Docker image for deployment).

## Environment Variables

Create a `.env` file at the project root:

```
PORT=3000
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
MAX_QUEUE_SIZE=5000      # max BullMQ waiting jobs before 503
WORKER_THREADS=          # defaults to os.cpus().length
```

## API

- `POST /api/upload` — multipart upload. Returns `{ jobId, message }` with HTTP 202. Optional form fields alongside the file: `width`, `height`, `format` (passed as `processingOptions` to the worker). Returns 503 when queue exceeds `MAX_QUEUE_SIZE`.
- `GET /health` — liveness check, returns `{ status: "ok" }`.

## Socket.io protocol

After a successful upload, clients connect to Socket.io and subscribe to progress events for their job:

```js
socket.emit('job:subscribe', jobId);         // join the room
socket.on('job:progress',   ({ jobId, progress }) => …);  // 0–100
socket.on('job:completed',  ({ jobId, result })   => …);
socket.on('job:failed',     ({ jobId, reason })   => …);
```

Progress events are broadcast via `QueueEvents` (Redis pub/sub), so this works across multiple server processes — the API server doesn't need to be the same process as the BullMQ worker.

## Architecture

SentinelScale is a high-performance media ingestion and processing server. The request flow is:

1. **HTTP upload** (`src/routes/upload.route.js`) — streams the multipart file directly to `uploads/` via busboy (never buffers the full file in RAM), checks queue backpressure first, then enqueues a BullMQ job.
2. **BullMQ queue** (`src/queues/ingestion.queue.js`) — named `media-ingestion`, backed by Redis (ioredis). Jobs retry 3× with exponential backoff; keeps last 100 completed and 500 failed.
3. **BullMQ worker** (`src/queues/ingestion.worker.js`) — picks up jobs at concurrency 5. For images it copies the file into a `SharedArrayBuffer` and dispatches to the image Piscina pool; for videos it dispatches the file path to the video Piscina pool. Calls `job.updateProgress()` throughout.
4. **Piscina thread pools** (`src/workers/pool.js`) — two pools: `imagePool` (full thread count) and `videoPool` (half thread count), kept separate so slow video jobs can't starve image jobs.
   - `src/workers/image.worker.js` — uses **sharp** to resize (fit: inside, no upscale) and convert; default output format is webp.
   - `src/workers/video.worker.js` — uses **fluent-ffmpeg** to transcode (libx264 + aac); output written to `os.tmpdir()`.
5. **Real-time progress** (`src/sockets/progress.socket.js`) — `QueueEvents` listens to Redis pub/sub for `progress`, `completed`, and `failed` events and re-emits them to the matching Socket.io room (room = jobId).
6. **Shared buffer util** (`src/utils/sharedBuffer.js`) — copies a Node Buffer into a `SharedArrayBuffer` with a spinlock so image worker threads can read it without serialization overhead.

`server.js` must `require('./queues/ingestion.worker')` to start the BullMQ worker on boot; without that line, jobs sit in the queue forever.

The worker deletes the temp upload file after processing.

## Key Design Constraints

- Image data crosses the main-thread → worker boundary via `SharedArrayBuffer` + `Atomics` spinlock (zero-copy). Don't serialize image buffers through the BullMQ job payload — a 50MB image × multiple workers would clone hundreds of MB.
- The `ingestion.worker.js` in `src/queues/` is the coordinator (BullMQ); the files in `src/workers/` are the CPU workers (Piscina). They are separate layers.
- BullMQ requires `maxRetriesPerRequest: null` on the ioredis config — leaving it at the ioredis default (20) causes BullMQ to throw on startup.
- Two separate Piscina pools (image vs. video) prevent slow video transcodes from blocking image processing.
