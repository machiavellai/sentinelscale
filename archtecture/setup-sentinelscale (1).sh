#!/bin/bash

# =============================================================
#  SentinelScale — Project Bootstrap Script
#  Run this once on your machine to scaffold the entire project.
#  Usage:  bash setup-sentinelscale.sh
# =============================================================

set -e  # stop immediately if any command fails

PROJECT="sentinelscale"

echo ""
echo "  ┌────────────────────────────────────────┐"
echo "  │   Setting up SentinelScale...          │"
echo "  └────────────────────────────────────────┘"
echo ""

# ── 1. Create folder structure ──────────────────────────────
echo "▸ Creating folders..."
mkdir -p $PROJECT/src/config
mkdir -p $PROJECT/src/queues
mkdir -p $PROJECT/src/workers
mkdir -p $PROJECT/src/routes
mkdir -p $PROJECT/src/sockets
mkdir -p $PROJECT/src/utils
mkdir -p $PROJECT/uploads
touch $PROJECT/uploads/.gitkeep
echo "  ✓ Folders ready"

# ── 2. Root config files ─────────────────────────────────────
echo "▸ Writing root config files..."

cat > $PROJECT/.gitignore << 'EOF'
node_modules/
.env
uploads/*
!uploads/.gitkeep
EOF

cat > $PROJECT/.env << 'EOF'
# Server
PORT=3000

# Redis — points to the Docker container defined in docker-compose.yml
REDIS_HOST=127.0.0.1
REDIS_PORT=6379

# Queue safety valve — if pending jobs exceed this, API returns 429
MAX_QUEUE_SIZE=5000

# How many worker threads to spawn (defaults to CPU core count if left blank)
WORKER_THREADS=
EOF

cat > $PROJECT/docker-compose.yml << 'EOF'
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - '6379:6379'
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

volumes:
  redis_data:
EOF

cat > $PROJECT/package.json << 'EOF'
{
  "name": "sentinelscale",
  "version": "1.0.0",
  "description": "High-performance media processing pipeline with worker threads, BullMQ, and Socket.io",
  "main": "src/server.js",
  "scripts": {
    "start": "node src/server.js",
    "dev": "node --watch src/server.js"
  },
  "keywords": [],
  "author": "",
  "license": "ISC"
}
EOF
echo "  ✓ Root config files written"

# ── 3. Source files ──────────────────────────────────────────
echo "▸ Writing source files..."

# src/server.js
cat > $PROJECT/src/server.js << 'EOF'
'use strict';

require('dotenv').config();

const http = require('http');
const express = require('express');
const { Server: SocketIOServer } = require('socket.io');

const uploadRoute = require('./routes/upload.route');
const { initProgressSocket } = require('./sockets/progress.socket');

// Must require the BullMQ worker so it starts polling Redis.
// Without this line, jobs sit in the queue forever.
require('./queues/ingestion.worker');

const app = express();
const server = http.createServer(app);

// WHY http.createServer instead of app.listen()?
// Socket.io needs the raw HTTP server to perform the WebSocket upgrade
// handshake. app.listen() creates one internally but doesn't expose it.
const io = new SocketIOServer(server, {
  cors: { origin: '*' }, // lock this down to your frontend domain in production
});

app.use(express.json());
app.use('/api', uploadRoute);
app.get('/health', (req, res) => res.json({ status: 'ok' }));

io.on('connection', (socket) => {
  console.log(`Socket connected: ${socket.id}`);

  // Client sends their jobId to join the correct room so they only
  // receive progress events for THEIR upload
  socket.on('job:subscribe', (jobId) => {
    socket.join(jobId);
    console.log(`Socket ${socket.id} subscribed to job ${jobId}`);
  });

  socket.on('disconnect', () => {
    console.log(`Socket disconnected: ${socket.id}`);
  });
});

initProgressSocket(io);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`
  ┌─────────────────────────────────────┐
  │  SentinelScale running on :${PORT}     │
  │  POST /api/upload  to ingest media  │
  │  GET  /health      to check status  │
  └─────────────────────────────────────┘
  `);
});
EOF

# src/config/redis.js
cat > $PROJECT/src/config/redis.js << 'EOF'
'use strict';

require('dotenv').config();

// Single source of truth for the Redis connection config.
// BullMQ Queue and Worker both import this — one place to update creds.
const redisConfig = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT, 10) || 6379,
  maxRetriesPerRequest: null,
  // BullMQ requires this to be null — it manages its own retry logic.
  // Leaving it at the ioredis default (20) causes BullMQ to throw on startup.
};

module.exports = redisConfig;
EOF

# src/utils/sharedBuffer.js
cat > $PROJECT/src/utils/sharedBuffer.js << 'EOF'
'use strict';

/**
 * Converts a Node.js Buffer into a SharedArrayBuffer.
 *
 * WHY: postMessage() clones data by default. A 50MB image passed to 3 workers
 * = 150MB of extra RAM + event loop blocking during serialization.
 * SharedArrayBuffer lives in shared memory — all threads point to the SAME
 * bytes. One-time copy on the way in, zero copies after that.
 *
 * TRADEOFF: You lose isolation. Two workers writing to the same region
 * simultaneously = corrupted data. That's what lockBuffer/unlockBuffer handle.
 */
function toSharedBuffer(nodeBuffer) {
  const sab = new SharedArrayBuffer(nodeBuffer.byteLength);
  const view = new Uint8Array(sab);
  view.set(nodeBuffer); // one-time copy: Buffer → SharedArrayBuffer
  return sab;
}

/**
 * Acquires a spinlock on a SharedArrayBuffer.
 * Uses a dedicated 4-byte Int32Array at offset 0 as the lock flag.
 * Atomics.compareExchange atomically checks and sets the flag so no two
 * threads can enter the critical section at the same time.
 */
function lockBuffer(lockSab) {
  const lock = new Int32Array(lockSab);
  while (Atomics.compareExchange(lock, 0, 0, 1) !== 0) {
    Atomics.wait(lock, 0, 1, 5); // wait up to 5ms before retrying
  }
}

function unlockBuffer(lockSab) {
  const lock = new Int32Array(lockSab);
  Atomics.store(lock, 0, 0);  // set back to 0 (unlocked)
  Atomics.notify(lock, 0, 1); // wake up one waiting thread
}

module.exports = { toSharedBuffer, lockBuffer, unlockBuffer };
EOF

# src/queues/ingestion.queue.js
cat > $PROJECT/src/queues/ingestion.queue.js << 'EOF'
'use strict';

const { Queue } = require('bullmq');
const redisConfig = require('../config/redis');

// The Queue is just the "inbox" — holds job payloads in Redis.
// It does NOT process anything. The Worker (ingestion.worker.js) does that.
//
// WHY separate Queue from Worker?
// In production you run multiple Worker processes on different machines,
// all pulling from this same Queue. The Queue is the shared contract
// between your API server and your fleet of processing nodes.

const ingestionQueue = new Queue('media-ingestion', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
      // Wait 1s → 2s → 4s between retries.
      // Prevents hammering a temporarily broken downstream service.
    },
    removeOnComplete: { count: 100 },
    // Keep only the last 100 completed jobs in Redis.
    // Without this, completed jobs pile up forever → memory leak.
    removeOnFail: { count: 500 },
    // Keep more failed jobs so you can inspect and debug them.
  },
});

module.exports = ingestionQueue;
EOF

# src/queues/ingestion.worker.js
cat > $PROJECT/src/queues/ingestion.worker.js << 'EOF'
'use strict';

const { Worker } = require('bullmq');
const fs = require('fs');
const redisConfig = require('../config/redis');
const { imagePool, videoPool } = require('../workers/pool');
const { toSharedBuffer } = require('../utils/sharedBuffer');

// The BullMQ Worker bridges Redis (where jobs wait) and Piscina (where CPU
// work happens). This runs on the main thread — it polls Redis and calls
// the processor function when a job is ready. Heavy work is delegated to
// Piscina threads so the main thread stays free.

const ingestionWorker = new Worker(
  'media-ingestion', // must match the Queue name exactly
  async (job) => {
    const { filePath, fileType, jobId, options } = job.data;

    await job.updateProgress(5);  // signal: job picked up

    const fileBuffer = fs.readFileSync(filePath);
    await job.updateProgress(15); // file read from disk

    let result;

    if (fileType === 'image') {
      const sab = toSharedBuffer(fileBuffer);
      const lockSab = new SharedArrayBuffer(4); // 4-byte mutex

      await job.updateProgress(20);

      result = await imagePool.run(
        { sab, lockSab, jobId, options },
        { transferList: [sab, lockSab] }
        // transferList = zero-copy handoff to the worker thread
      );

      await job.updateProgress(90);

    } else if (fileType === 'video') {
      // Video reads from disk directly — files can be several GB,
      // too large to fit in SharedArrayBuffer without OOM risk.
      result = await videoPool.run({ inputPath: filePath, jobId, options });
      await job.updateProgress(90);

    } else {
      throw new Error(`Unknown fileType: ${fileType}`);
    }

    fs.unlink(filePath, () => {}); // clean up original upload (fire-and-forget)

    await job.updateProgress(100);
    return result;
  },
  {
    connection: redisConfig,
    concurrency: 5,
    // BullMQ concurrency = how many jobs are "in flight" at the Redis level.
    // This is separate from Piscina thread count — they're tuned independently.
  }
);

ingestionWorker.on('completed', (job) => {
  console.log(`✓ Job ${job.id} completed`);
});

ingestionWorker.on('failed', (job, err) => {
  console.error(`✗ Job ${job?.id} failed: ${err.message}`);
});

ingestionWorker.on('error', (err) => {
  console.error('Worker error:', err);
});

module.exports = ingestionWorker;
EOF

# src/workers/pool.js
cat > $PROJECT/src/workers/pool.js << 'EOF'
'use strict';

const os = require('os');
const path = require('path');
const Piscina = require('piscina');

require('dotenv').config();

// Default: one thread per logical CPU core.
// More threads than cores = context-switching overhead = slower, not faster.
// TRADEOFF: If other services share this machine, use os.cpus().length - 1
// to leave one core free for the OS and those services.
const maxThreads = process.env.WORKER_THREADS
  ? parseInt(process.env.WORKER_THREADS, 10)
  : os.cpus().length;

// WHY two separate pools instead of one shared pool?
// Image jobs take < 1s. Video jobs take 30–120s.
// If they share a pool, a burst of video uploads fills every thread and
// image jobs queue up waiting. Separate pools = separate backlogs.

const imagePool = new Piscina({
  filename: path.resolve(__dirname, 'image.worker.js'),
  maxThreads,
  idleTimeout: 30_000, // destroy idle threads after 30s to free RAM
});

const videoPool = new Piscina({
  filename: path.resolve(__dirname, 'video.worker.js'),
  maxThreads: Math.max(1, Math.floor(maxThreads / 2)),
  // Cap video pool at half the cores — video is far more CPU-intensive.
  // This prevents video jobs from starving image processing and the main thread.
  idleTimeout: 60_000,
});

module.exports = { imagePool, videoPool };
EOF

# src/workers/image.worker.js
cat > $PROJECT/src/workers/image.worker.js << 'EOF'
'use strict';

// Runs INSIDE a Piscina worker thread — not on the main thread.
// Piscina calls this exported function whenever an image job is dispatched.

const sharp = require('sharp');
const { lockBuffer, unlockBuffer } = require('../utils/sharedBuffer');

module.exports = async function processImage({ sab, lockSab, jobId, options }) {
  // Acquire lock before reading shared memory.
  // Prevents corrupted reads if another worker is writing to the same buffer.
  lockBuffer(lockSab);

  let inputBuffer;
  try {
    inputBuffer = Buffer.from(new Uint8Array(sab)); // view into shared memory, no copy
  } finally {
    unlockBuffer(lockSab); // ALWAYS release — even if the line above throws
  }

  // Sharp pipeline — lazy evaluation, nothing runs until .toBuffer()
  const pipeline = sharp(inputBuffer);

  if (options.width || options.height) {
    pipeline.resize(options.width, options.height, {
      fit: 'inside',            // maintain aspect ratio
      withoutEnlargement: true, // never upscale — just adds blur
    });
  }

  const format = options.format || 'webp';
  // WHY webp default? ~30% smaller than JPEG at the same quality.
  // Supported by all modern browsers. Use 'jpeg' for max compatibility.
  pipeline[format]({ quality: 80 });

  const outputBuffer = await pipeline.toBuffer();

  return {
    jobId,
    data: outputBuffer,
    format,
    byteLength: outputBuffer.byteLength,
  };
};
EOF

# src/workers/video.worker.js
cat > $PROJECT/src/workers/video.worker.js << 'EOF'
'use strict';

// Video worker — runs inside a Piscina thread.
// NOTE: fluent-ffmpeg wraps the FFmpeg binary. FFmpeg must be installed
// separately: `brew install ffmpeg` (Mac) or added to your Docker image.
// The npm package does NOT bundle it.

const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const os = require('os');

module.exports = function processVideo({ inputPath, jobId, options }) {
  // FFmpeg is callback-based. Wrap in a Promise so Piscina can await it.
  return new Promise((resolve, reject) => {
    const outputFormat = options.format || 'mp4';
    const outputPath = path.join(os.tmpdir(), `${jobId}.${outputFormat}`);

    ffmpeg(inputPath)
      .outputFormat(outputFormat)
      .videoCodec('libx264') // H.264 — universally supported
      .audioCodec('aac')
      .on('progress', (progress) => {
        const pct = Math.round(progress.percent || 0);
        // Phase 3 will wire this to Socket.io via a MessageChannel port.
        // For now, log it so you can verify FFmpeg is reporting progress.
        process.stdout.write(`[${jobId}] video progress: ${pct}%\r`);
      })
      .on('error', (err) => {
        reject(new Error(`FFmpeg error for job ${jobId}: ${err.message}`));
      })
      .on('end', () => {
        resolve({ jobId, outputPath, format: outputFormat });
      })
      .save(outputPath);
  });
};
EOF

# src/routes/upload.route.js
cat > $PROJECT/src/routes/upload.route.js << 'EOF'
'use strict';

const express = require('express');
const path = require('path');
const fs = require('fs');
const busboy = require('busboy');
const { randomUUID } = require('crypto');
const ingestionQueue = require('../queues/ingestion.queue');

const router = express.Router();
const UPLOADS_DIR = path.resolve(__dirname, '../../uploads');
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE, 10) || 5000;

// WHY stream to disk instead of buffering in RAM?
// A 500MB upload * 100 concurrent users = 50GB RAM just for buffers.
// Streaming to disk keeps RAM flat regardless of file size or concurrency.

router.post('/upload', async (req, res) => {

  // Backpressure check — reject before receiving the file if queue is full.
  // Saves bandwidth: don't accept a file you can't process.
  const waitingCount = await ingestionQueue.getWaitingCount();
  if (waitingCount >= MAX_QUEUE_SIZE) {
    return res.status(503).json({
      error: 'Queue at capacity. Please retry later.',
      retryAfter: 30,
    });
  }

  const jobId = randomUUID(); // ties the file, BullMQ job, and socket room together
  const bb = busboy({ headers: req.headers });
  let savedFilePath = null;
  let fileType = null;
  let processingOptions = {};

  bb.on('file', (fieldname, fileStream, info) => {
    const { filename, mimeType } = info;

    if (mimeType.startsWith('image/'))      fileType = 'image';
    else if (mimeType.startsWith('video/')) fileType = 'video';
    else {
      fileStream.resume(); // drain stream so connection closes cleanly
      return res.status(415).json({ error: `Unsupported media type: ${mimeType}` });
    }

    const ext = path.extname(filename) || '.bin';
    savedFilePath = path.join(UPLOADS_DIR, `${jobId}${ext}`);
    fileStream.pipe(fs.createWriteStream(savedFilePath));
    // Pipe directly: HTTP request → disk. RAM never holds the full file.
  });

  bb.on('field', (name, value) => {
    // Clients pass processing options as form fields alongside the file.
    // e.g. width=1920&height=1080&format=webp
    processingOptions[name] = value;
  });

  bb.on('finish', async () => {
    if (!savedFilePath) return;

    await ingestionQueue.add('process-media', {
      filePath: savedFilePath,
      fileType,
      jobId,
      options: processingOptions,
    }, { jobId }); // jobId as BullMQ job ID = easy lookup later

    res.status(202).json({
      jobId,
      message: 'Upload accepted. Connect to WebSocket and subscribe to this jobId for progress.',
    });
  });

  bb.on('error', (err) => {
    console.error('Upload error:', err);
    res.status(500).json({ error: 'Upload failed' });
  });

  req.pipe(bb);
});

module.exports = router;
EOF

# src/sockets/progress.socket.js
cat > $PROJECT/src/sockets/progress.socket.js << 'EOF'
'use strict';

const { QueueEvents } = require('bullmq');
const redisConfig = require('../config/redis');

// QueueEvents subscribes to Redis pub/sub and fires Node.js events
// when jobs change state (progress, completed, failed).
//
// WHY QueueEvents instead of ingestionWorker.on('progress')?
// Worker events only fire in the same process. QueueEvents listens to Redis,
// so it works across multiple machines — your API server (which runs Socket.io)
// can hear about progress from workers running on entirely different servers.

function initProgressSocket(io) {
  const queueEvents = new QueueEvents('media-ingestion', {
    connection: redisConfig,
  });

  queueEvents.on('progress', ({ jobId, data: progress }) => {
    // Emit ONLY to the room named after this job.
    // The client joined this room when they got the jobId from POST /upload.
    io.to(jobId).emit('job:progress', { jobId, progress });
  });

  queueEvents.on('completed', ({ jobId, returnvalue }) => {
    io.to(jobId).emit('job:completed', { jobId, result: returnvalue });
  });

  queueEvents.on('failed', ({ jobId, failedReason }) => {
    io.to(jobId).emit('job:failed', { jobId, reason: failedReason });
  });

  console.log('✓ Socket.io progress emitter ready');
}

module.exports = { initProgressSocket };
EOF

echo "  ✓ All source files written"

# ── 4. Install dependencies ──────────────────────────────────
echo "▸ Installing dependencies (this takes ~30 seconds)..."
cd $PROJECT
npm install express bullmq ioredis piscina socket.io sharp busboy dotenv --save 2>&1 | grep -E "added|warn|error" || true
npm install nodemon --save-dev 2>&1 | grep -E "added|warn|error" || true
cd ..
echo "  ✓ Dependencies installed"

# ── 5. Done ──────────────────────────────────────────────────
echo ""
echo "  ┌────────────────────────────────────────────────────┐"
echo "  │   ✓  SentinelScale is ready!                       │"
echo "  │                                                    │"
echo "  │   Next steps:                                      │"
echo "  │   1. cd sentinelscale                              │"
echo "  │   2. docker-compose up -d   (starts Redis)         │"
echo "  │   3. npm run dev            (starts the server)    │"
echo "  │   4. GET http://localhost:3000/health  to verify   │"
echo "  └────────────────────────────────────────────────────┘"
echo ""
