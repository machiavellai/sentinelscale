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

// How BullMQ delay works: when you call `queue.add(name, data, { delay: <ms> })`,
// BullMQ does NOT run the job right away. It stores the job in a special Redis
// sorted set (the "delayed" set), scored by the timestamp `now + delay`. A job
// only becomes eligible for a worker to pick up once that timestamp has passed.
// So `delay` is "how many milliseconds from now should this job start waiting
// in line" - it is NOT how long the job itself takes to run.
//
// We cap how far into the future someone can schedule a job. Without a cap,
// a client could schedule a job 10 years out, and its uploaded file would sit
// on disk on our server for 10 years doing nothing but taking up space. This
// is configurable via env var so ops can tune it without a code change, same
// pattern as MAX_QUEUE_SIZE above.
const MAX_DELAY_MS = parseInt(process.env.MAX_DELAY_MS, 10) || 24 * 60 * 60 * 1000; // default: 24 hours

router.post('/upload', async (req, res) => {
  try {
    const waitingCount = await ingestionQueue.getWaitingCount();

    if (waitingCount >= MAX_QUEUE_SIZE) {
      return res.status(503).json({
        error: 'Queue at capacity. Please retry later.',
        retryAfter: 30,
      });
    }

    const jobId = randomUUID();
    const bb = busboy({ headers: req.headers });

    let savedFilePath = null;
    let fileType = null;
    const processingOptions = {};
    // `delay` and `processAt` are scheduling instructions for BullMQ, not
    // processing instructions for sharp/ffmpeg - so we keep them in their own
    // object instead of mixing them into `processingOptions` (which gets
    // forwarded straight into the Piscina worker pools further down the
    // pipeline and shouldn't have to know/care about scheduling fields).
    const scheduling = {};

    bb.on('file', (fieldname, fileStream, info) => {
      const { filename, mimeType } = info;

      if (mimeType.startsWith('image/')) {
        fileType = 'image';
      } else if (mimeType.startsWith('video/')) {
        fileType = 'video';
      } else {
        fileStream.resume();
        return res.status(415).json({
          error: `Unsupported media type: ${mimeType}`,
        });
      }

      const ext = path.extname(filename) || '.bin';
      savedFilePath = path.join(UPLOADS_DIR, `${jobId}${ext}`);

      const writeStream = fs.createWriteStream(savedFilePath);
      fileStream.pipe(writeStream);

      writeStream.on('error', (err) => {
        console.error('File write error:', err);
        return res.status(500).json({ error: 'Failed to save upload' });
      });
    });

    bb.on('field', (name, value) => {
      // Route the two scheduling fields into `scheduling`; everything else
      // (width, height, format, ...) keeps going into `processingOptions` like
      // it always has.
      if (name === 'delay' || name === 'processAt') {
        scheduling[name] = value;
      } else {
        processingOptions[name] = value;
      }
    });

    bb.on('finish', async () => {
      if (!savedFilePath || !fileType) {
        return res.status(400).json({
          error: 'No valid media file was uploaded.',
        });
      }

      // A client can schedule a job in one of two equivalent ways:
      //   - `delay`: "start this job N milliseconds from now"     e.g. delay=15000
      //   - `processAt`: "start this job at this exact timestamp" e.g. processAt=2026-07-04T09:00:00Z
      // Only one should be used at a time - allowing both would mean we'd
      // have to decide which one wins, which is just a source of confusing
      // bugs, so we simply reject the request instead.
      if (scheduling.delay !== undefined && scheduling.processAt !== undefined) {
        return res.status(400).json({
          error: 'Provide either "delay" or "processAt", not both.',
        });
      }

      // `delayMs` is what we'll actually hand to BullMQ. It always starts at
      // 0, meaning "no delay, process as soon as a worker is free" - which is
      // exactly today's existing behavior when neither field is sent.
      let delayMs = 0;

      if (scheduling.delay !== undefined) {
        delayMs = Number(scheduling.delay);
        // `Number('abc')` is NaN, and NaN comparisons are always false, so we
        // have to check for NaN explicitly with Number.isFinite - a plain
        // `if (!delayMs)` would NOT catch this case.
        if (!Number.isFinite(delayMs) || !Number.isInteger(delayMs)) {
          return res.status(400).json({
            error: '"delay" must be an integer number of milliseconds.',
          });
        }
      } else if (scheduling.processAt !== undefined) {
        // Date.parse gives us the target time as milliseconds-since-epoch.
        // Subtracting "now" converts "run at this clock time" into "run in
        // this many milliseconds from now", which is the only form BullMQ
        // actually understands.
        const targetTimestamp = Date.parse(scheduling.processAt);
        if (Number.isNaN(targetTimestamp)) {
          return res.status(400).json({
            error: '"processAt" must be a valid date/time string (e.g. an ISO 8601 timestamp).',
          });
        }
        delayMs = targetTimestamp - Date.now();
      }

      // One validation step covers both scheduling styles at once:
      //   - a negative `delay` (someone asked to go back in time), and
      //   - a `processAt` that's already in the past (same problem, different
      //     input) both result in a negative `delayMs` here, so one check
      //     catches both.
      if (delayMs < 0) {
        return res.status(400).json({
          error: 'Scheduled time must be in the future.',
        });
      }

      if (delayMs > MAX_DELAY_MS) {
        return res.status(400).json({
          error: `Cannot schedule more than ${MAX_DELAY_MS}ms into the future.`,
        });
      }

      await ingestionQueue.add(
        'process-media',
        {
          filePath: savedFilePath,
          fileType,
          jobId,
          options: processingOptions,
        },
        { jobId, delay: delayMs }
      );

      const response = {
        jobId,
        message: 'Upload accepted. Subscribe to this jobId for progress.',
      };

      // Only mention scheduling in the response when a delay actually applies -
      // no point telling the client "scheduled for right now."
      if (delayMs > 0) {
        response.scheduledFor = new Date(Date.now() + delayMs).toISOString();
        response.message = `Upload accepted and scheduled to process at ${response.scheduledFor}.`;
      }

      return res.status(202).json(response);
    });

    bb.on('error', (err) => {
      console.error('Upload parsing error:', err);
      return res.status(500).json({ error: 'Upload failed' });
    });

    req.pipe(bb);
  } catch (err) {
    console.error('Upload route error:', err);
    return res.status(500).json({ error: 'Server error during upload' });
  }
});

module.exports = router;