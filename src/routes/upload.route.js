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
      processingOptions[name] = value;
    });

    bb.on('finish', async () => {
      if (!savedFilePath || !fileType) {
        return res.status(400).json({
          error: 'No valid media file was uploaded.',
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
        { jobId }
      );

      return res.status(202).json({
        jobId,
        message: 'Upload accepted. Subscribe to this jobId for progress.',
      });
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