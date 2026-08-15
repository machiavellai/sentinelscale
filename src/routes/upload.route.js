'use strict';

const express = require('express');
const ingestionQueue = require('../queues/ingestion.queue');
const handleUpload = require('../handlers/upload.handler');

const router = express.Router();
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE, 10) || 5000;

router.post('/upload', async (req, res) => {
  try {
    //asking BullMQ for the current number of jobs in the "waiting" state (i.e. jobs
    const waitingCount = await ingestionQueue.getWaitingCount();

    if (waitingCount >= MAX_QUEUE_SIZE) {
      return res.status(503).json({
        error: 'Queue at capacity. Please retry later.',
        retryAfter: 30,
      });
    }

    await handleUpload(req, res);
  } catch (err) {
    console.error('Upload route error:', err);
    return res.status(500).json({ error: 'Server error during upload' });
  }
});

module.exports = router;
