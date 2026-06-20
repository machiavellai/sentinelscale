'use strict';

const { Worker } = require('bullmq');
const fs = require('fs');
const redisConfig = require('../config/redis');
const { imagePool, videoPool } = require('../workers/pool');
const { toSharedBuffer } = require('../utils/sharedBuffer');

const ingestionWorker = new Worker(
  'media-ingestion',
  async (job) => {
    const { filePath, fileType, jobId, options } = job.data;

    await job.updateProgress(5);

    const fileBuffer = fs.readFileSync(filePath);
    await job.updateProgress(15);

    let result;

    if (fileType === 'image') {
      const sab = toSharedBuffer(fileBuffer);
      const lockSab = new SharedArrayBuffer(4);

      await job.updateProgress(20);

      result = await imagePool.run({
        sab,
        lockSab,
        jobId,
        options,
      });

      await job.updateProgress(90);
    } else if (fileType === 'video') {
      result = await videoPool.run({ inputPath: filePath, jobId, options });
      await job.updateProgress(90);
    } else {
      throw new Error(`Unknown fileType: ${fileType}`);
    }

    fs.unlink(filePath, () => {});
    await job.updateProgress(100);

    return result;
  },
  {
    connection: redisConfig,
    concurrency: 5,
  }
);

ingestionWorker.on('completed', (job) => {
  console.log(`Job ${job.id} completed`);
});

ingestionWorker.on('failed', (job, err) => {
  console.error(`Job ${job && job.id} failed: ${err.message}`);
});

ingestionWorker.on('error', (err) => {
  console.error('Worker error:', err);
});

module.exports = ingestionWorker;