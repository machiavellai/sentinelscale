"use strict";

const { Worker } = require("bullmq");
const fs = require("fs");
const path = require("path");
const redisConfig = require("../config/RedisConfig");
const { imagePool, videoPool } = require("../workers/pool");
const { toSharedBuffer } = require("../utils/sharedBuffer");

const PROCESSED_DIR = path.resolve(__dirname, "../../processed");

const ingestionWorker = new Worker(
  "media-ingestion",
  async (job) => {
    console.log(`picked up ${job.id} priority=${job.opts.priority}`);
    const { filePath, fileType, jobId, options } = job.data;

    await job.updateProgress(5);

    let result;

    if (fileType === "image") {
      const fileBuffer = fs.readFileSync(filePath);
      const sab = toSharedBuffer(fileBuffer);

      const lockSab = new SharedArrayBuffer(4);

      await job.updateProgress(20);

      const rawResult = await imagePool.run({ sab, lockSab, jobId, options });
      await job.updateProgress(90);

      const outputPath = path.join(
        PROCESSED_DIR,
        `${jobId}.${rawResult.format}`,
      );
      fs.writeFileSync(outputPath, rawResult.data);
      result = {
        jobId,
        outputPath,
        format: rawResult.format,
        byteLength: rawResult.byteLength,
      };
    } else if (fileType === "video") {
      const outputPath = path.join(
        PROCESSED_DIR,
        `${jobId}.${options.format || "mp4"}`,
      );
      result = await videoPool.run({
        inputPath: filePath,
        jobId,
        options,
        outputPath,
      });
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
  },
);

ingestionWorker.on("completed", (job) => {
  console.log(`Job ${job.id} completed`);
});

ingestionWorker.on("progress", (job, progress) => {
  console.log(`Job ${job.id} progress=${progress}`);
});

ingestionWorker.on("failed", (job, err) => {
  console.error(`Job ${job && job.id} failed: ${err.message}`);
});

ingestionWorker.on("error", (err) => {
  console.error("Worker error:", err);
});


module.exports = ingestionWorker;
