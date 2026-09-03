"use strict";

const path = require("path");
const fs = require("fs");
const busboy = require("busboy");
const { randomUUID } = require("crypto");
const ingestionQueue = require("../queues/ingestion.queue");
const { detectStreamType } = require("../utils/fileTypeDetector");

const UPLOADS_DIR = path.resolve(__dirname, "../../uploads");
fs.mkdirSync(UPLOADS_DIR, { recursive: true, mode: 0o750 });

// `delay` schedules a job N ms from now; capped so files don't sit on disk indefinitely.
const MAX_DELAY_MS =
  parseInt(process.env.MAX_DELAY_MS, 10) || 24 * 60 * 60 * 1000; // default: 24 hours

// BullMQ priority: 0 = unprioritized/FIFO, 1-10 = ordered tiers (1 beats 2 beats 3...).
const MIN_PRIORITY = 0;
const MAX_PRIORITY = 10;

class ScheduleValidationError extends Error {
  constructor(message) {
    super(message);
    this.status = 400; // every scheduling failure is a 400
  }
}

class PriorityValidationError extends Error {
  constructor(message) {
    super(message);
    this.status = 400; // every priority failure is a 400
  }
}

function resolvePriority(priority) {
  if (priority === undefined || priority === null) {
    return MIN_PRIORITY;
  }

  const priorityValue = Number(priority);

  if (!Number.isFinite(priorityValue) || !Number.isInteger(priorityValue)) {
    throw new PriorityValidationError('"priority" must be an integer number.');
  }

  if (priorityValue < MIN_PRIORITY || priorityValue > MAX_PRIORITY) {
    throw new PriorityValidationError(
      `"priority" must be between ${MIN_PRIORITY} and ${MAX_PRIORITY}.`,
    );
  }

  return priorityValue;
}

// Accepts either `delay` (ms from now) or `processAt` (ISO timestamp), not both.
function resolveScheduleDelay(scheduling) {
  if (scheduling.delay !== undefined && scheduling.processAt !== undefined) {
    throw new ScheduleValidationError(
      'Provide either "delay" or "processAt", not both.',
    );
  }

  let delayMs = 0;

  if (scheduling.delay !== undefined) {
    delayMs = Number(scheduling.delay);
    if (!Number.isFinite(delayMs) || !Number.isInteger(delayMs)) {
      throw new ScheduleValidationError(
        '"delay" must be an integer number of milliseconds.',
      );
    }
  } else if (scheduling.processAt !== undefined) {
    const targetTimestamp = Date.parse(scheduling.processAt);
    if (Number.isNaN(targetTimestamp)) {
      throw new ScheduleValidationError(
        '"processAt" must be a valid date/time string (e.g. an ISO 8601 timestamp).',
      );
    }
    delayMs = targetTimestamp - Date.now();
  }

  if (delayMs < 0) {
    throw new ScheduleValidationError("Scheduled time must be in the future.");
  }

  if (delayMs > MAX_DELAY_MS) {
    throw new ScheduleValidationError(
      `Cannot schedule more than ${MAX_DELAY_MS}ms into the future.`,
    );
  }

  return delayMs;
}

async function handleUpload(req, res) {
  const jobId = randomUUID();
  const bb = busboy({ headers: req.headers });

  let savedFilePath = null;
  let fileType = null;
  let responded = false;
  let fileWritten = Promise.resolve();
  const processingOptions = {};
  const scheduling = {}; // BullMQ scheduling fields, kept separate from processingOptions

  // Guards every response site so a second response after one already sent is a no-op.
  function respond(status, body) {
    if (responded) return;
    responded = true;
    res.status(status).json(body);
  }

  // Removes an orphaned upload before responding, so a rejected/failed request doesn't leak disk space.
  function respondAndCleanup(status, body) {
    if (savedFilePath) {
      fs.unlink(savedFilePath, (err) => {
        if (err && err.code !== "ENOENT") {
          console.error("Failed to remove orphaned upload:", err);
        }
      });
    }
    respond(status, body);
  }

  bb.on("file", (fieldname, fileStream, info) => {
    const { filename } = info;
    // `info.mimeType` is client-supplied and not trusted for routing - detectStreamType sniffs real magic bytes instead.

    const ext = path.extname(filename) || ".bin";
    savedFilePath = path.join(UPLOADS_DIR, `${jobId}${ext}`);

    // Detection is async, so the rest of the per-file logic lives in this IIFE; its promise IS `fileWritten`.
    fileWritten = (async () => {
      try {
        const detectedStream = await detectStreamType(fileStream);
        const sniffedMime = detectedStream.fileType?.mime;

        if (!sniffedMime) {
          detectedStream.resume();
          respond(415, { error: "Could not verify file type." });
          return;
        }

        if (sniffedMime.startsWith("image/")) {
          fileType = "image";
        } else if (sniffedMime.startsWith("video/")) {
          fileType = "video";
        } else {
          detectedStream.resume();
          respond(415, { error: `Unsupported media type: ${sniffedMime}` });
          return;
        }

        const writeStream = fs.createWriteStream(savedFilePath);
        detectedStream.pipe(writeStream);

        await new Promise((resolve) => {
          writeStream.on("finish", resolve);
          writeStream.on("error", (err) => {
            console.error("File write error:", err);
            respondAndCleanup(500, { error: "Failed to save upload" });
            resolve();
          });
        });
      } catch (err) {
        console.error("File type detection error:", err);
        respondAndCleanup(500, { error: "Failed to process upload." });
      }
    })();
  });

  let rawPriority;

  bb.on("field", (name, value) => {
    // Scheduling/priority fields are routed away from processingOptions, which is forwarded as-is to the Piscina workers.
    if (name === "delay" || name === "processAt") {
      scheduling[name] = value;
    } else if (name === "priority") {
      rawPriority = value;
    } else {
      processingOptions[name] = value;
    }
  });

  bb.on("finish", async () => {
    await fileWritten;
    if (responded) return;

    if (!savedFilePath || !fileType) {
      return respond(400, { error: "No valid media file was uploaded." });
    }

    try {
      const delayMs = resolveScheduleDelay(scheduling);
      const priority = resolvePriority(rawPriority);

      await ingestionQueue.add(
        "process-media",
        {
          filePath: savedFilePath,
          fileType,
          jobId,
          options: processingOptions,
        },
        { jobId, delay: delayMs, priority },
      );

      const response = {
        jobId,
        message: "Upload accepted. Subscribe to this jobId for progress.",
      };

      if (delayMs > 0) {
        response.scheduledFor = new Date(Date.now() + delayMs).toISOString();
        response.message = `Upload accepted and scheduled to process at ${response.scheduledFor}.`;
      }

      return respond(202, response);
    } catch (err) {
      if (
        err instanceof ScheduleValidationError ||
        err instanceof PriorityValidationError
      ) {
        return respondAndCleanup(400, { error: err.message });
      }
      console.error("Upload finish handler error:", err);
      return respondAndCleanup(500, { error: "Server error during upload" });
    }
  });

  bb.on("error", (err) => {
    console.error("Upload parsing error:", err);
    respondAndCleanup(500, { error: "Upload failed" });
  });

  req.pipe(bb);
}

module.exports = handleUpload;
