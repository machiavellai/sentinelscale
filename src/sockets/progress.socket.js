
'use strict';

const { QueueEvents } = require('bullmq');
const redisConfig = require('../config/RedisConfig');

function initProgressSocket(io) {
  const queueEvents = new QueueEvents('media-ingestion', { connection: redisConfig });

  io.on('connection', (socket) => {
    socket.on('job:subscribe', (jobId) => {
      socket.join(jobId);
    });
  });

  // Fires once, right when a delayed job is first stored in BullMQ's delayed
  // set (i.e. right after upload.route.js calls `queue.add(..., { delay })`).
  // Without this listener, a client that subscribes to a delayed job would
  // hear nothing at all until the delay expires and processing actually
  // starts - this event lets the UI show "scheduled for <time>" in the
  // meantime instead of just going quiet.
  //
  // Gotcha: BullMQ names this event's payload field `delay`, but despite the
  // name it is NOT a duration - it's the absolute timestamp (milliseconds
  // since epoch) at which the job becomes eligible to run, sent to us as a
  // string over Redis. We convert it with Number(...) and then wrap it in
  // `new Date(...)` to turn it into a normal timestamp.
  queueEvents.on('delayed', ({ jobId, delay }) => {
    io.to(jobId).emit('job:scheduled', {
      jobId,
      processAt: new Date(Number(delay)).toISOString(),
    });
  });

  queueEvents.on('progress', ({ jobId, data: progress }) => {
    io.to(jobId).emit('job:progress', { jobId, progress });
  });

  queueEvents.on('completed', ({ jobId, returnvalue: result }) => {
    io.to(jobId).emit('job:completed', { jobId, result });
  });

  queueEvents.on('failed', ({ jobId, failedReason: reason }) => {
    io.to(jobId).emit('job:failed', { jobId, reason });
  });
}

module.exports = { initProgressSocket };
