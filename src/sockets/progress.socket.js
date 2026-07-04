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
