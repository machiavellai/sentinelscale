const {Queue} = require('bullmq')
const redisConfig = require('../config/redis');

const ingestionQueue = new Queue('media-ingestion', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000
    },
    removeOnComplete: {
      count: 100
    },
    removeOnFail: {
      count: 500
    }
  }
});

module.exports = ingestionQueue;