require('dotenv').config();

const redisConfig = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT, 10) || 6379,
  maxRetriesPerRequest: null
};

module.exports = redisConfig;