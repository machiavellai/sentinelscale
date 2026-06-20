'use strict';

const os = require('os');
const path = require('path');
const Piscina = require('piscina');

require('dotenv').config();

const maxThreads = process.env.WORKER_THREADS
  ? parseInt(process.env.WORKER_THREADS, 10)
  : os.cpus().length;

const imagePool = new Piscina({
  filename: path.resolve(__dirname, 'image.worker.js'),
  maxThreads,
  idleTimeout: 30000,
});

const videoPool = new Piscina({
  filename: path.resolve(__dirname, 'video.worker.js'),
  maxThreads: Math.max(1, Math.floor(maxThreads / 2)),
  idleTimeout: 60000,
});

module.exports = { imagePool, videoPool };