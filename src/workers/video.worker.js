'use strict';

const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const os = require('os');

module.exports = function processVideo({ inputPath, jobId, options }) {
  return new Promise((resolve, reject) => {
    const outputFormat = options.format || 'mp4';
    const outputPath = path.join(os.tmpdir(), `${jobId}.${outputFormat}`);

    ffmpeg(inputPath)
      .outputFormat(outputFormat)
      .videoCodec('libx264')
      .audioCodec('aac')
      .on('progress', (progress) => {
        const pct = Math.round(progress.percent || 0);
        process.stdout.write(`[${jobId}] video progress: ${pct}%\r`);
      })
      .on('error', (err) => {
        reject(new Error(`FFmpeg error for job ${jobId}: ${err.message}`));
      })
      .on('end', () => {
        resolve({ jobId, outputPath, format: outputFormat });
      })
      .save(outputPath);
  });
};