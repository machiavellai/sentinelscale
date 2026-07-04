'use strict';

const ffmpeg = require('fluent-ffmpeg');
const ffmpegStatic = require('ffmpeg-static');
const path = require('path');
const os = require('os');

ffmpeg.setFfmpegPath(ffmpegStatic);

const TIMEOUT_MS = 5 * 60 * 1000; // 10 minutes

module.exports = function processVideo({ inputPath, jobId, options, outputPath }) {
  return new Promise((resolve, reject) => {
    const outputFormat = options.format || 'mp4';
    const finalOutputPath = outputPath || path.join(os.tmpdir(), `${jobId}.${outputFormat}`);

    let lastPct = 0;
    const timer = setTimeout(() => {
      command.kill('SIGKILL');
      reject(new Error(`FFmpeg timeout after ${TIMEOUT_MS / 60000} minutes for job ${jobId}`));
    }, TIMEOUT_MS);

    const command = ffmpeg(inputPath)
      .outputFormat(outputFormat)
      .videoCodec('libx264')
      .audioCodec('aac')
      .on('progress', (progress) => {
        // percent is null when ffmpeg can't read total duration from container
        const pct = progress.percent != null
          ? Math.min(100, Math.round(progress.percent))
          : lastPct;
        if (pct !== lastPct) {
          lastPct = pct;
          console.log(`[${jobId}] video progress: ${pct}%`);
        }
      })
      .on('error', (err) => {
        clearTimeout(timer);
        reject(new Error(`FFmpeg error for job ${jobId}: ${err.message}`));
      })
      .on('end', () => {
        clearTimeout(timer);
        resolve({ jobId, outputPath: finalOutputPath, format: outputFormat });
      })
      .save(finalOutputPath);
  });
};