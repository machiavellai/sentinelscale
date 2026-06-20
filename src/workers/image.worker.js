'use strict';

const sharp = require('sharp');
const { lockBuffer, unlockBuffer } = require('../utils/sharedBuffer');

module.exports = async function processImage({ sab, lockSab, jobId, options }) {
  lockBuffer(lockSab);

  let inputBuffer;
  try {
    inputBuffer = Buffer.from(new Uint8Array(sab));
  } finally {
    unlockBuffer(lockSab);
  }

  const pipeline = sharp(inputBuffer);

  if (options.width || options.height) {
    pipeline.resize(options.width, options.height, {
      fit: 'inside',
      withoutEnlargement: true,
    });
  }

  const format = options.format || 'webp';
  pipeline[format]({ quality: 80 });

  const outputBuffer = await pipeline.toBuffer();

  return {
    jobId,
    data: outputBuffer,
    format,
    byteLength: outputBuffer.byteLength,
  };
};