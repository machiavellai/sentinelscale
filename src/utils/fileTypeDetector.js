"use strict";

const { Readable } = require("stream");

// file-type is ESM-only, so it's dynamically imported and cached rather than required.
let fileTypeStreamPromise;

function loadFileTypeStream() {
  if (!fileTypeStreamPromise) {
    fileTypeStreamPromise = import("file-type").then(
      (mod) => mod.fileTypeStream,
    );
  }
  return fileTypeStreamPromise;
}

// Uses fileTypeStream (not fileTypeFromStream) so the sniffed bytes can still be piped onward afterward; converts to/from
// WHATWG web streams since that's what file-type expects, and carries the `.fileType` tag across manually.
async function detectStreamType(nodeReadable) {
  const fileTypeStream = await loadFileTypeStream();

  const webReadable = Readable.toWeb(nodeReadable);
  const detectionWebStream = await fileTypeStream(webReadable);

  const detectedStream = Readable.fromWeb(detectionWebStream);
  detectedStream.fileType = detectionWebStream.fileType;

  return detectedStream;
}

module.exports = { detectStreamType };
