"use strict";

const { Readable } = require("stream");

// file-type is ESM-only (as of v22), but this codebase is CommonJS, so it
// can't be `require()`'d directly - that throws ERR_REQUIRE_ESM. A dynamic
// `import()` works from an async function regardless of the caller's module
// system, so we lazy-load it once and cache the promise for every later call.
let fileTypeStreamPromise;

function loadFileTypeStream() {
  if (!fileTypeStreamPromise) {
    fileTypeStreamPromise = import("file-type").then(
      (mod) => mod.fileTypeStream,
    );
  }
  return fileTypeStreamPromise;
}

// We use `fileTypeStream` here, not `fileTypeFromStream` - `fileTypeFromStream`
// consumes the stream to sniff the type and gives back only `{ext, mime}`,
// with no way to recover the bytes afterward. `fileTypeStream` sniffs the
// same head bytes but hands back a NEW stream with them reattached at the
// front, tagged with a `.fileType` property, so the full file can still be
// piped onward afterward. We need both: know the type AND still save the
// complete file to disk.
//
// `fileTypeStream` sniffs a WHATWG web ReadableStream (it calls
// `.getReader()` internally), not a Node.js stream - so a busboy fileStream
// has to be converted with `Readable.toWeb()` first. The result comes back
// as a web stream too, so it's converted back with `Readable.fromWeb()` so
// it can still be `.pipe()`d like the original. `Readable.fromWeb()` doesn't
// copy custom properties, so `.fileType` is carried over manually.
async function detectStreamType(nodeReadable) {
  const fileTypeStream = await loadFileTypeStream();

  const webReadable = Readable.toWeb(nodeReadable);
  const detectionWebStream = await fileTypeStream(webReadable);

  const detectedStream = Readable.fromWeb(detectionWebStream);
  detectedStream.fileType = detectionWebStream.fileType;

  return detectedStream;
}

module.exports = { detectStreamType };
