'use strict';

function toSharedBuffer(nodeBuffer) {
  const sab = new SharedArrayBuffer(nodeBuffer.byteLength);
  const view = new Uint8Array(sab);
  view.set(nodeBuffer);
  return sab;
}

function lockBuffer(lockSab) {
  const lock = new Int32Array(lockSab);
  while (Atomics.compareExchange(lock, 0, 0, 1) !== 0) {
    Atomics.wait(lock, 0, 1, 5);
  }
}

function unlockBuffer(lockSab) {
  const lock = new Int32Array(lockSab);
  Atomics.store(lock, 0, 0);
  Atomics.notify(lock, 0, 1);
}

module.exports = { toSharedBuffer, lockBuffer, unlockBuffer };