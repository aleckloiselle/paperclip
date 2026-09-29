import { createHash } from "node:crypto";
import { closeSync, constants, fstatSync, lstatSync, openSync, readSync } from "node:fs";
import { parentPort } from "node:worker_threads";
import { DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES } from "../control-plane/durable-prp-control-plane.js";
import type { RunnerdStateFile, RunnerdStateRead } from "./runnerd-state-reader.js";

function read(file: RunnerdStateFile): RunnerdStateRead {
  const fd = openSync(file.path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd);
    if (!before.isFile() || before.size > file.maximum)
      throw new Error("native_runner_state_file_unsafe");
    const bytes = Buffer.allocUnsafe(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      const count = readSync(fd, bytes, offset, Math.min(48 * 1024, bytes.length - offset), offset);
      if (!count) throw new Error("native_runner_state_file_changed");
      offset += count;
    }
    const state: unknown = JSON.parse(bytes.toString("utf8"));
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    const after = fstatSync(fd);
    const pathNow = lstatSync(file.path);
    if ([after, pathNow].some((stat) =>
      !stat.isFile() || stat.dev !== before.dev || stat.ino !== before.ino ||
      stat.size !== before.size || stat.mtimeMs !== before.mtimeMs || stat.ctimeMs !== before.ctimeMs
    )) throw new Error("native_runner_state_file_changed");
    return {
      state: state !== null && typeof state === "object" && !Array.isArray(state)
        ? state as Record<string, unknown> : {},
      sha256,
    };
  } finally {
    closeSync(fd);
  }
}

if (!parentPort) throw new Error("native_runner_state_worker_requires_parent");
parentPort.on("message", (files: RunnerdStateFile[]) => {
  try {
    if (!Array.isArray(files) || files.length < 1 || files.length > 3 ||
      files.some((file) => !Number.isSafeInteger(file.maximum) || file.maximum < 1 ||
        file.maximum > DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES) ||
      files.reduce((sum, file) => sum + file.maximum, 0) > 256 * 1024 * 1024)
      throw new Error("native_runner_state_file_unsafe");
    parentPort!.postMessage({ result: files.map(read) });
  } catch (error) {
    const message = error instanceof Error && /^native_[a-z_]+$/.test(error.message)
      ? error.message : "native_runner_state_read_failed";
    parentPort!.postMessage({ error: message });
  }
});
