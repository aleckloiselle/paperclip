import { parentPort } from "node:worker_threads";
import { createHash } from "node:crypto";
import {
  readNativeJournalProjection,
  scanNativeStateFile,
} from "./native-journal-projection.js";
import type { NativeJournalProofJob } from "./native-journal-projection-async.js";

if (!parentPort) throw new Error("native_journal_worker_requires_parent");
parentPort.on("message", (job: NativeJournalProofJob) => {
  try {
    let result: unknown;
    if (job.kind === "projection")
      result = readNativeJournalProjection(job.path, job.purpose);
    else if (job.kind === "scan")
      result = scanNativeStateFile(job.path, job.maximum);
    else if (job.kind === "includes") {
      let tail = "",
        found = false;
      const overlap = Math.max(0, ...job.needles.map((value) => value.length));
      scanNativeStateFile(job.path, job.maximum, (chunk) => {
        const value = tail + chunk.toString("utf8");
        if (job.needles.some((needle) => value.includes(needle))) found = true;
        tail = value.slice(-overlap);
      });
      result = found;
    } else {
      const digest = createHash("sha256").update("[");
      const fileSha256 = job.files.map((file, index) => {
        digest.update(index ? ',"' : '"');
        const proof = scanNativeStateFile(file.path, file.maximum, (chunk) =>
          digest.update(chunk.toString("base64")),
        );
        digest.update('"');
        return proof.sha256;
      });
      result = { sha256: digest.update("]").digest("hex"), fileSha256 };
    }
    parentPort!.postMessage({ result });
  } catch (error) {
    const message =
      error instanceof Error && /^native_[a-z_]+$/.test(error.message)
        ? error.message
        : "native_journal_worker_read_failed";
    parentPort!.postMessage({ error: message });
  }
});
