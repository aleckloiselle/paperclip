import { execFileSync } from "node:child_process";
import { closeSync, constants, mkdtempSync, openSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createNativeJournalProofReader } from "./native-journal-projection-async.js";

// Release even the broken blocking-open implementation before cleanup. An
// assertion failure must not leave a native syscall or test worker stranded.
function releaseFifo(path: string) {
  try {
    const writer = openSync(path, constants.O_WRONLY | constants.O_NONBLOCK);
    closeSync(writer);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENXIO") throw error;
  }
}

it.skipIf(process.platform === "win32").each(["scan", "projection"] as const)(
  "rejects a FIFO without a writer and closes the %s worker within its deadline",
  async (kind) => {
    const root = mkdtempSync(join(tmpdir(), "journal-fifo-"));
    const fifo = join(root, "state.json"), regular = join(root, "regular.json");
    const reader = createNativeJournalProofReader({ timeoutMs: 3_000, maxQueue: 1 });
    let read: Promise<unknown> | undefined;
    let readDeadline: ReturnType<typeof setTimeout> | undefined;
    let closeDeadline: ReturnType<typeof setTimeout> | undefined;
    try {
      execFileSync("mkfifo", [fifo]);
      writeFileSync(regular, "{}");
      // Start the worker before the FIFO job, so cold-loader time cannot pass
      // the test by terminating the worker before it attempts the hostile open.
      await reader.read({ kind: "scan", path: regular, maximum: 100 });
      const job = kind === "scan"
        ? { kind, path: fifo, maximum: 100 } as const
        : { kind, path: fifo, purpose: "evidence" } as const;
      read = reader.read(job).then(
        () => "unexpected_success",
        (error: Error) => error.message,
      );
      const result = await Promise.race([
        read,
        new Promise<string>(resolve => {
          readDeadline = setTimeout(() => resolve("read_still_blocked"), 3_500);
        }),
      ]);
      clearTimeout(readDeadline);
      const closed = await Promise.race([
        reader.close().then(() => true),
        new Promise<boolean>(resolve => {
          closeDeadline = setTimeout(() => resolve(false), 500);
        }),
      ]);
      clearTimeout(closeDeadline);
      expect(result).toBe(kind === "scan" ? "native_state_file_too_large" : "native_journal_too_large");
      expect(closed).toBe(true);
    } finally {
      clearTimeout(readDeadline);
      clearTimeout(closeDeadline);
      releaseFifo(fifo);
      await read;
      await reader.close();
      rmSync(root, { recursive: true, force: true });
    }
  },
  20_000,
);
