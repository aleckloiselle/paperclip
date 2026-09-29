import { createHash } from "node:crypto";
import {
  closeSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  rmSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it } from "vitest";
import {
  createNativeJournalProofReader,
  nativeStateBase64Fingerprint,
  nativeStateFileIncludes,
  readNativeJournalProjection,
  scanNativeStateFile,
} from "./native-journal-projection-async.js";
import { runnerdStateProvesIncompleteBootstrap } from "./native-session-executor.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});
function fixture(historyMiB = 0) {
  const root = mkdtempSync(join(tmpdir(), "journal-responsive-"));
  roots.push(root);
  mkdirSync(join(root, "control-plane"));
  const path = join(root, "control-plane/control-plane-state.json");
  const fd = openSync(path, "w");
  writeSync(
    fd,
    '{"schema":"paperclip.runner.durable.control-plane-state.v1","identity":{"runId":"bounded"},"connectionCount":0,"commands":[],"committedEvents":[',
  );
  // A near-limit journal consists of individually bounded event frames, not
  // one oversized output that the writer could never accept.
  const chunk = Buffer.alloc(512 * 1024, 120);
  for (let i = 0; i < Math.max(1, historyMiB * 2); i++) {
    if (i) writeSync(fd, ",");
    writeSync(
      fd,
      '{"envelope":{"payload":{"eventType":"item.completed","payload":{"text":"',
    );
    if (historyMiB) writeSync(fd, chunk);
    writeSync(fd, '"}}}}');
  }
  writeSync(fd, "]}");
  closeSync(fd);
  return { root, path };
}
describe("off-thread durable journal proofs", () => {
  it("keeps the event loop responsive during near-limit controller admission", async () => {
    const { root, path } = fixture(190);
    let ticks = 0,
      largestGap = 0,
      last = performance.now();
    const pulse = setInterval(() => {
      const now = performance.now();
      largestGap = Math.max(largestGap, now - last);
      last = now;
      ticks++;
    }, 5);
    const started = performance.now();
    try {
      const [proof, bootstrap] = await Promise.all([
        readNativeJournalProjection(path),
        runnerdStateProvesIncompleteBootstrap(root),
      ]);
      expect(proof.byteSize).toBeGreaterThan(190 * 1024 * 1024);
      expect(proof.value).toMatchObject({ identity: { runId: "bounded" } });
      expect(bootstrap).toBe(false);
      expect(ticks).toBeGreaterThan(0);
      expect(largestGap).toBeLessThan(500);
      expect(performance.now() - started).toBeLessThan(30_000);
    } finally {
      clearInterval(pulse);
    }
  }, 40_000);
  it("preserves complete raw hashes and the existing base64 fingerprint", async () => {
    const { root, path } = fixture(3);
    const second = join(root, "other.json"),
      bytes = Buffer.from('{"value":"second"}');
    writeFileSync(second, bytes);
    const projection = await readNativeJournalProjection(path);
    expect(projection.value).toMatchObject({ identity: { runId: "bounded" } });
    const file = await scanNativeStateFile(path, 4 * 1024 * 1024);
    expect(file.sha256).toBe(projection.sha256);
    expect(file.byteSize).toBe(projection.byteSize);
    const fingerprint = await nativeStateBase64Fingerprint([
      { path: second, maximum: 100 },
      { path: second, maximum: 100 },
    ]);
    expect(fingerprint.sha256).toBe(
      createHash("sha256")
        .update(
          JSON.stringify([bytes.toString("base64"), bytes.toString("base64")]),
        )
        .digest("hex"),
    );
    expect(fingerprint.fileSha256).toEqual(
      Array(2).fill(createHash("sha256").update(bytes).digest("hex")),
    );
    expect(
      await nativeStateFileIncludes(path, 4 * 1024 * 1024, [
        '"runId":"bounded"',
      ]),
    ).toBe(true);
    expect(
      await nativeStateFileIncludes(path, 4 * 1024 * 1024, ["absent-marker"]),
    ).toBe(false);
  });
  it("bounds queued work and terminates timed-out workers before closing", async () => {
    const { path } = fixture(190);
    const reader = createNativeJournalProofReader({
      timeoutMs: 20,
      maxQueue: 1,
    });
    const job = {
      kind: "projection" as const,
      path,
      purpose: "evidence" as const,
    };
    const results = Promise.allSettled([
      reader.read(job),
      reader.read(job),
      reader.read(job),
    ]);
    try {
      expect(await results).toMatchObject([
        {
          status: "rejected",
          reason: { message: "native_journal_worker_timeout" },
        },
        {
          status: "rejected",
          reason: { message: "native_journal_worker_timeout" },
        },
        {
          status: "rejected",
          reason: { message: "native_journal_worker_busy" },
        },
      ]);
    } finally {
      await reader.close();
    }
    await expect(reader.read(job)).rejects.toThrow(
      "native_journal_worker_busy",
    );
    // An independent ordinary proof still succeeds after worker termination.
    const ordinary = fixture();
    expect(
      (await readNativeJournalProjection(ordinary.path)).value,
    ).toMatchObject({ identity: { runId: "bounded" } });
  });
});
