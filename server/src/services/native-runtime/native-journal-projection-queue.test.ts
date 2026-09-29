import { afterEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ active: 0, peak: 0, terminated: 0 }));
vi.mock("node:perf_hooks", () => ({ performance: { now: () => Date.now() } }));
vi.mock("node:worker_threads", async () => {
  const { EventEmitter } = await import("node:events");
  return {
    Worker: class extends EventEmitter {
      timer?: ReturnType<typeof setTimeout>;
      constructor() {
        super();
        state.peak = Math.max(state.peak, ++state.active);
      }
      postMessage(job: { path: string }) {
        this.timer = setTimeout(() => this.emit("message", {
          result: { sha256: job.path, byteSize: 190 * 1024 * 1024 },
        }), job.path === "slow" ? 110 : 60);
      }
      ref() {}
      unref() {}
      async terminate() {
        clearTimeout(this.timer);
        state.active--;
        state.terminated++;
        return 0;
      }
    },
  };
});
import { createNativeJournalProofReader } from "./native-journal-projection-async.js";

afterEach(() => vi.useRealTimers());
describe("journal proof queue admission", () => {
  it("rejects stale queued work as busy without starting it", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const reader = createNativeJournalProofReader({ timeoutMs: 100, queueTimeoutMs: 50 });
    const job = { kind: "scan" as const, path: "valid", maximum: 192 * 1024 * 1024 };
    const results = Promise.allSettled([reader.read(job), reader.read(job)]);
    try {
      await vi.advanceTimersByTimeAsync(60);
      expect(await results).toMatchObject([
        { status: "fulfilled", value: { sha256: "valid" } },
        { status: "rejected", reason: { message: "native_journal_worker_busy" } },
      ]);
    } finally {
      await reader.close();
    }
  });
  it("gives each admitted proof a full execution budget after waiting", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const reader = createNativeJournalProofReader({ timeoutMs: 100 });
    const job = { kind: "scan" as const, path: "valid", maximum: 192 * 1024 * 1024 };
    // Each proof fits its budget; their aggregate duration exceeds one budget.
    const results = Promise.allSettled([reader.read(job), reader.read(job)]);
    try {
      await vi.advanceTimersByTimeAsync(120);
      expect(await results).toMatchObject([
        { status: "fulfilled", value: { sha256: "valid" } },
        { status: "fulfilled", value: { sha256: "valid" } },
      ]);
    } finally {
      await reader.close();
    }
  });
  it("terminates an actual overrun before giving the next proof its own budget", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    state.peak = state.active = state.terminated = 0;
    const reader = createNativeJournalProofReader({ timeoutMs: 100 });
    const job = { kind: "scan" as const, maximum: 192 * 1024 * 1024 };
    const results = Promise.allSettled([
      reader.read({ ...job, path: "slow" }),
      reader.read({ ...job, path: "valid" }),
    ]);
    try {
      await vi.advanceTimersByTimeAsync(160);
      expect(await results).toMatchObject([
        { status: "rejected", reason: { message: "native_journal_worker_timeout" } },
        { status: "fulfilled", value: { sha256: "valid" } },
      ]);
      expect(state.peak).toBe(1);
      expect(state.terminated).toBe(1);
    } finally {
      await reader.close();
    }
  });
});
