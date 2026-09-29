import { performance } from "node:perf_hooks";
import { Worker } from "node:worker_threads";

export type RunnerdStateFile = { path: string; maximum: number };
export type RunnerdStateRead = { state: Record<string, unknown>; sha256: string };
type Pending = {
  files: RunnerdStateFile[];
  deadline: number;
  resolve: (result: RunnerdStateRead[]) => void;
  reject: (error: Error) => void;
};

/** Transport needs complete command receipts, so these reads cannot use the
 * server's smaller evidence projection. Bound full parsing separately and keep
 * file I/O, JSON decoding and hashing off the control-plane event loop. */
export function createRunnerdStateReader(
  options: { timeoutMs?: number; queueTimeoutMs?: number; maxQueue?: number } = {},
) {
  const queue: Pending[] = [];
  let active: Pending | undefined;
  let closed = false;
  let worker: Worker | undefined;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopping: Promise<unknown> | undefined;

  async function stop() {
    clearTimeout(idleTimer);
    const previous = worker;
    worker = undefined;
    if (previous) {
      stopping = previous.terminate();
      await stopping;
      stopping = undefined;
    }
  }
  async function finish(error?: Error, result?: RunnerdStateRead[]) {
    const pending = active;
    active = undefined;
    clearTimeout(timer);
    if (error) {
      // Do not admit another reader until the previous thread is gone.
      await stop();
      pending?.reject(error);
    } else if (pending) {
      pending.resolve(result!);
      worker?.unref();
      idleTimer = setTimeout(() => {
        if (!active) void stop().then(drain);
      }, 15_000);
      idleTimer.unref();
    }
    drain();
  }
  function spawn() {
    const source = new URL(import.meta.url).pathname.endsWith(".ts");
    const url = new URL(
      source ? "./runnerd-state-reader-worker.ts" : "./runnerd-state-reader-worker.js",
      import.meta.url,
    );
    const resourceLimits = {
      maxOldGenerationSizeMb: 512,
      maxYoungGenerationSizeMb: 32,
      stackSizeMb: 4,
    };
    const next = source
      ? new Worker(
          `import(${JSON.stringify(import.meta.resolve("tsx/esm/api"))}).then(({tsImport}) => tsImport(${JSON.stringify(url.href)}, ${JSON.stringify(import.meta.url)}));`,
          { eval: true, resourceLimits },
        )
      : new Worker(url, { resourceLimits });
    next.on("message", (message: { result?: RunnerdStateRead[]; error?: string }) => {
      if (worker !== next || !active) return;
      void finish(message.error ? new Error(message.error) : undefined, message.result);
    });
    next.on("error", () => {
      if (worker === next) void finish(new Error("native_runner_state_worker_failed"));
    });
    next.on("exit", () => {
      if (worker === next) void finish(new Error("native_runner_state_worker_exited"));
    });
    return next;
  }
  function drain() {
    if (closed || active || stopping) return;
    while (queue.length) {
      const pending = queue.shift()!;
      if (pending.deadline <= performance.now()) {
        pending.reject(new Error("native_runner_state_worker_busy"));
        continue;
      }
      clearTimeout(idleTimer);
      active = pending;
      try {
        worker ??= spawn();
        worker.ref();
        timer = setTimeout(() => {
          void finish(new Error("native_runner_state_worker_timeout"));
        }, options.timeoutMs ?? 30_000);
        worker.postMessage(pending.files);
      } catch {
        void finish(new Error("native_runner_state_worker_failed"));
      }
      return;
    }
  }
  return {
    read(files: RunnerdStateFile[]): Promise<RunnerdStateRead[]> {
      if (closed || queue.length >= (options.maxQueue ?? 8))
        return Promise.reject(new Error("native_runner_state_worker_busy"));
      return new Promise((resolve, reject) => {
        queue.push({ files, resolve, reject, deadline: performance.now() + (options.queueTimeoutMs ?? 30_000) });
        drain();
      });
    },
    async close() {
      closed = true;
      for (const pending of queue.splice(0))
        pending.reject(new Error("native_runner_state_worker_closed"));
      if (active) await finish(new Error("native_runner_state_worker_closed"));
      await stop();
      await stopping;
    },
  };
}

const reader = createRunnerdStateReader();
export function readRunnerdStateFiles(files: RunnerdStateFile[]) {
  return reader.read(files);
}
