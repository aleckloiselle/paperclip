import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES } from "../control-plane/durable-prp-control-plane.js";
import { createRunnerdStateReader } from "./runnerd-state-reader.js";

describe("runnerd state reader", () => {
  const roots: string[] = [];
  const readers: ReturnType<typeof createRunnerdStateReader>[] = [];
  async function setup(options?: Parameters<typeof createRunnerdStateReader>[0]) {
    const root = await mkdtemp(join(tmpdir(), "runnerd-state-reader-"));
    roots.push(root);
    const reader = createRunnerdStateReader(options);
    readers.push(reader);
    return { root, reader, path: join(root, "state.json") };
  }
  afterEach(async () => {
    await Promise.all(readers.splice(0).map((reader) => reader.close()));
    await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
  });

  it("reads complete command receipts above the old 64 MiB cap without synchronous parsing", async () => {
    const { path, reader } = await setup();
    const content = JSON.stringify({ identity: { runId: "same-run" }, commands: [{
      type: "turn.start", payload: { text: "x".repeat(65 * 1024 * 1024) },
      status: "completed", result: { result: { providerExitConfirmed: true } },
    }] });
    await writeFile(path, content);
    const expectedHash = createHash("sha256").update(content).digest("hex");
    let ticks = 0;
    const timer = setInterval(() => { ticks += 1; }, 1);
    try {
      const [result] = await reader.read([{ path, maximum: DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES }]);
      expect(result!.sha256).toBe(expectedHash);
      expect(result!.state.identity).toEqual({ runId: "same-run" });
      const command = (result!.state.commands as Array<{ payload: { text: string }; result: unknown }>)[0]!;
      expect(command.payload.text.length).toBe(65 * 1024 * 1024);
      expect(command.result).toEqual({ result: { providerExitConfirmed: true } });
      expect(ticks).toBeGreaterThan(0);
    } finally { clearInterval(timer); }
  }, 30_000);

  it("hashes exact raw bytes for each maintenance file", async () => {
    const { root, reader } = await setup();
    const contents = ["{\"commands\":[]}\n", "  {\"lifecycle\":\"suspended\"}", "{\"threadId\":\"provider-session\"}"];
    const files = await Promise.all(contents.map(async (text, index) => {
      const path = join(root, `${index}.json`);
      await writeFile(path, text);
      return { path, maximum: 1024 };
    }));
    const result = await reader.read(files);
    expect(result.map((entry) => entry.sha256)).toEqual(contents.map((text) => createHash("sha256").update(text).digest("hex")));
    expect(result.map((entry) => entry.state)).toEqual(contents.map((text) => JSON.parse(text)));
  });

  it("rejects oversized, malformed, symlink and directory state then accepts a valid retry", async () => {
    const { path, root, reader } = await setup();
    await writeFile(path, "{\"ok\":true}");
    await expect(reader.read([{ path, maximum: 2 }])).rejects.toThrow("native_runner_state_file_unsafe");
    const link = join(root, "link.json");
    await symlink(path, link);
    await expect(reader.read([{ path: link, maximum: 1024 }])).rejects.toThrow("native_runner_state_read_failed");
    await expect(reader.read([{ path: root, maximum: 1024 }])).rejects.toThrow();
    await writeFile(path, "{\"ok\":true}garbage");
    await expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_read_failed");
    await writeFile(path, "{\"ok\":true}");
    expect((await reader.read([{ path, maximum: 1024 }]))[0]!.state).toEqual({ ok: true });
  }, 15_000);

  it.skipIf(process.platform === "win32")("rejects a FIFO without waiting for a writer", async () => {
    const { path, reader } = await setup();
    execFileSync("mkfifo", [path]);
    await expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_file_unsafe");
  });

  it("bounds queued work and terminates an expired reader before settling", async () => {
    const { path, reader } = await setup({ timeoutMs: 1, queueTimeoutMs: 1, maxQueue: 1 });
    await writeFile(path, "{}");
    const first = expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_worker_timeout");
    const queued = expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_worker_busy");
    await expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_worker_busy");
    await Promise.all([first, queued]);
    await reader.close();
    await expect(reader.read([{ path, maximum: 1024 }])).rejects.toThrow("native_runner_state_worker_busy");
  });
});
