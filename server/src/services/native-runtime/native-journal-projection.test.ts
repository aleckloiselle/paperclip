import { createHash } from "node:crypto";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  readNativeJournalProjection,
  scanNativeStateFile,
  NATIVE_JOURNAL_PROJECTION_BUDGET,
} from "./native-journal-projection.js";
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});
function fixture(raw: string) {
  const root = mkdtempSync(join(tmpdir(), "journal-projection-"));
  roots.push(root);
  mkdirSync(join(root, "control-plane"));
  const path = join(root, "control-plane/control-plane-state.json");
  writeFileSync(path, raw);
  return { root, path };
}
const event = (type: string, payload: unknown) => ({
  eventType: type,
  sourceSeq: 1,
  sourceEventId: "event-1",
  priority: 1,
  deliveryCount: 1,
  logicalEffectCount: 1,
  envelope: {
    runId: "run-1",
    normalizedSessionId: "session-1",
    payload: { eventType: type, runId: "run-1", payload },
  },
});
describe("bounded native journal proof projection", () => {
  it("does not materialize large tool history in the proof scanner", () => {
    const { path } = fixture(
      JSON.stringify({
        schema: "paperclip.runner.durable.control-plane-state.v1",
        identity: { runId: "run-1" },
        connectionCount: 0,
        commands: [],
        committedEvents: [
          event("item.completed", { text: "x".repeat(24 * 1024 * 1024) }),
        ],
      }),
    );
    const parse = JSON.parse;
    const sizes: number[] = [];
    vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
      sizes.push(text.length);
      return parse(text, reviver);
    });
    expect(readNativeJournalProjection(path).retainedBudgetBytes).toBeLessThan(
      NATIVE_JOURNAL_PROJECTION_BUDGET,
    );
    expect(Math.max(...sizes)).toBeLessThan(NATIVE_JOURNAL_PROJECTION_BUDGET);
  });
  it("hashes omitted bytes while retaining semantic and process proof", () => {
    const semantic = {
      semantic_tool: {
        operationId: "paperclip_finish",
        input: { summary: "exact" },
      },
    };
    const state = {
      schema: "schema",
      identity: { runId: "exact" },
      commands: [
        {
          type: "run.attach",
          payload: { nested: { prompt: "must retain" } },
          result: { status: "completed" },
        },
        {
          type: "turn.start",
          payload: { prompt: "discard" },
          result: { result: { providerTurnId: "turn" } },
        },
      ],
      committedEvents: [
        event("item.completed", {
          text: "x".repeat(3 * 1024 * 1024),
          processId: 42,
        }),
        event("semantic_tool.input", semantic),
      ],
    };
    const raw = JSON.stringify(state),
      { path } = fixture(raw),
      proof = readNativeJournalProjection(path);
    expect(proof.sha256).toBe(createHash("sha256").update(raw).digest("hex"));
    expect(proof.retainedBudgetBytes).toBeLessThan(32 * 1024);
    expect(proof.value).toMatchObject({
      commands: [
        {
          payload: state.commands[0]!.payload,
          result: state.commands[0]!.result,
        },
        { payload: {}, result: { result: { providerTurnId: "turn" } } },
      ],
      committedEvents: [
        { envelope: { payload: { payload: { processId: 42 } } } },
        { envelope: { payload: { payload: semantic } } },
      ],
    });
    expect(JSON.stringify(proof.value)).not.toContain('"text"');
  });
  it("preserves duplicate-key last-wins semantics and original malformed types", () => {
    const { path } = fixture(
      '{"schema":"old","schema":"new","identity":{"runId":"wrong","runId":"right","__proto__":{"polluted":true}},"commands":[{"type":"run.attach","payload":null,"result":[]},{"type":"turn.start","payload":"bad","result":false}],"committedEvents":[{"envelope":null}]}',
    );
    const proof = readNativeJournalProjection(path).value;
    expect(proof).toMatchObject({
      schema: "new",
      identity: { runId: "right" },
      commands: [
        { payload: null, result: [] },
        { payload: "bad", result: false },
      ],
      committedEvents: [{ envelope: null }],
    });
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });
  it.each([
    '{"identity":{},"ignored":"bad\\q"}',
    '{"identity":{},"ignored":[1,]}',
    '{"identity":{},"ignored":01}',
    '{"identity":{},"ignored":"unterminated}',
    '{"identity":{}} trailing',
    '{"identity":{},"ignored":{"x":1,}}',
  ])("rejects malformed JSON even in discarded bytes: %s", (raw) => {
    const { path } = fixture(raw);
    expect(() => readNativeJournalProjection(path, "identity")).toThrow();
  });
  it("rejects excessive essential evidence before parsing a large token", () => {
    const { path } = fixture(
      JSON.stringify({
        schema: "s",
        identity: { runId: "x".repeat(8 * 1024 * 1024) },
      }),
    );
    expect(() => readNativeJournalProjection(path)).toThrow(
      "native_journal_projection_budget_exceeded",
    );
  });
  it("bounds complete semantic evidence without silently dropping it", () => {
    const { path } = fixture(
      JSON.stringify({
        schema: "s",
        commands: [
          {
            type: "semantic_tool.result",
            payload: {
              operationId: "paperclip_finish",
              input: "x".repeat(8 * 1024 * 1024),
            },
            result: null,
          },
        ],
      }),
    );
    expect(() => readNativeJournalProjection(path)).toThrow(
      "native_journal_projection_budget_exceeded",
    );
  });
  it("admits many individually bounded semantic results without retaining their ordinary output", () => {
    const commands = Array.from({ length: 24 }, (_, index) => ({
      commandId: `command-${index}`,
      controllerSeq: index + 1,
      type: "semantic_tool.result",
      status: "completed",
      payload: {
        operationId: "call_api",
        callId: `call-${index}`,
        input: { path: "/api/issues" },
        result: { text: "x".repeat(500 * 1024) },
      },
      result: { status: "completed", result: { callId: `call-${index}` } },
    }));
    expect(
      commands.every(
        (command) =>
          Buffer.byteLength(JSON.stringify(command)) < 1024 * 1024 - 4096,
      ),
    ).toBe(true);
    const raw = JSON.stringify({
      schema: "schema",
      identity: { runId: "run-1" },
      commands,
      committedEvents: [
        event("mcp_app.tool_input", { input: "x".repeat(500 * 1024) }),
        event("semantic_tool.input", {
          semantic_tool: {
            operationId: "call_api",
            input: "x".repeat(500 * 1024),
          },
        }),
        event("harness.diagnostic", {
          providerMethod: "acpx/process",
          pid: 42,
          output: "x".repeat(500 * 1024),
        }),
      ],
    });
    expect(Buffer.byteLength(raw)).toBeGreaterThan(
      NATIVE_JOURNAL_PROJECTION_BUDGET,
    );
    const { path } = fixture(raw),
      proof = readNativeJournalProjection(path);
    expect(proof.sha256).toBe(createHash("sha256").update(raw).digest("hex"));
    expect(proof.retainedBudgetBytes).toBeLessThan(64 * 1024);
    expect(proof.value).toMatchObject({
      commands: commands.map((command) => ({
        commandId: command.commandId,
        status: "completed",
      })),
      committedEvents: [
        { eventType: "mcp_app.tool_input" },
        {
          envelope: {
            payload: {
              payload: { semantic_tool: { operationId: "call_api" } },
            },
          },
        },
        {
          envelope: {
            payload: { payload: { providerMethod: "acpx/process", pid: 42 } },
          },
        },
      ],
    });
    expect(JSON.stringify(proof.value)).not.toContain('"output"');
    expect(JSON.stringify(proof.value)).not.toContain('"text"');
  });
  it("charges retained keys once, releasing discarded and superseded fields", () => {
    const discarded = Array.from(
      { length: 15000 },
      (_, index) => `"discard-${index}":0`,
    ).join(",");
    const duplicate = Array.from(
      { length: 10000 },
      () => '"runId":"same"',
    ).join(",");
    const { path } = fixture(`{"identity":{${duplicate}},${discarded}}`);
    const simple = fixture('{"identity":{"runId":"same"}}');
    const proof = readNativeJournalProjection(path);
    expect(proof.value).toEqual({ identity: { runId: "same" } });
    expect(proof.retainedBudgetBytes).toBe(
      readNativeJournalProjection(simple.path).retainedBudgetBytes,
    );
  });
  it("keeps the full 4096-event writer window with maximum-length durable identity headers", () => {
    const committedEvents = Array.from({ length: 4096 }, (_, index) => ({
      ...event("item.completed", { text: "ordinary output", processId: 42 }),
      sourceSeq: index + 1,
      sourceEventId: `${index}`.padEnd(160, "x"),
      envelope: {
        runId: "r".repeat(160),
        normalizedSessionId: "s".repeat(160),
        payload: {
          schema: "paperclip.prp.event.v1",
          sourceSeq: index + 1,
          sourceEventId: `${index}`.padEnd(160, "x"),
          sourceInstanceId: "i".repeat(160),
          runId: "r".repeat(160),
          normalizedSessionId: "s".repeat(160),
          turnId: "t".repeat(240),
          itemId: "i".repeat(240),
          eventType: "item.completed",
          payload: { text: "ordinary output", processId: 42 },
        },
      },
    }));
    const raw = JSON.stringify({ committedEvents }),
      { path } = fixture(raw);
    const proof = readNativeJournalProjection(path);
    expect(
      (proof.value as { committedEvents: unknown[] }).committedEvents,
    ).toHaveLength(4096);
    expect(proof.retainedBudgetBytes).toBeLessThan(
      NATIVE_JOURNAL_PROJECTION_BUDGET,
    );
    expect(proof.sha256).toBe(createHash("sha256").update(raw).digest("hex"));
  });
  it("retains finish validation fields and every provider process owner", () => {
    const semantic = {
      operationId: "paperclip_finish",
      input: { summary: "exact" },
      correlation: { runId: "run-1", extraBinding: "retain" },
      content: { digest: "retain" },
      artifactRefs: "malformed",
      retryable: "malformed",
      unrelatedOutput: "discard",
    };
    const { path } = fixture(
      JSON.stringify({
        commands: [
          {
            type: "semantic_tool.result",
            payload: {
              ...semantic,
              isError: false,
              result: { success: true, output: "discard" },
              sourceEventId: "event-1",
            },
            result: {
              status: "completed",
              result: { callId: "call-1", output: "discard" },
            },
          },
        ],
        committedEvents: [
          event("semantic_tool.input", { semantic_tool: semantic }),
          event("session.reconciled", {
            processId: 11,
            providerAccountSessionId: "account-session",
            previousProcessId: 10,
            providerDescriptor: { agentPid: 12 },
            providerIdentity: { processGroupId: 13 },
            runtimeIdentity: { sidecarPid: 14 },
            output: "discard",
          }),
        ],
      }),
    );
    const proof = readNativeJournalProjection(path);
    expect(proof.value).toMatchObject({
      commands: [
        {
          payload: {
            input: semantic.input,
            correlation: semantic.correlation,
            isError: false,
            result: { success: true },
          },
          result: { result: { callId: "call-1" } },
        },
      ],
      committedEvents: [
        {
          envelope: {
            payload: {
              payload: {
                semantic_tool: {
                  input: semantic.input,
                  artifactRefs: "malformed",
                  retryable: "malformed",
                  correlation: semantic.correlation,
                },
              },
            },
          },
        },
        {
          envelope: {
            payload: {
              payload: {
                processId: 11,
                providerAccountSessionId: "account-session",
                previousProcessId: 10,
                providerDescriptor: { agentPid: 12 },
                providerIdentity: { processGroupId: 13 },
                runtimeIdentity: { sidecarPid: 14 },
              },
            },
          },
        },
      ],
    });
    expect(JSON.stringify(proof.value)).not.toContain("discard");
  });
  it("keeps the turn identity without materializing unrelated turn result text", () => {
    const { path } = fixture(
      JSON.stringify({
        commands: [
          {
            type: "turn.start",
            payload: {},
            result: {
              result: {
                providerTurnId: "turn-1",
                output: "x".repeat(12 * 1024 * 1024),
              },
            },
          },
        ],
      }),
    );
    const proof = readNativeJournalProjection(path);
    expect(proof.value).toMatchObject({
      commands: [{ result: { result: { providerTurnId: "turn-1" } } }],
    });
    expect(proof.retainedBudgetBytes).toBeLessThan(4096);
  });
  it("rejects excessive nesting even in discarded history", () => {
    const { path } = fixture(
      '{"ignored":' + "[".repeat(130) + "0" + "]".repeat(130) + "}",
    );
    expect(() => readNativeJournalProjection(path, "identity")).toThrow(
      "native_journal_projection_budget_exceeded",
    );
  });
  it("rejects symlinks and detects file changes during incremental scans", () => {
    const { path, root } = fixture('{"identity":{}}');
    const link = join(root, "link");
    symlinkSync(path, link);
    expect(() => readNativeJournalProjection(link)).toThrow();
    expect(() =>
      scanNativeStateFile(path, 1024, () =>
        writeFileSync(path, "changed and larger"),
      ),
    ).toThrow("native_state_file_changed");
  });
  it("streams byte-identical base64 chunks, including arbitrary tails", () => {
    for (const size of [0, 1, 2, 3, 49151, 49152, 49153, 100000]) {
      const { path } = fixture("x".repeat(size));
      let encoded = "";
      const proof = scanNativeStateFile(
        path,
        200000,
        (chunk) => (encoded += chunk.toString("base64")),
      );
      expect(encoded).toBe(readFileSync(path).toString("base64"));
      expect(proof.byteSize).toBe(size);
    }
  });
});
