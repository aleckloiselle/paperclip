import { createHash } from "node:crypto";
import { closeSync, constants, fstatSync, openSync, readSync } from "node:fs";
import { DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES } from "../../vendor/paperclip-runner/index.js";

// These are read-only proof projections, never replacement journal contents.
// The raw writer limit still applies, including to bytes omitted from the proof.
export const NATIVE_JOURNAL_PROJECTION_BUDGET = 8 * 1024 * 1024;
const CHUNK = 48 * 1024; // divisible by three for the existing base64 fingerprint
const OMIT = Symbol("omitted");
const REF = Symbol("raw JSON span");
type Span = { [REF]: true; start: number; end: number };
type Mode = "keep" | "skip" | "span";
type Select = (path: string[]) => Mode;
const object = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const isSpan = (v: unknown): v is Span => object(v) && REF in v;
const full: Select = () => "keep";
const stateFields = new Set([
  "schema",
  "identity",
  "tickets",
  "leases",
  "commands",
  "committedEvents",
  "ackedSourceSeq",
  "connectionCount",
  "commandDeliveryCounts",
  "runAttachTemplate",
  "warmTransition",
  "completedWarmTransition",
  "replayDeliveries",
  "duplicateCommandResults",
  "freshBootstraps",
  "malformedFrames",
]);
/** Bound the retained JSON representation, not cumulative parsing work or an
 * engine-specific guess at object overhead. The worker separately bounds heap
 * and concurrency. Keys inspected
 * only to discard a field, replaced duplicate values, and deferred spans do not
 * keep consuming memory after their replacement. */
class Budget {
  used = 0;
  private sizes = new WeakMap<object, number>();
  charge(bytes: number) {
    this.used += bytes;
    if (this.used > NATIVE_JOURNAL_PROJECTION_BUDGET)
      throw new Error("native_journal_projection_budget_exceeded");
  }
  size(value: unknown): number {
    return value !== null && typeof value === "object"
      ? (this.sizes.get(value) ?? 0)
      : typeof value === "string"
        ? Buffer.byteLength(JSON.stringify(value))
        : (JSON.stringify(value)?.length ?? 4);
  }
  remember(value: object, bytes: number) {
    this.sizes.set(value, bytes);
  }
  release(value: unknown) {
    this.used -= this.size(value);
  }
}

class JsonScanner {
  private buffer = Buffer.allocUnsafe(CHUNK);
  private begin = -1;
  private length = 0;
  offset: number;
  constructor(
    private fd: number,
    private end: number,
    private budget: Budget,
    start = 0,
    private digest?: ReturnType<typeof createHash>,
  ) {
    this.offset = start;
  }
  private peek(): number {
    if (this.offset >= this.end) return -1;
    if (this.offset < this.begin || this.offset >= this.begin + this.length) {
      this.begin = this.offset;
      this.length = readSync(
        this.fd,
        this.buffer,
        0,
        Math.min(CHUNK, this.end - this.offset),
        this.offset,
      );
      if (!this.length) throw new Error("native_state_file_changed");
      this.digest?.update(this.buffer.subarray(0, this.length));
    }
    return this.buffer[this.offset - this.begin]!;
  }
  private space() {
    while ([32, 9, 10, 13].includes(this.peek())) this.offset++;
  }
  private expect(byte: number) {
    if (this.peek() !== byte) throw new Error("native_journal_invalid_json");
    this.offset++;
  }
  private token(start: number, end: number, key = false): unknown {
    const size = end - start;
    if (key && size > 4096)
      throw new Error("native_journal_projection_budget_exceeded");
    // A key is transient until its selected child is retained. Bound decoding
    // before allocating, then account its retained JSON bytes after decoding.
    const reserved = key ? 0 : size;
    this.budget.charge(reserved);
    const bytes = Buffer.allocUnsafe(size);
    let count = 0;
    while (count < size) {
      const read = readSync(this.fd, bytes, count, size - count, start + count);
      if (!read) throw new Error("native_state_file_changed");
      count += read;
    }
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    if (!key) this.budget.charge(this.budget.size(value) - reserved);
    return value;
  }
  private string(capture: boolean, key = false): unknown {
    const start = this.offset;
    this.expect(34);
    for (;;) {
      const c = this.peek();
      this.offset++;
      if (c === 34) break;
      if (c < 32) throw new Error("native_journal_invalid_json");
      if (c === 92) {
        const escape = this.peek();
        this.offset++;
        if (escape === 117) {
          for (let n = 0; n < 4; n++) {
            const hex = this.peek();
            if (!(
              (hex >= 48 && hex <= 57) ||
              (hex >= 65 && hex <= 70) ||
              (hex >= 97 && hex <= 102)
            ))
              throw new Error("native_journal_invalid_json");
            this.offset++;
          }
        } else if (![34, 92, 47, 98, 102, 110, 114, 116].includes(escape))
          throw new Error("native_journal_invalid_json");
      }
    }
    return capture ? this.token(start, this.offset, key) : OMIT;
  }
  value(path: string[], select: Select, depth = 0, forced?: Mode): unknown {
    if (depth > 128)
      throw new Error("native_journal_projection_budget_exceeded");
    this.space();
    const start = this.offset,
      mode = forced ?? select(path),
      c = this.peek();
    if (mode === "span") {
      this.value(path, select, depth, "skip");
      this.budget.charge(48);
      const span = { [REF]: true, start, end: this.offset } satisfies Span;
      this.budget.remember(span, 48);
      return span;
    }
    const keep = mode === "keep";
    if (c === 34) return this.string(keep);
    if (c === 123 || c === 91) {
      const before = this.budget.used;
      if (keep) this.budget.charge(2);
      const array = c === 91;
      this.offset++;
      this.space();
      const result: unknown[] | Record<string, unknown> = array ? [] : {};
      const close = array ? 93 : 125;
      if (this.peek() === close) {
        this.offset++;
        if (keep) this.budget.remember(result, this.budget.used - before);
        return keep ? result : OMIT;
      }
      let index = 0;
      for (;;) {
        this.space();
        let key: string;
        if (array) key = String(index++);
        else {
          const parsed = this.string(keep, true);
          key = keep ? (parsed as string) : "";
          this.space();
          this.expect(58);
        }
        const child = this.value(
          keep ? [...path, key] : path,
          select,
          depth + 1,
          keep ? undefined : "skip",
        );
        if (keep && child !== OMIT) {
          if (Object.hasOwn(result, key))
            this.budget.release((result as Record<string, unknown>)[key]);
          else this.budget.charge(array ? 1 : this.budget.size(key) + 2);
          Object.defineProperty(result, key, {
            value: child,
            configurable: true,
            enumerable: true,
            writable: true,
          });
        }
        this.space();
        if (this.peek() === close) {
          this.offset++;
          if (keep) this.budget.remember(result, this.budget.used - before);
          return keep ? result : OMIT;
        }
        this.expect(44);
      }
    }
    for (const [first, literal, value] of [
      [116, "true", true],
      [102, "false", false],
      [110, "null", null],
    ] as const) {
      if (c === first) {
        for (const ch of literal) this.expect(ch.charCodeAt(0));
        if (keep) this.budget.charge(literal.length);
        return keep ? value : OMIT;
      }
    }
    let number = "";
    while (
      this.peek() >= 0 &&
      ![32, 9, 10, 13, 44, 93, 125].includes(this.peek())
    ) {
      if (number.length >= 128) throw new Error("native_journal_invalid_json");
      number += String.fromCharCode(this.peek());
      this.offset++;
    }
    if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(number))
      throw new Error("native_journal_invalid_json");
    if (keep) this.budget.charge(this.budget.size(JSON.parse(number)));
    return keep ? JSON.parse(number) : OMIT;
  }
  parse(select: Select): unknown {
    const result = this.value([], select);
    this.space();
    if (this.offset !== this.end)
      throw new Error("native_journal_invalid_json");
    return result;
  }
}

function unchanged(
  before: ReturnType<typeof fstatSync>,
  after: ReturnType<typeof fstatSync>,
) {
  if (
    before.size !== after.size ||
    before.mtimeMs !== after.mtimeMs ||
    before.ino !== after.ino
  )
    throw new Error("native_state_file_changed");
}

export function readNativeJournalProjection(
  path: string,
  purpose: "identity" | "evidence" = "evidence",
) {
  const fd = openSync(
    path,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
  );
  try {
    const before = fstatSync(fd);
    if (
      !before.isFile() ||
      before.size > DURABLE_PRP_CONTROL_PLANE_MAX_STATE_BYTES
    )
      throw new Error("native_journal_too_large");
    const budget = new Budget(),
      digest = createHash("sha256");
    const select: Select = (parts) => {
      if (
        parts.length === 1 &&
        (purpose === "identity"
          ? !["schema", "identity"].includes(parts[0]!)
          : !stateFields.has(parts[0]!))
      )
        return "skip";
      if (
        parts[0] === "commands" &&
        parts.length === 3 &&
        ["payload", "result"].includes(parts[2]!)
      )
        return "span";
      if (
        parts[0] === "committedEvents" &&
        parts.length === 4 &&
        parts.slice(2).join("/") === "envelope/payload"
      )
        return "span";
      if (parts[0] === "committedEvents") {
        if (
          parts.length === 3 &&
          ![
            "sourceSeq",
            "sourceEventId",
            "eventType",
            "priority",
            "envelope",
            "deliveryCount",
            "logicalEffectCount",
          ].includes(parts[2]!)
        )
          return "skip";
        if (
          parts.length === 4 &&
          parts[2] === "envelope" &&
          !["runId", "normalizedSessionId", "payload"].includes(parts[3]!)
        )
          return "skip";
      }
      return "keep";
    };
    const value = new JsonScanner(fd, before.size, budget, 0, digest).parse(
      select,
    );
    const materialize = (v: unknown, selection: Select): unknown => {
      if (!isSpan(v)) return v;
      budget.release(v);
      return new JsonScanner(fd, v.end, budget, v.start).parse(selection);
    };
    // A projection preserves original primitive/array/object types. It must not
    // turn malformed authority into a valid empty object.
    const fields =
      (names: string[]): Select =>
      (parts) =>
        parts.length === 1 && !names.includes(parts[0]!) ? "skip" : "keep";
    const inspect = (v: unknown, selection: Select): unknown =>
      isSpan(v)
        ? new JsonScanner(fd, v.end, new Budget(), v.start).parse(selection)
        : v;
    const finishOperation = (v: unknown) =>
      object(v) && v.operationId === "paperclip_finish";
    const semanticOperation: Select = (parts) =>
      (parts.length === 1 && parts[0] !== "semantic_tool") ||
      (parts.length === 2 && parts[1] !== "operationId")
        ? "skip"
        : "keep";
    // These fields participate in validatePrpEvent's semantic envelope schema.
    // In particular optional receipt/reference fields must remain available to
    // validation; deleting invalid optional fields could invent valid proof.
    const semanticFields = new Set([
      "schema",
      "schemaVersion",
      "phase",
      "operationId",
      "callId",
      "correlation",
      "idempotencyKey",
      "content",
      "outcome",
      "code",
      "retryable",
      "authorizationBoundary",
      "operationReceiptId",
      "auditReceiptId",
      "currentRevision",
      "duplicateOfReceiptId",
      "artifactRefs",
      "targets",
      "causalRefs",
    ]);
    const eventCommon = [
      "processId",
      "channel",
      "providerPhase",
      "providerMethod",
    ];
    const proofEvents = new Set([
      "session.started",
      "session.resumed",
      "session.reconciled",
      "turn.accepted",
      "semantic_tool.input",
      "semantic_tool.result",
    ]);
    const eventFields = new Set([
      "schema",
      "sourceEventId",
      "sourceSeq",
      "sourceInstanceId",
      "sourceKind",
      "runId",
      "normalizedSessionId",
      "turnId",
      "itemId",
      "eventType",
      "schemaVersion",
      "priority",
      "emittedAt",
      "observedAt",
      "payload",
      "debug",
    ]);
    if (object(value)) {
      if (Array.isArray(value.commands))
        for (const command of value.commands) {
          if (!object(command)) continue;
          const complete = command.type === "run.attach";
          const finish =
            command.type === "semantic_tool.result" &&
            finishOperation(inspect(command.payload, fields(["operationId"])));
          if ("payload" in command)
            command.payload = materialize(
              command.payload,
              complete
                ? full
                : finish
                  ? (parts) =>
                      (parts.length === 1 &&
                        ![
                          "callId",
                          "operationId",
                          "isError",
                          "sourceEventId",
                          "sourceEventType",
                          "input",
                          "correlation",
                          "result",
                        ].includes(parts[0]!)) ||
                      (parts.length === 2 &&
                        parts[0] === "result" &&
                        parts[1] !== "success")
                        ? "skip"
                        : "keep"
                  : fields(
                      command.type === "run.prepare"
                        ? ["completionContract"]
                        : command.type === "semantic_tool.result"
                          ? ["operationId", "callId"]
                          : [],
                    ),
            );
          if ("result" in command)
            command.result = materialize(
              command.result,
              complete
                ? full
                : finish
                  ? (parts) =>
                      (parts.length === 1 &&
                        !["status", "result"].includes(parts[0]!)) ||
                      (parts.length === 2 &&
                        parts[0] === "result" &&
                        parts[1] !== "callId")
                        ? "skip"
                        : "keep"
                  : command.type === "turn.start"
                    ? (parts) =>
                        (parts.length === 1 && parts[0] !== "result") ||
                        (parts.length === 2 && parts[1] !== "providerTurnId")
                          ? "skip"
                          : "keep"
                    : fields([]),
            );
        }
      if (Array.isArray(value.committedEvents))
        for (const entry of value.committedEvents) {
          if (
            !object(entry) ||
            !object(entry.envelope) ||
            !("payload" in entry.envelope)
          )
            continue;
          const rawEvent = entry.envelope.payload;
          const event = inspect(rawEvent, (parts) =>
            parts[0] === "payload"
              ? semanticOperation(parts.slice(1))
              : fields(["eventType", "payload"])(parts),
          );
          if (object(event)) {
            let selection: Select;
            if (
              ["semantic_tool.input", "semantic_tool.result"].includes(
                String(event.eventType),
              )
            ) {
              const finish =
                object(event.payload) &&
                finishOperation(event.payload.semantic_tool);
              selection = (parts) =>
                (parts.length === 1 &&
                  ![...eventCommon, "semantic_tool"].includes(parts[0]!)) ||
                (parts.length === 2 &&
                  parts[0] === "semantic_tool" &&
                  !(finish
                    ? semanticFields.has(parts[1]!) ||
                      (parts[1] === "input" &&
                        event.eventType === "semantic_tool.input")
                    : parts[1] === "operationId"))
                  ? "skip"
                  : "keep";
            } else if (
              [
                "session.started",
                "session.resumed",
                "session.reconciled",
              ].includes(String(event.eventType))
            ) {
              // Every recorded process owner remains a stop requirement.
              selection = fields([
                ...eventCommon,
                "providerSessionId",
                "providerAccountSessionId",
                "driverSessionId",
                "previousProcessId",
                "process_id",
                "processGroupId",
                "providerPid",
                "codexPid",
                "sidecarPid",
                "agentPid",
                "agentProcessId",
                "providerDescriptor",
                "providerIdentity",
                "runtimeIdentity",
              ]);
            } else if (event.eventType === "turn.accepted") {
              selection = fields([
                ...eventCommon,
                "providerTurnId",
                "providerSessionId",
              ]);
            } else if (event.eventType === "harness.diagnostic") {
              selection = fields([...eventCommon, "pid"]);
            } else {
              // MCP/runtime inputs remain as events: their presence alone denies
              // effect-free replay. Generic tool output is not cleanup authority.
              selection = fields(["processId"]);
            }
            entry.envelope.payload = materialize(rawEvent, (parts) => {
              if (parts[0] === "payload") return selection(parts.slice(1));
              // Only authority-bearing events are passed to validatePrpEvent.
              // Generic event identities/text are not authority; retain their
              // durable headers, raw type and process owner as negative evidence.
              if (
                parts.length === 1 &&
                !(proofEvents.has(String(event.eventType))
                  ? eventFields.has(parts[0]!)
                  : parts[0] === "eventType")
              )
                return "skip";
              // debug is schema-checked only as an object, never proof content.
              if (parts[0] === "debug" && parts.length > 1) return "skip";
              return "keep";
            });
          } else entry.envelope.payload = materialize(rawEvent, fields([]));
        }
    }
    unchanged(before, fstatSync(fd));
    return {
      value,
      sha256: digest.digest("hex"),
      byteSize: before.size,
      retainedBudgetBytes: budget.used,
    };
  } finally {
    closeSync(fd);
  }
}

/** Hash the complete source, including discarded history, without retaining it. */
export function scanNativeStateFile(
  path: string,
  maximum: number,
  consume?: (chunk: Buffer) => void,
) {
  const fd = openSync(
    path,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
  );
  try {
    const before = fstatSync(fd);
    if (!before.isFile() || before.size > maximum)
      throw new Error("native_state_file_too_large");
    const buffer = Buffer.allocUnsafe(CHUNK),
      digest = createHash("sha256");
    let offset = 0;
    while (offset < before.size) {
      const length = Math.min(CHUNK, before.size - offset);
      let count = 0;
      while (count < length) {
        const read = readSync(
          fd,
          buffer,
          count,
          length - count,
          offset + count,
        );
        if (!read) throw new Error("native_state_file_changed");
        count += read;
      }
      const chunk = buffer.subarray(0, count);
      digest.update(chunk);
      consume?.(chunk);
      offset += count;
    }
    unchanged(before, fstatSync(fd));
    return { sha256: digest.digest("hex"), byteSize: before.size };
  } finally {
    closeSync(fd);
  }
}
