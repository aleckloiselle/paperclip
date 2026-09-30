import { and, asc, desc, eq, exists, gte, inArray, isNull, like, notInArray, or, sql } from "drizzle-orm";
import { agents, agentWakeupRequests, heartbeatRuns, issues, type Db } from "@paperclipai/db";
import type { PubsubInboundMessage } from "./pubsub.js";
import { issueService } from "./issues.js";
import { PUBSUB_WAKE_COOLDOWN_MS, PUBSUB_WAKE_STALE_MS } from "@paperclipai/shared";
/** The company's standing issue anchors PubSub wakes that carry no other local task scope. */
const PUBSUB_COORDINATION_ISSUE_TITLE = "PubSub Coordination";
/** Receipt statuses in which the heartbeat core owns the wake right now (in flight or durably deferred). */
const LIVE_WAKE_STATUSES = ["queued", "claimed", "coalesced", "deferred_issue_execution", "running"] as const;
/** Live receipts plus terminal core receipts that handled the wake (or own its bounded retry lane). The service re-arm sweep mirrors this verdict: a message with any of these receipts is already durably delivered and must not be re-armed. */
export const PUBSUB_DELIVERED_WAKE_STATUSES = [...LIVE_WAKE_STATUSES, "completed", "failed"] as const;
/** The heartbeat's documented budget-pause cancellation — the only cancellation that must redeliver. */
export const PUBSUB_BUDGET_PAUSE_CANCELLATION = "Cancelled due to budget pause";
/**
 * Run statuses meaning the heartbeat core has settled the linked run. Mirrors the
 * receipt-reconciliation sweep in the PubSub service: a live wake receipt whose
 * run has settled no longer holds the company's wake slot — the platform's own
 * recovery (controller-lease expiry, the periodic orphan reaper, and that
 * reconciliation) is what transitions the run into these statuses, so slot
 * release tracks the platform's recovery instead of an arbitrary clock.
 */
export const PUBSUB_SETTLED_RUN_STATUSES = [
  "succeeded", "failed", "cancelled", "timed_out", "interrupted", "skipped",
] as const;
/**
 * Task-state journal topics: the activity journal's signed peer traffic. The
 * PubSub receiver persists every inbound message to inbox/history before this
 * wake runs, so these events are visible to fleet peers from history without
 * ever enqueuing a CEO wake. fleet.task.review (operator escalation) is
 * deliberately NOT in this table and keeps the existing wake path.
 */
const TASK_STATE_JOURNAL_TOPICS: Record<string, true> = {
  "fleet.task.created": true,
  "fleet.task.updated": true,
  "fleet.task.comment_added": true,
  "fleet.task.completed": true,
  "fleet.task.blocked": true,
  "fleet.task.cancelled": true,
};
interface PubsubHeartbeat {
  wakeup(agentId: string, options: {
    source: "automation";
    triggerDetail: "system";
    reason: string;
    idempotencyKey: string;
    requestedByActorType: "system";
    requestedByActorId: string;
    allowRunCoalescing: false;
    payload: Record<string, unknown>;
    contextSnapshot: Record<string, unknown>;
  }): Promise<unknown>;
}

/** The same persisted root CEO is used for API authorization and inbound wakes. */
export async function findPubsubCeo(db: Db, companyId: string) {
  const [ceo] = await db.select({ id: agents.id, adapterType: agents.adapterType }).from(agents).where(and(
    eq(agents.companyId, companyId),
    eq(agents.role, "ceo"),
    isNull(agents.reportsTo),
    notInArray(agents.status, ["terminated", "pending_approval"]),
  )).orderBy(asc(agents.createdAt), asc(agents.id)).limit(1);
  return ceo ?? null;
}

/**
 * Native-runner CEOs can only execute scoped runs: a fresh Paperclip Runner
 * selection rejects a null issue with paperclip_runner_issue_ineligible, while
 * the durable wake receipt would already suppress redelivery. The wake is
 * therefore associated with the company's standing PubSub coordination issue
 * and its id is persisted on the wake request and run snapshot, not
 * recomputed at execution.
 *
 * Creation is concurrent-safe: it runs inside a transaction and the issue
 * service's `allowDuplicate: false` path serializes same-title creators under
 * an advisory lock, deduping against the open standing issue — so parallel
 * PubSub wakes for a fresh company always converge on a single issue.
 */
export async function findOrCreatePubsubCoordinationIssue(db: Db, companyId: string): Promise<string> {
  return db.transaction(async (tx) => {
    const [standing] = await tx.select({ id: issues.id }).from(issues).where(and(
      eq(issues.companyId, companyId),
      eq(issues.title, PUBSUB_COORDINATION_ISSUE_TITLE),
      notInArray(issues.status, ["done", "cancelled"]),
    )).orderBy(asc(issues.createdAt)).limit(1);
    if (standing) return standing.id;
    // Unassigned `todo` like the board concierge's standing issue: the service
    // rejects in_progress issues without an assignee, and the run does not need
    // ownership to execute on the issue.
    const created = await issueService(db).create(companyId, {
      title: PUBSUB_COORDINATION_ISSUE_TITLE,
      description: "Standing issue anchoring PubSub coordination traffic for the company's CEO.",
      status: "todo",
      priority: "medium",
      allowDuplicate: false,
    }, tx);
    return created.id;
  });
}

export function createPubsubWake(db: Db, heartbeat: PubsubHeartbeat) {
  return async (companyId: string, message: PubsubInboundMessage) => {
    if (TASK_STATE_JOURNAL_TOPICS[message.topic]) return;
    const idempotencyKey = `pubsub:${companyId}:${message.fromInstance}:${message.fromCompany}:${message.id}`;
    // Receipt verdict: a delivered receipt means the core owns the delivery; an
    // intentional (non-budget) cancellation is operator intent and is final.
    // Neither exists, or only `skipped` / budget-pause `cancelled` receipts exist,
    // means the wake was never durably delivered — enqueue it (re)below. The
    // service's re-arm sweep re-sets wakePending for receipts cancelled while
    // this worker was gone.
    const receipts = await db.select({ status: agentWakeupRequests.status, error: agentWakeupRequests.error })
      .from(agentWakeupRequests)
      .where(and(
        eq(agentWakeupRequests.companyId, companyId),
        eq(agentWakeupRequests.idempotencyKey, idempotencyKey),
      )).orderBy(desc(agentWakeupRequests.requestedAt)).limit(16);
    if (receipts.some((receipt) => PUBSUB_DELIVERED_WAKE_STATUSES.includes(receipt.status as (typeof PUBSUB_DELIVERED_WAKE_STATUSES)[number]))) return;
    if (receipts.some((receipt) => receipt.status === "cancelled" && receipt.error !== PUBSUB_BUDGET_PAUSE_CANCELLATION)) return;

    // Coalescing, cooldown, and the wake enqueue run under a company-scoped
    // advisory lock inside one transaction, so concurrent server instances
    // cannot both pass the guard before either commits: the losing worker
    // blocks on the lock, observes the winner's committed receipt, and defers
    // with the same durable backpressure. The cooldown extends the in-flight
    // guard to recently settled wakes, bounding sustained CEO execution to
    // one run per company per window instead of one per completed message.
    //
    // The live clause is bound to the linked run's liveness, not just receipt
    // age: a receipt whose run the platform's own recovery (controller-lease
    // expiry, the periodic orphan reaper, receipt reconciliation) has not
    // settled holds the slot even past the stale window — a healthy
    // long-running CEO wake must not release the slot mid-run and admit a
    // second, non-coalesced wake. Live receipts without a settled-run link
    // (not yet claimed by a run) fall back to the stale window, which bounds
    // the slot for owners that died before their run settled; the per-agent
    // concurrency policy and the PubSub rate/pending-wake limits still bound
    // any wake admitted past the guard.
    await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`pubsub-wake:${companyId}`}, 0))`);
      const [blockedCompanyWake] = await tx.select({ id: agentWakeupRequests.id }).from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, companyId),
        like(agentWakeupRequests.idempotencyKey, `pubsub:${companyId}:%`),
        or(
          and(inArray(agentWakeupRequests.status, [...LIVE_WAKE_STATUSES]),
            or(
              exists(
                tx.select({ id: heartbeatRuns.id })
                  .from(heartbeatRuns)
                  .where(and(
                    eq(heartbeatRuns.id, agentWakeupRequests.runId),
                    notInArray(heartbeatRuns.status, [...PUBSUB_SETTLED_RUN_STATUSES]),
                  )),
              ),
              gte(agentWakeupRequests.updatedAt, new Date(Date.now() - PUBSUB_WAKE_STALE_MS)),
            ),
          ),
          and(inArray(agentWakeupRequests.status, ["completed", "failed"]),
            gte(agentWakeupRequests.finishedAt, new Date(Date.now() - PUBSUB_WAKE_COOLDOWN_MS))),
        ),
      )).limit(1);
      if (blockedCompanyWake) throw new Error("PubSub wake coalesced: another PubSub wake is in flight or recently settled for this company");

      const ceo = await findPubsubCeo(db, companyId);
      if (!ceo) throw new Error("PubSub delivery is waiting for the company's CEO");
      const wakeMessage = {
        id: message.id,
        topic: message.topic,
        payload: message.payload,
        fromInstance: message.fromInstance,
        fromCompany: message.fromCompany,
        fromAgent: message.fromAgent,
        fromRole: message.fromRole,
      };
      // Only native-runner CEOs need a local task scope to execute; other
      // adapters keep the existing unscoped wake behavior.
      const coordinationIssueId = ceo.adapterType === "paperclip_runner"
        ? await findOrCreatePubsubCoordinationIssue(db, companyId)
        : null;
      await heartbeat.wakeup(ceo.id, {
        source: "automation",
        triggerDetail: "system",
        reason: "pubsub_message",
        idempotencyKey,
        requestedByActorType: "system",
        requestedByActorId: "pubsub",
        allowRunCoalescing: false,
        payload: coordinationIssueId ? { issueId: coordinationIssueId, pubsubMessage: wakeMessage } : { pubsubMessage: wakeMessage },
        contextSnapshot: coordinationIssueId
          ? { issueId: coordinationIssueId, wakeReason: "pubsub_message", pubsubMessage: wakeMessage }
          : { wakeReason: "pubsub_message", pubsubMessage: wakeMessage },
      });
      // A null result can mean durable deferral OR a skipped wake. Only the former
      // satisfies delivery; the core keeps retrying skipped/unavailable CEOs.
      const [delivered] = await tx.select({ id: agentWakeupRequests.id }).from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, companyId),
        eq(agentWakeupRequests.idempotencyKey, idempotencyKey),
        inArray(agentWakeupRequests.status, [...PUBSUB_DELIVERED_WAKE_STATUSES]),
      )).limit(1);
      if (!delivered) throw new Error("PubSub CEO wake was not durably queued");
    });
  };
}
