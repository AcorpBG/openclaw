import {
  isHeartbeatActionWakeReason,
  normalizeHeartbeatWakeReason,
  resolveHeartbeatReasonKind,
} from "./heartbeat-reason.js";

export type HeartbeatRunResult =
  | { status: "ran"; durationMs: number }
  | { status: "skipped"; reason: string }
  | { status: "failed"; reason: string };

export type HeartbeatWakeHandler = (opts: {
  reason?: string;
  agentId?: string;
  sessionKey?: string;
}) => Promise<HeartbeatRunResult>;

export function setHeartbeatsEnabled(enabled: boolean) {
  state.heartbeatsEnabled = enabled;
}

export function areHeartbeatsEnabled(): boolean {
  return state.heartbeatsEnabled;
}

type WakeTimerKind = "normal" | "retry";
type PendingWakeReason = {
  reason: string;
  priority: number;
  requestedAt: number;
  agentId?: string;
  sessionKey?: string;
};

type HeartbeatWakeState = {
  heartbeatsEnabled: boolean;
  handler: HeartbeatWakeHandler | null;
  handlerGeneration: number;
  pendingWakes: Map<string, PendingWakeReason>;
  scheduled: boolean;
  running: boolean;
  timer: NodeJS.Timeout | null;
  timerDueAt: number | null;
  timerKind: WakeTimerKind | null;
};

const HEARTBEAT_WAKE_STATE_KEY = "__openclawHeartbeatWakeState";
// Keep wake coordination on globalThis so duplicate bundled module copies
// still share a single handler/timer lifecycle within one process.
const state = ((globalThis as Record<string, unknown>)[HEARTBEAT_WAKE_STATE_KEY] ??= {
  heartbeatsEnabled: true,
  handler: null,
  handlerGeneration: 0,
  pendingWakes: new Map<string, PendingWakeReason>(),
  scheduled: false,
  running: false,
  timer: null,
  timerDueAt: null,
  timerKind: null,
}) as HeartbeatWakeState;

const DEFAULT_COALESCE_MS = 250;
const DEFAULT_RETRY_MS = 1_000;
const REASON_PRIORITY = {
  RETRY: 0,
  INTERVAL: 1,
  DEFAULT: 2,
  ACTION: 3,
} as const;

function resolveReasonPriority(reason: string): number {
  const kind = resolveHeartbeatReasonKind(reason);
  if (kind === "retry") {
    return REASON_PRIORITY.RETRY;
  }
  if (kind === "interval") {
    return REASON_PRIORITY.INTERVAL;
  }
  if (isHeartbeatActionWakeReason(reason)) {
    return REASON_PRIORITY.ACTION;
  }
  return REASON_PRIORITY.DEFAULT;
}

function normalizeWakeReason(reason?: string): string {
  return normalizeHeartbeatWakeReason(reason);
}

function normalizeWakeTarget(value?: string): string | undefined {
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed || undefined;
}

function getWakeTargetKey(params: { agentId?: string; sessionKey?: string }) {
  const agentId = normalizeWakeTarget(params.agentId);
  const sessionKey = normalizeWakeTarget(params.sessionKey);
  return `${agentId ?? ""}::${sessionKey ?? ""}`;
}

function queuePendingWakeReason(params?: {
  reason?: string;
  requestedAt?: number;
  agentId?: string;
  sessionKey?: string;
}) {
  const requestedAt = params?.requestedAt ?? Date.now();
  const normalizedReason = normalizeWakeReason(params?.reason);
  const normalizedAgentId = normalizeWakeTarget(params?.agentId);
  const normalizedSessionKey = normalizeWakeTarget(params?.sessionKey);
  const wakeTargetKey = getWakeTargetKey({
    agentId: normalizedAgentId,
    sessionKey: normalizedSessionKey,
  });
  const next: PendingWakeReason = {
    reason: normalizedReason,
    priority: resolveReasonPriority(normalizedReason),
    requestedAt,
    agentId: normalizedAgentId,
    sessionKey: normalizedSessionKey,
  };
  const previous = state.pendingWakes.get(wakeTargetKey);
  if (!previous) {
    state.pendingWakes.set(wakeTargetKey, next);
    return;
  }
  if (next.priority > previous.priority) {
    state.pendingWakes.set(wakeTargetKey, next);
    return;
  }
  if (next.priority === previous.priority && next.requestedAt >= previous.requestedAt) {
    state.pendingWakes.set(wakeTargetKey, next);
  }
}

function schedule(coalesceMs: number, kind: WakeTimerKind = "normal") {
  const delay = Number.isFinite(coalesceMs) ? Math.max(0, coalesceMs) : DEFAULT_COALESCE_MS;
  const dueAt = Date.now() + delay;
  if (state.timer) {
    // Keep retry cooldown as a hard minimum delay. This prevents the
    // finally-path reschedule (often delay=0) from collapsing backoff.
    if (state.timerKind === "retry") {
      return;
    }
    // If existing timer fires sooner or at the same time, keep it.
    if (typeof state.timerDueAt === "number" && state.timerDueAt <= dueAt) {
      return;
    }
    // New request needs to fire sooner — preempt the existing timer.
    clearTimeout(state.timer);
    state.timer = null;
    state.timerDueAt = null;
    state.timerKind = null;
  }
  state.timerDueAt = dueAt;
  state.timerKind = kind;
  state.timer = setTimeout(async () => {
    state.timer = null;
    state.timerDueAt = null;
    state.timerKind = null;
    state.scheduled = false;
    const active = state.handler;
    if (!active) {
      return;
    }
    if (state.running) {
      state.scheduled = true;
      schedule(delay, kind);
      return;
    }

    const pendingBatch = Array.from(state.pendingWakes.values());
    state.pendingWakes.clear();
    state.running = true;
    try {
      for (const pendingWake of pendingBatch) {
        const wakeOpts = {
          reason: pendingWake.reason ?? undefined,
          ...(pendingWake.agentId ? { agentId: pendingWake.agentId } : {}),
          ...(pendingWake.sessionKey ? { sessionKey: pendingWake.sessionKey } : {}),
        };
        const res = await active(wakeOpts);
        if (res.status === "skipped" && res.reason === "requests-in-flight") {
          // The main lane is busy; retry this wake target soon.
          queuePendingWakeReason({
            reason: pendingWake.reason ?? "retry",
            agentId: pendingWake.agentId,
            sessionKey: pendingWake.sessionKey,
          });
          schedule(DEFAULT_RETRY_MS, "retry");
        }
      }
    } catch {
      // Error is already logged by the heartbeat runner; schedule a retry.
      for (const pendingWake of pendingBatch) {
        queuePendingWakeReason({
          reason: pendingWake.reason ?? "retry",
          agentId: pendingWake.agentId,
          sessionKey: pendingWake.sessionKey,
        });
      }
      schedule(DEFAULT_RETRY_MS, "retry");
    } finally {
      state.running = false;
      if (state.pendingWakes.size > 0 || state.scheduled) {
        schedule(delay, "normal");
      }
    }
  }, delay);
  state.timer.unref?.();
}

/**
 * Register (or clear) the heartbeat wake handler.
 * Returns a disposer function that clears this specific registration.
 * Stale disposers (from previous registrations) are no-ops, preventing
 * a race where an old runner's cleanup clears a newer runner's handler.
 */
export function setHeartbeatWakeHandler(next: HeartbeatWakeHandler | null): () => void {
  state.handlerGeneration += 1;
  const generation = state.handlerGeneration;
  state.handler = next;
  if (next) {
    // New lifecycle starting (e.g. after SIGUSR1 in-process restart).
    // Clear any timer metadata from the previous lifecycle so stale retry
    // cooldowns do not delay a fresh handler.
    if (state.timer) {
      clearTimeout(state.timer);
    }
    state.timer = null;
    state.timerDueAt = null;
    state.timerKind = null;
    // Reset module-level execution state that may be stale from interrupted
    // runs in the previous lifecycle. Without this, `running === true` from
    // an interrupted heartbeat blocks all future schedule() attempts, and
    // `scheduled === true` can cause spurious immediate re-runs.
    state.running = false;
    state.scheduled = false;
  }
  if (state.handler && state.pendingWakes.size > 0) {
    schedule(DEFAULT_COALESCE_MS, "normal");
  }
  return () => {
    if (state.handlerGeneration !== generation) {
      return;
    }
    if (state.handler !== next) {
      return;
    }
    state.handlerGeneration += 1;
    state.handler = null;
  };
}

export function requestHeartbeatNow(opts?: {
  reason?: string;
  coalesceMs?: number;
  agentId?: string;
  sessionKey?: string;
}) {
  queuePendingWakeReason({
    reason: opts?.reason,
    agentId: opts?.agentId,
    sessionKey: opts?.sessionKey,
  });
  schedule(opts?.coalesceMs ?? DEFAULT_COALESCE_MS, "normal");
}

export function hasHeartbeatWakeHandler() {
  return state.handler !== null;
}

export function hasPendingHeartbeatWake() {
  return state.pendingWakes.size > 0 || Boolean(state.timer) || state.scheduled;
}

export function resetHeartbeatWakeStateForTests() {
  if (state.timer) {
    clearTimeout(state.timer);
  }
  state.timer = null;
  state.timerDueAt = null;
  state.timerKind = null;
  state.pendingWakes.clear();
  state.scheduled = false;
  state.running = false;
  state.handlerGeneration += 1;
  state.handler = null;
}
