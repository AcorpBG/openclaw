import { Type } from "@sinclair/typebox";
import { logVerbose } from "../../globals.js";
import type { GatewayMessageChannel } from "../../utils/message-channel.js";
import { ACP_SPAWN_MODES, ACP_SPAWN_STREAM_TARGETS, spawnAcpDirect } from "../acp-spawn.js";
import { optionalStringEnum } from "../schema/typebox.js";
import type { SpawnedToolContext } from "../spawned-context.js";
import { SUBAGENT_SPAWN_MODES, spawnSubagentDirect } from "../subagent-spawn.js";
import type { AnyAgentTool } from "./common.js";
import { jsonResult, readStringParam, ToolInputError } from "./common.js";

const SESSIONS_SPAWN_RUNTIMES = ["subagent", "acp"] as const;
const SESSIONS_SPAWN_SANDBOX_MODES = ["inherit", "require"] as const;
const UNSUPPORTED_SESSIONS_SPAWN_PARAM_KEYS = [
  "target",
  "transport",
  "channel",
  "to",
  "threadId",
  "thread_id",
  "replyTo",
  "reply_to",
] as const;

function formatSessionsSpawnDiagnosticValue(value: unknown): string {
  return typeof value === "string" && value.trim() ? value.trim() : "-";
}

function logSessionsSpawnDiagnostic(message: string): void {
  logVerbose(`sessions-spawn: ${message}`);
}

const SessionsSpawnToolSchema = Type.Object({
  task: Type.String(),
  label: Type.Optional(Type.String()),
  runtime: optionalStringEnum(SESSIONS_SPAWN_RUNTIMES),
  agentId: Type.Optional(Type.String()),
  model: Type.Optional(Type.String()),
  thinking: Type.Optional(Type.String()),
  cwd: Type.Optional(Type.String()),
  runTimeoutSeconds: Type.Optional(Type.Number({ minimum: 0 })),
  // Back-compat: older callers used timeoutSeconds for this tool.
  timeoutSeconds: Type.Optional(Type.Number({ minimum: 0 })),
  thread: Type.Optional(Type.Boolean()),
  mode: optionalStringEnum(SUBAGENT_SPAWN_MODES),
  cleanup: optionalStringEnum(["delete", "keep"] as const),
  sandbox: optionalStringEnum(SESSIONS_SPAWN_SANDBOX_MODES),
  streamTo: optionalStringEnum(ACP_SPAWN_STREAM_TARGETS),

  // Inline attachments (snapshot-by-value).
  // NOTE: Attachment contents are redacted from transcript persistence by sanitizeToolCallInputs.
  attachments: Type.Optional(
    Type.Array(
      Type.Object({
        name: Type.String(),
        content: Type.String(),
        encoding: Type.Optional(optionalStringEnum(["utf8", "base64"] as const)),
        mimeType: Type.Optional(Type.String()),
      }),
      { maxItems: 50 },
    ),
  ),
  attachAs: Type.Optional(
    Type.Object({
      // Where the spawned agent should look for attachments.
      // Kept as a hint; implementation materializes into the child workspace.
      mountPath: Type.Optional(Type.String()),
    }),
  ),
});

export function createSessionsSpawnTool(
  opts?: {
    agentSessionKey?: string;
    agentChannel?: GatewayMessageChannel;
    agentAccountId?: string;
    agentTo?: string;
    agentThreadId?: string | number;
    sandboxed?: boolean;
    /** Explicit agent ID override for cron/hook sessions where session key parsing may not work. */
    requesterAgentIdOverride?: string;
  } & SpawnedToolContext,
): AnyAgentTool {
  return {
    label: "Sessions",
    name: "sessions_spawn",
    description:
      'Spawn an isolated session (runtime="subagent" or runtime="acp"). mode="run" is one-shot and mode="session" is persistent/thread-bound. Subagents inherit the parent workspace directory automatically.',
    parameters: SessionsSpawnToolSchema,
    execute: async (_toolCallId, args) => {
      const params = args as Record<string, unknown>;
      const rawRuntime = params.runtime === "acp" ? "acp" : "subagent";
      logSessionsSpawnDiagnostic(
        [
          "entry",
          `runtime=${rawRuntime}`,
          `agentId=${formatSessionsSpawnDiagnosticValue(params.agentId)}`,
          `mode=${formatSessionsSpawnDiagnosticValue(params.mode)}`,
          `thread=${params.thread === true}`,
          `model=${formatSessionsSpawnDiagnosticValue(params.model)}`,
          `thinking=${formatSessionsSpawnDiagnosticValue(params.thinking)}`,
          `cwd=${formatSessionsSpawnDiagnosticValue(params.cwd)}`,
        ].join(" "),
      );
      const unsupportedParam = UNSUPPORTED_SESSIONS_SPAWN_PARAM_KEYS.find((key) =>
        Object.hasOwn(params, key),
      );
      if (unsupportedParam) {
        logSessionsSpawnDiagnostic(
          ["reject unsupported-param", `runtime=${rawRuntime}`, `param=${unsupportedParam}`].join(
            " ",
          ),
        );
        throw new ToolInputError(
          `sessions_spawn does not support "${unsupportedParam}". Use "message" or "sessions_send" for channel delivery.`,
        );
      }
      const task = readStringParam(params, "task", { required: true });
      const label = typeof params.label === "string" ? params.label.trim() : "";
      const runtime = params.runtime === "acp" ? "acp" : "subagent";
      const requestedAgentId = readStringParam(params, "agentId");
      const modelOverride = readStringParam(params, "model");
      const thinkingOverrideRaw = readStringParam(params, "thinking");
      const cwd = readStringParam(params, "cwd");
      const mode = params.mode === "run" || params.mode === "session" ? params.mode : undefined;
      const cleanup =
        params.cleanup === "keep" || params.cleanup === "delete" ? params.cleanup : "keep";
      const sandbox = params.sandbox === "require" ? "require" : "inherit";
      const streamTo = params.streamTo === "parent" ? "parent" : undefined;
      // Back-compat: older callers used timeoutSeconds for this tool.
      const timeoutSecondsCandidate =
        typeof params.runTimeoutSeconds === "number"
          ? params.runTimeoutSeconds
          : typeof params.timeoutSeconds === "number"
            ? params.timeoutSeconds
            : undefined;
      const runTimeoutSeconds =
        typeof timeoutSecondsCandidate === "number" && Number.isFinite(timeoutSecondsCandidate)
          ? Math.max(0, Math.floor(timeoutSecondsCandidate))
          : undefined;
      const thread = params.thread === true;
      const attachments = Array.isArray(params.attachments)
        ? (params.attachments as Array<{
            name: string;
            content: string;
            encoding?: "utf8" | "base64";
            mimeType?: string;
          }>)
        : undefined;

      logSessionsSpawnDiagnostic(
        [
          "normalized",
          `runtime=${runtime}`,
          `agentId=${formatSessionsSpawnDiagnosticValue(requestedAgentId)}`,
          `mode=${formatSessionsSpawnDiagnosticValue(mode)}`,
          `thread=${thread}`,
          `model=${formatSessionsSpawnDiagnosticValue(modelOverride)}`,
          `thinking=${formatSessionsSpawnDiagnosticValue(thinkingOverrideRaw)}`,
          `cwd=${formatSessionsSpawnDiagnosticValue(cwd)}`,
          `sandbox=${sandbox}`,
          `streamTo=${formatSessionsSpawnDiagnosticValue(streamTo)}`,
          `attachments=${attachments?.length ?? 0}`,
        ].join(" "),
      );

      if (streamTo && runtime !== "acp") {
        logSessionsSpawnDiagnostic(
          [
            "reject stream-target-runtime-mismatch",
            `runtime=${runtime}`,
            `streamTo=${streamTo}`,
          ].join(" "),
        );
        return jsonResult({
          status: "error",
          error: `streamTo is only supported for runtime=acp; got runtime=${runtime}`,
        });
      }

      if (runtime === "acp") {
        if (Array.isArray(attachments) && attachments.length > 0) {
          logSessionsSpawnDiagnostic(
            [
              "reject acp-attachments-unsupported",
              `runtime=${runtime}`,
              `attachments=${attachments.length}`,
            ].join(" "),
          );
          return jsonResult({
            status: "error",
            error:
              "attachments are currently unsupported for runtime=acp; use runtime=subagent or remove attachments",
          });
        }
        logSessionsSpawnDiagnostic(
          [
            "dispatch spawnAcpDirect",
            `runtime=${runtime}`,
            `agentId=${formatSessionsSpawnDiagnosticValue(requestedAgentId)}`,
            `mode=${formatSessionsSpawnDiagnosticValue(mode)}`,
            `thread=${thread}`,
            `model=${formatSessionsSpawnDiagnosticValue(modelOverride)}`,
            `thinking=${formatSessionsSpawnDiagnosticValue(thinkingOverrideRaw)}`,
            `cwd=${formatSessionsSpawnDiagnosticValue(cwd)}`,
          ].join(" "),
        );
        const result = await spawnAcpDirect(
          {
            task,
            label: label || undefined,
            agentId: requestedAgentId,
            model: modelOverride,
            thinking: thinkingOverrideRaw,
            cwd,
            mode: mode && ACP_SPAWN_MODES.includes(mode) ? mode : undefined,
            thread,
            sandbox,
            streamTo,
          },
          {
            agentSessionKey: opts?.agentSessionKey,
            agentChannel: opts?.agentChannel,
            agentAccountId: opts?.agentAccountId,
            agentTo: opts?.agentTo,
            agentThreadId: opts?.agentThreadId,
            sandboxed: opts?.sandboxed,
          },
        );
        return jsonResult(result);
      }

      const result = await spawnSubagentDirect(
        {
          task,
          label: label || undefined,
          agentId: requestedAgentId,
          model: modelOverride,
          thinking: thinkingOverrideRaw,
          runTimeoutSeconds,
          thread,
          mode,
          cleanup,
          sandbox,
          expectsCompletionMessage: true,
          attachments,
          attachMountPath:
            params.attachAs && typeof params.attachAs === "object"
              ? readStringParam(params.attachAs as Record<string, unknown>, "mountPath")
              : undefined,
        },
        {
          agentSessionKey: opts?.agentSessionKey,
          agentChannel: opts?.agentChannel,
          agentAccountId: opts?.agentAccountId,
          agentTo: opts?.agentTo,
          agentThreadId: opts?.agentThreadId,
          agentGroupId: opts?.agentGroupId,
          agentGroupChannel: opts?.agentGroupChannel,
          agentGroupSpace: opts?.agentGroupSpace,
          requesterAgentIdOverride: opts?.requesterAgentIdOverride,
          workspaceDir: opts?.workspaceDir,
        },
      );

      return jsonResult(result);
    },
  };
}
