import { createInterface } from "node:readline";
import type {
  AcpRuntimeCapabilities,
  AcpRuntimeDoctorReport,
  AcpRuntime,
  AcpRuntimeEnsureInput,
  AcpRuntimeErrorCode,
  AcpRuntimeEvent,
  AcpRuntimeHandle,
  AcpRuntimeStatus,
  AcpRuntimeTurnInput,
  PluginLogger,
} from "openclaw/plugin-sdk/acpx";
import { AcpRuntimeError } from "openclaw/plugin-sdk/acpx";
import { toAcpMcpServers, type ResolvedAcpxPluginConfig } from "./config.js";
import { checkAcpxVersion } from "./ensure.js";
import { buildCodexBootstrapAgentCommand } from "./runtime-internals/codex-agent-command.js";
import {
  parseJsonLines,
  parsePromptEventLine,
  toAcpxErrorEvent,
} from "./runtime-internals/events.js";
import {
  buildMcpProxyAgentCommand,
  resolveAcpxAgentCommandWithSource,
} from "./runtime-internals/mcp-agent-command.js";
import {
  resolveSpawnFailure,
  type SpawnCommandCache,
  type SpawnCommandOptions,
  type SpawnResolutionEvent,
  spawnAndCollect,
  spawnWithResolvedCommand,
  waitForExit,
} from "./runtime-internals/process.js";
import {
  asOptionalString,
  asTrimmedString,
  buildPermissionArgs,
  deriveAgentFromSessionKey,
  isRecord,
  type AcpxCodexBootstrapState,
  type AcpxHandleState,
  type AcpxJsonObject,
} from "./runtime-internals/shared.js";

export const ACPX_BACKEND_ID = "acpx";

const ACPX_RUNTIME_HANDLE_PREFIX = "acpx:v1:";
const DEFAULT_AGENT_FALLBACK = "codex";
const ACPX_EXIT_CODE_PERMISSION_DENIED = 5;
const ACPX_CODEX_BOOTSTRAP_ENV_KEY = "OPENCLAW_ACPX_CODEX_BOOTSTRAP";
const ACPX_CODEX_BOOTSTRAP_WRAPPER_PATH_HINT = "codex-bootstrap-wrapper.mjs";
const ACPX_MCP_PROXY_PATH_HINT = "mcp-proxy.mjs";
const ACPX_CAPABILITIES: AcpRuntimeCapabilities = {
  controls: ["session/set_mode", "session/set_config_option", "session/status"],
};

type DecodedCodexBootstrapEnv = {
  present: boolean;
  decoded: boolean;
  state?: AcpxCodexBootstrapState;
};

type CachedAgentCommandDetails = {
  rawAgentCommand: string;
  source: "config-override" | "builtin-fallback" | "agent-id";
  targetCommand: string;
  wrapper: "none" | "codex-bootstrap" | "mcp-proxy" | "codex-bootstrap+mcp-proxy";
};

type ResolvedAgentCommandDetails =
  | ({
      rawAgentCommand: string;
      cacheHit: boolean;
    } & CachedAgentCommandDetails)
  | {
      rawAgentCommand: null;
      cacheHit: false;
      source: "direct-agent";
      targetCommand?: undefined;
      wrapper: "none";
    };

function formatPermissionModeGuidance(): string {
  return "Configure plugins.entries.acpx.config.permissionMode to one of: approve-reads, approve-all, deny-all.";
}

function formatAcpxExitMessage(params: {
  stderr: string;
  exitCode: number | null | undefined;
}): string {
  const stderr = params.stderr.trim();
  if (params.exitCode === ACPX_EXIT_CODE_PERMISSION_DENIED) {
    return [
      stderr || "Permission denied by ACP runtime (acpx).",
      "ACPX blocked a write/exec permission request in a non-interactive session.",
      formatPermissionModeGuidance(),
    ].join(" ");
  }
  return stderr || `acpx exited with code ${params.exitCode ?? "unknown"}`;
}

export function encodeAcpxRuntimeHandleState(state: AcpxHandleState): string {
  const payload = Buffer.from(JSON.stringify(state), "utf8").toString("base64url");
  return `${ACPX_RUNTIME_HANDLE_PREFIX}${payload}`;
}

export function decodeAcpxRuntimeHandleState(runtimeSessionName: string): AcpxHandleState | null {
  const trimmed = runtimeSessionName.trim();
  if (!trimmed.startsWith(ACPX_RUNTIME_HANDLE_PREFIX)) {
    return null;
  }
  const encoded = trimmed.slice(ACPX_RUNTIME_HANDLE_PREFIX.length);
  if (!encoded) {
    return null;
  }
  try {
    const raw = Buffer.from(encoded, "base64url").toString("utf8");
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed)) {
      return null;
    }
    const name = asTrimmedString(parsed.name);
    const agent = asTrimmedString(parsed.agent);
    const cwd = asTrimmedString(parsed.cwd);
    const mode = asTrimmedString(parsed.mode);
    const codexBootstrap = parseCodexBootstrapState(parsed.codexBootstrap);
    const acpxRecordId = asOptionalString(parsed.acpxRecordId);
    const backendSessionId = asOptionalString(parsed.backendSessionId);
    const agentSessionId = asOptionalString(parsed.agentSessionId);
    if (!name || !agent || !cwd) {
      return null;
    }
    if (mode !== "persistent" && mode !== "oneshot") {
      return null;
    }
    return {
      name,
      agent,
      cwd,
      mode,
      ...(codexBootstrap ? { codexBootstrap } : {}),
      ...(acpxRecordId ? { acpxRecordId } : {}),
      ...(backendSessionId ? { backendSessionId } : {}),
      ...(agentSessionId ? { agentSessionId } : {}),
    };
  } catch {
    return null;
  }
}

function parseCodexBootstrapState(value: unknown): AcpxCodexBootstrapState | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const model = asOptionalString(value.model);
  const reasoningEffort = asOptionalString(value.reasoningEffort);
  if (!model && !reasoningEffort) {
    return undefined;
  }
  return {
    ...(model ? { model } : {}),
    ...(reasoningEffort ? { reasoningEffort } : {}),
  };
}

function decodeCodexBootstrapEnvDetailed(env?: Record<string, string>): DecodedCodexBootstrapEnv {
  const encoded = env?.[ACPX_CODEX_BOOTSTRAP_ENV_KEY];
  if (!encoded) {
    return {
      present: false,
      decoded: false,
    };
  }
  try {
    const parsed = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")) as unknown;
    const state = parseCodexBootstrapState(parsed);
    return {
      present: true,
      decoded: Boolean(state),
      ...(state ? { state } : {}),
    };
  } catch {
    return {
      present: true,
      decoded: false,
    };
  }
}

export class AcpxRuntime implements AcpRuntime {
  private healthy = false;
  private readonly logger?: PluginLogger;
  private readonly queueOwnerTtlSeconds: number;
  private readonly spawnCommandCache: SpawnCommandCache = {};
  private readonly agentCommandCache = new Map<string, CachedAgentCommandDetails>();
  private readonly spawnCommandOptions: SpawnCommandOptions;
  private readonly loggedSpawnResolutions = new Set<string>();

  constructor(
    private readonly config: ResolvedAcpxPluginConfig,
    opts?: {
      logger?: PluginLogger;
      queueOwnerTtlSeconds?: number;
    },
  ) {
    this.logger = opts?.logger;
    const requestedQueueOwnerTtlSeconds = opts?.queueOwnerTtlSeconds;
    this.queueOwnerTtlSeconds =
      typeof requestedQueueOwnerTtlSeconds === "number" &&
      Number.isFinite(requestedQueueOwnerTtlSeconds) &&
      requestedQueueOwnerTtlSeconds >= 0
        ? requestedQueueOwnerTtlSeconds
        : this.config.queueOwnerTtlSeconds;
    this.spawnCommandOptions = {
      strictWindowsCmdWrapper: this.config.strictWindowsCmdWrapper,
      cache: this.spawnCommandCache,
      onResolved: (event) => {
        this.logSpawnResolution(event);
      },
    };
  }

  isHealthy(): boolean {
    return this.healthy;
  }

  private debug(message: string): void {
    this.logger?.debug?.(message);
  }

  private formatDiagnosticValue(value: string | undefined | null): string {
    const trimmed = typeof value === "string" ? value.trim() : "";
    return trimmed || "-";
  }

  private summarizeCodexBootstrap(state?: AcpxCodexBootstrapState): string {
    return [
      `model=${this.formatDiagnosticValue(state?.model)}`,
      `reasoning=${this.formatDiagnosticValue(state?.reasoningEffort)}`,
    ].join(" ");
  }

  private summarizeCommandLine(value: string | undefined): string {
    const text = (value ?? "").replace(/\s+/g, " ").trim();
    if (!text) {
      return "-";
    }
    return text.length > 220 ? `${text.slice(0, 217)}...` : text;
  }

  private summarizeStatusDetails(detail: AcpxJsonObject | undefined): string {
    if (!detail) {
      return "-";
    }
    const summary: Record<string, unknown> = {};
    for (const key of [
      "status",
      "pid",
      "reason",
      "message",
      "runtimeDetails",
      "acpxRecordId",
      "acpxSessionId",
      "agentSessionId",
    ]) {
      if (detail[key] !== undefined) {
        summary[key] = detail[key];
      }
    }
    try {
      const serialized = JSON.stringify(summary);
      return serialized.length > 300 ? `${serialized.slice(0, 297)}...` : serialized;
    } catch {
      return "[unserializable-status-detail]";
    }
  }

  private logVerbRouting(params: {
    verb: string;
    agent: string;
    cwd: string;
    sessionName?: string;
    codexBootstrap?: AcpxCodexBootstrapState;
    resolution: ResolvedAgentCommandDetails;
  }): void {
    this.debug(
      [
        "acpx runtime: verb route",
        `verb=${params.verb}`,
        `session=${this.formatDiagnosticValue(params.sessionName)}`,
        `agent=${params.agent}`,
        `cwd=${this.formatDiagnosticValue(params.cwd)}`,
        `wrapper=${params.resolution.wrapper}`,
        `commandSource=${params.resolution.source}`,
        `cacheHit=${params.resolution.cacheHit}`,
        `hasBootstrap=${Boolean(
          params.codexBootstrap?.model || params.codexBootstrap?.reasoningEffort,
        )}`,
        this.summarizeCodexBootstrap(params.codexBootstrap),
        `targetCommand=${this.summarizeCommandLine(params.resolution.targetCommand)}`,
      ].join(" "),
    );
  }

  private logSpawnResolution(event: SpawnResolutionEvent): void {
    const key = `${event.command}::${event.strictWindowsCmdWrapper ? "strict" : "compat"}::${event.resolution}`;
    if (event.cacheHit || this.loggedSpawnResolutions.has(key)) {
      return;
    }
    this.loggedSpawnResolutions.add(key);
    this.logger?.debug?.(
      `acpx spawn resolver: command=${event.command} mode=${event.strictWindowsCmdWrapper ? "strict" : "compat"} resolution=${event.resolution}`,
    );
  }

  async probeAvailability(): Promise<void> {
    const versionCheck = await checkAcpxVersion({
      command: this.config.command,
      cwd: this.config.cwd,
      expectedVersion: this.config.expectedVersion,
      spawnOptions: this.spawnCommandOptions,
    });
    if (!versionCheck.ok) {
      this.healthy = false;
      return;
    }

    try {
      const result = await spawnAndCollect(
        {
          command: this.config.command,
          args: ["--help"],
          cwd: this.config.cwd,
        },
        this.spawnCommandOptions,
      );
      this.healthy = result.error == null && (result.code ?? 0) === 0;
    } catch {
      this.healthy = false;
    }
  }

  async ensureSession(input: AcpRuntimeEnsureInput): Promise<AcpRuntimeHandle> {
    const sessionName = asTrimmedString(input.sessionKey);
    if (!sessionName) {
      throw new AcpRuntimeError("ACP_SESSION_INIT_FAILED", "ACP session key is required.");
    }
    const agent = asTrimmedString(input.agent);
    if (!agent) {
      throw new AcpRuntimeError("ACP_SESSION_INIT_FAILED", "ACP agent id is required.");
    }
    const cwd = asTrimmedString(input.cwd) || this.config.cwd;
    const mode = input.mode;
    const bootstrapDecode =
      agent === "codex"
        ? decodeCodexBootstrapEnvDetailed(input.env)
        : ({ present: false, decoded: false } satisfies DecodedCodexBootstrapEnv);
    const codexBootstrap = bootstrapDecode.state;
    if (bootstrapDecode.present && !bootstrapDecode.decoded) {
      this.logger?.warn?.(
        `acpx runtime: Codex bootstrap env present but decode failed sessionKey=${sessionName}`,
      );
    }
    this.debug(
      [
        "acpx runtime: ensureSession start",
        `sessionKey=${sessionName}`,
        `agent=${agent}`,
        `mode=${mode}`,
        `cwd=${this.formatDiagnosticValue(cwd)}`,
        `bootstrapEnvPresent=${bootstrapDecode.present}`,
        `bootstrapEnvDecoded=${bootstrapDecode.decoded}`,
        this.summarizeCodexBootstrap(codexBootstrap),
      ].join(" "),
    );
    const ensureCommand = await this.buildVerbArgs({
      agent,
      cwd,
      codexBootstrap,
      verb: "sessions.ensure",
      sessionName,
      command: ["sessions", "ensure", "--name", sessionName],
    });

    let events = await this.runControlCommand({
      label: "sessions.ensure",
      args: ensureCommand,
      cwd,
      fallbackCode: "ACP_SESSION_INIT_FAILED",
    });
    let ensuredEvent = events.find(
      (event) =>
        asOptionalString(event.agentSessionId) ||
        asOptionalString(event.acpxSessionId) ||
        asOptionalString(event.acpxRecordId),
    );

    if (!ensuredEvent) {
      this.debug(
        `acpx runtime: ensureSession missing identifiers after sessions.ensure sessionKey=${sessionName}; retrying sessions.new`,
      );
      const newCommand = await this.buildVerbArgs({
        agent,
        cwd,
        codexBootstrap,
        verb: "sessions.new",
        sessionName,
        command: ["sessions", "new", "--name", sessionName],
      });
      events = await this.runControlCommand({
        label: "sessions.new",
        args: newCommand,
        cwd,
        fallbackCode: "ACP_SESSION_INIT_FAILED",
      });
      ensuredEvent = events.find(
        (event) =>
          asOptionalString(event.agentSessionId) ||
          asOptionalString(event.acpxSessionId) ||
          asOptionalString(event.acpxRecordId),
      );
      if (!ensuredEvent) {
        throw new AcpRuntimeError(
          "ACP_SESSION_INIT_FAILED",
          `ACP session init failed: neither 'sessions ensure' nor 'sessions new' returned valid session identifiers for ${sessionName}.`,
        );
      }
    }

    const acpxRecordId = ensuredEvent ? asOptionalString(ensuredEvent.acpxRecordId) : undefined;
    const agentSessionId = ensuredEvent ? asOptionalString(ensuredEvent.agentSessionId) : undefined;
    const backendSessionId = ensuredEvent
      ? asOptionalString(ensuredEvent.acpxSessionId)
      : undefined;
    this.debug(
      [
        "acpx runtime: ensureSession handle-state persisted",
        `sessionKey=${sessionName}`,
        `mode=${mode}`,
        `acpxRecordId=${this.formatDiagnosticValue(acpxRecordId)}`,
        `backendSessionId=${this.formatDiagnosticValue(backendSessionId)}`,
        `agentSessionId=${this.formatDiagnosticValue(agentSessionId)}`,
        `bootstrapPersisted=${Boolean(codexBootstrap)}`,
        this.summarizeCodexBootstrap(codexBootstrap),
      ].join(" "),
    );

    return {
      sessionKey: input.sessionKey,
      backend: ACPX_BACKEND_ID,
      runtimeSessionName: encodeAcpxRuntimeHandleState({
        name: sessionName,
        agent,
        cwd,
        mode,
        ...(codexBootstrap ? { codexBootstrap } : {}),
        ...(acpxRecordId ? { acpxRecordId } : {}),
        ...(backendSessionId ? { backendSessionId } : {}),
        ...(agentSessionId ? { agentSessionId } : {}),
      }),
      cwd,
      ...(acpxRecordId ? { acpxRecordId } : {}),
      ...(backendSessionId ? { backendSessionId } : {}),
      ...(agentSessionId ? { agentSessionId } : {}),
    };
  }

  async *runTurn(input: AcpRuntimeTurnInput): AsyncIterable<AcpRuntimeEvent> {
    const state = this.resolveHandleState(input.handle);
    const args = await this.buildPromptArgs({
      agent: state.agent,
      sessionName: state.name,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
    });

    const cancelOnAbort = async () => {
      await this.cancel({
        handle: input.handle,
        reason: "abort-signal",
      }).catch((err) => {
        this.logger?.warn?.(`acpx runtime abort-cancel failed: ${String(err)}`);
      });
    };
    const onAbort = () => {
      void cancelOnAbort();
    };

    if (input.signal?.aborted) {
      await cancelOnAbort();
      return;
    }
    if (input.signal) {
      input.signal.addEventListener("abort", onAbort, { once: true });
    }
    const child = spawnWithResolvedCommand(
      {
        command: this.config.command,
        args,
        cwd: state.cwd,
      },
      this.spawnCommandOptions,
    );
    this.debug(
      [
        "acpx runtime: runTurn spawned",
        `sessionKey=${input.handle.sessionKey}`,
        `session=${state.name}`,
        `agent=${state.agent}`,
        `pid=${child.pid ?? "unknown"}`,
        this.summarizeCodexBootstrap(state.codexBootstrap),
      ].join(" "),
    );
    child.stdin.on("error", () => {
      // Ignore EPIPE when the child exits before stdin flush completes.
    });

    child.stdin.end(input.text);

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    let sawDone = false;
    let sawError = false;
    const lines = createInterface({ input: child.stdout });
    try {
      for await (const line of lines) {
        const parsed = parsePromptEventLine(line);
        if (!parsed) {
          continue;
        }
        if (parsed.type === "done") {
          if (sawDone) {
            continue;
          }
          sawDone = true;
        }
        if (parsed.type === "error") {
          sawError = true;
        }
        yield parsed;
      }

      const exit = await waitForExit(child);
      if (exit.error) {
        const spawnFailure = resolveSpawnFailure(exit.error, state.cwd);
        this.debug(
          `acpx runtime: runTurn exit error session=${state.name} pid=${child.pid ?? "unknown"} error=${exit.error.message}`,
        );
        if (spawnFailure === "missing-command") {
          this.healthy = false;
          throw new AcpRuntimeError(
            "ACP_BACKEND_UNAVAILABLE",
            `acpx command not found: ${this.config.command}`,
            { cause: exit.error },
          );
        }
        if (spawnFailure === "missing-cwd") {
          throw new AcpRuntimeError(
            "ACP_TURN_FAILED",
            `ACP runtime working directory does not exist: ${state.cwd}`,
            { cause: exit.error },
          );
        }
        throw new AcpRuntimeError("ACP_TURN_FAILED", exit.error.message, { cause: exit.error });
      }

      this.debug(
        [
          "acpx runtime: runTurn exit",
          `session=${state.name}`,
          `pid=${child.pid ?? "unknown"}`,
          `code=${exit.code ?? "null"}`,
          `signal=${exit.signal ?? "null"}`,
          `sawDone=${sawDone}`,
          `sawError=${sawError}`,
          `stderr=${this.summarizeCommandLine(stderr)}`,
        ].join(" "),
      );

      if ((exit.code ?? 0) !== 0 && !sawError) {
        yield {
          type: "error",
          message: formatAcpxExitMessage({
            stderr,
            exitCode: exit.code,
          }),
        };
        return;
      }

      if (!sawDone && !sawError) {
        yield { type: "done" };
      }
    } finally {
      lines.close();
      if (input.signal) {
        input.signal.removeEventListener("abort", onAbort);
      }
    }
  }

  getCapabilities(): AcpRuntimeCapabilities {
    return ACPX_CAPABILITIES;
  }

  async getStatus(input: {
    handle: AcpRuntimeHandle;
    signal?: AbortSignal;
  }): Promise<AcpRuntimeStatus> {
    const state = this.resolveHandleState(input.handle);
    const args = await this.buildVerbArgs({
      agent: state.agent,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
      verb: "status",
      sessionName: state.name,
      command: ["status", "--session", state.name],
    });
    const events = await this.runControlCommand({
      label: "status",
      args,
      cwd: state.cwd,
      fallbackCode: "ACP_TURN_FAILED",
      ignoreNoSession: true,
      signal: input.signal,
    });
    const detail = events.find((event) => !toAcpxErrorEvent(event)) ?? events[0];
    if (!detail) {
      this.logger?.warn?.(`acpx runtime: status result unavailable session=${state.name}`);
      return {
        summary: "acpx status unavailable",
      };
    }
    const status = asTrimmedString(detail.status) || "unknown";
    const acpxRecordId = asOptionalString(detail.acpxRecordId);
    const acpxSessionId = asOptionalString(detail.acpxSessionId);
    const agentSessionId = asOptionalString(detail.agentSessionId);
    const pid = typeof detail.pid === "number" && Number.isFinite(detail.pid) ? detail.pid : null;
    const summary = [
      `status=${status}`,
      acpxRecordId ? `acpxRecordId=${acpxRecordId}` : null,
      acpxSessionId ? `acpxSessionId=${acpxSessionId}` : null,
      pid != null ? `pid=${pid}` : null,
    ]
      .filter(Boolean)
      .join(" ");
    const statusLog = [
      "acpx runtime: status result",
      `session=${state.name}`,
      `status=${status}`,
      `pid=${pid ?? "unknown"}`,
      `acpxRecordId=${this.formatDiagnosticValue(acpxRecordId)}`,
      `backendSessionId=${this.formatDiagnosticValue(acpxSessionId)}`,
      `agentSessionId=${this.formatDiagnosticValue(agentSessionId)}`,
      `details=${this.summarizeStatusDetails(detail)}`,
    ].join(" ");
    if (status === "alive") {
      this.debug(statusLog);
    } else {
      this.logger?.warn?.(statusLog);
    }
    return {
      summary,
      ...(acpxRecordId ? { acpxRecordId } : {}),
      ...(acpxSessionId ? { backendSessionId: acpxSessionId } : {}),
      ...(agentSessionId ? { agentSessionId } : {}),
      details: detail,
    };
  }

  async setMode(input: { handle: AcpRuntimeHandle; mode: string }): Promise<void> {
    const state = this.resolveHandleState(input.handle);
    const mode = asTrimmedString(input.mode);
    if (!mode) {
      throw new AcpRuntimeError("ACP_TURN_FAILED", "ACP runtime mode is required.");
    }
    const args = await this.buildVerbArgs({
      agent: state.agent,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
      verb: "set-mode",
      sessionName: state.name,
      command: ["set-mode", mode, "--session", state.name],
    });
    await this.runControlCommand({
      label: "set-mode",
      args,
      cwd: state.cwd,
      fallbackCode: "ACP_TURN_FAILED",
    });
  }

  async setConfigOption(input: {
    handle: AcpRuntimeHandle;
    key: string;
    value: string;
  }): Promise<void> {
    const state = this.resolveHandleState(input.handle);
    const key = asTrimmedString(input.key);
    const value = asTrimmedString(input.value);
    if (!key || !value) {
      throw new AcpRuntimeError("ACP_TURN_FAILED", "ACP config option key/value are required.");
    }
    const args = await this.buildVerbArgs({
      agent: state.agent,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
      verb: "set",
      sessionName: state.name,
      command: ["set", key, value, "--session", state.name],
    });
    await this.runControlCommand({
      label: "set",
      args,
      cwd: state.cwd,
      fallbackCode: "ACP_TURN_FAILED",
    });
  }

  async doctor(): Promise<AcpRuntimeDoctorReport> {
    const versionCheck = await checkAcpxVersion({
      command: this.config.command,
      cwd: this.config.cwd,
      expectedVersion: this.config.expectedVersion,
      spawnOptions: this.spawnCommandOptions,
    });
    if (!versionCheck.ok) {
      this.healthy = false;
      const details = [
        versionCheck.expectedVersion ? `expected=${versionCheck.expectedVersion}` : null,
        versionCheck.installedVersion ? `installed=${versionCheck.installedVersion}` : null,
      ].filter((detail): detail is string => Boolean(detail));
      return {
        ok: false,
        code: "ACP_BACKEND_UNAVAILABLE",
        message: versionCheck.message,
        installCommand: versionCheck.installCommand,
        details,
      };
    }

    try {
      const result = await spawnAndCollect(
        {
          command: this.config.command,
          args: ["--help"],
          cwd: this.config.cwd,
        },
        this.spawnCommandOptions,
      );
      if (result.error) {
        const spawnFailure = resolveSpawnFailure(result.error, this.config.cwd);
        if (spawnFailure === "missing-command") {
          this.healthy = false;
          return {
            ok: false,
            code: "ACP_BACKEND_UNAVAILABLE",
            message: `acpx command not found: ${this.config.command}`,
            installCommand: this.config.installCommand,
          };
        }
        if (spawnFailure === "missing-cwd") {
          this.healthy = false;
          return {
            ok: false,
            code: "ACP_BACKEND_UNAVAILABLE",
            message: `ACP runtime working directory does not exist: ${this.config.cwd}`,
          };
        }
        this.healthy = false;
        return {
          ok: false,
          code: "ACP_BACKEND_UNAVAILABLE",
          message: result.error.message,
          details: [String(result.error)],
        };
      }
      if ((result.code ?? 0) !== 0) {
        this.healthy = false;
        return {
          ok: false,
          code: "ACP_BACKEND_UNAVAILABLE",
          message: result.stderr.trim() || `acpx exited with code ${result.code ?? "unknown"}`,
        };
      }
      this.healthy = true;
      return {
        ok: true,
        message: `acpx command available (${this.config.command}, version ${versionCheck.version}${this.config.expectedVersion ? `, expected ${this.config.expectedVersion}` : ""})`,
      };
    } catch (error) {
      this.healthy = false;
      return {
        ok: false,
        code: "ACP_BACKEND_UNAVAILABLE",
        message: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async cancel(input: { handle: AcpRuntimeHandle; reason?: string }): Promise<void> {
    const state = this.resolveHandleState(input.handle);
    const args = await this.buildVerbArgs({
      agent: state.agent,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
      verb: "cancel",
      sessionName: state.name,
      command: ["cancel", "--session", state.name],
    });
    await this.runControlCommand({
      label: "cancel",
      args,
      cwd: state.cwd,
      fallbackCode: "ACP_TURN_FAILED",
      ignoreNoSession: true,
    });
  }

  async close(input: { handle: AcpRuntimeHandle; reason: string }): Promise<void> {
    const state = this.resolveHandleState(input.handle);
    const args = await this.buildVerbArgs({
      agent: state.agent,
      cwd: state.cwd,
      codexBootstrap: state.codexBootstrap,
      verb: "sessions.close",
      sessionName: state.name,
      command: ["sessions", "close", state.name],
    });
    await this.runControlCommand({
      label: "sessions.close",
      args,
      cwd: state.cwd,
      fallbackCode: "ACP_TURN_FAILED",
      ignoreNoSession: true,
    });
  }

  private resolveHandleState(handle: AcpRuntimeHandle): AcpxHandleState {
    const decoded = decodeAcpxRuntimeHandleState(handle.runtimeSessionName);
    if (decoded) {
      this.debug(
        [
          "acpx runtime: handle-state decoded",
          `sessionKey=${handle.sessionKey}`,
          `session=${decoded.name}`,
          `agent=${decoded.agent}`,
          `mode=${decoded.mode}`,
          `backendSessionId=${this.formatDiagnosticValue(decoded.backendSessionId)}`,
          `agentSessionId=${this.formatDiagnosticValue(decoded.agentSessionId)}`,
          `bootstrapPersisted=${Boolean(decoded.codexBootstrap)}`,
          this.summarizeCodexBootstrap(decoded.codexBootstrap),
        ].join(" "),
      );
      return decoded;
    }

    const legacyName = asTrimmedString(handle.runtimeSessionName);
    if (!legacyName) {
      throw new AcpRuntimeError(
        "ACP_SESSION_INIT_FAILED",
        "Invalid acpx runtime handle: runtimeSessionName is missing.",
      );
    }

    this.debug(
      [
        "acpx runtime: handle-state legacy fallback",
        `sessionKey=${handle.sessionKey}`,
        `runtimeSessionName=${this.formatDiagnosticValue(legacyName)}`,
        `agent=${deriveAgentFromSessionKey(handle.sessionKey, DEFAULT_AGENT_FALLBACK)}`,
      ].join(" "),
    );

    return {
      name: legacyName,
      agent: deriveAgentFromSessionKey(handle.sessionKey, DEFAULT_AGENT_FALLBACK),
      cwd: this.config.cwd,
      mode: "persistent",
    };
  }

  private async buildPromptArgs(params: {
    agent: string;
    sessionName: string;
    cwd: string;
    codexBootstrap?: AcpxCodexBootstrapState;
  }): Promise<string[]> {
    const prefix = [
      "--format",
      "json",
      "--json-strict",
      "--cwd",
      params.cwd,
      ...buildPermissionArgs(this.config.permissionMode),
      "--non-interactive-permissions",
      this.config.nonInteractivePermissions,
    ];
    if (this.config.timeoutSeconds) {
      prefix.push("--timeout", String(this.config.timeoutSeconds));
    }
    prefix.push("--ttl", String(this.queueOwnerTtlSeconds));
    return await this.buildVerbArgs({
      agent: params.agent,
      cwd: params.cwd,
      codexBootstrap: params.codexBootstrap,
      verb: "prompt",
      sessionName: params.sessionName,
      command: ["prompt", "--session", params.sessionName, "--file", "-"],
      prefix,
    });
  }

  private async buildVerbArgs(params: {
    agent: string;
    cwd: string;
    command: string[];
    verb: string;
    sessionName?: string;
    prefix?: string[];
    codexBootstrap?: AcpxCodexBootstrapState;
  }): Promise<string[]> {
    const prefix = params.prefix ?? ["--format", "json", "--json-strict", "--cwd", params.cwd];
    const resolution = await this.resolveRawAgentCommandDetails({
      agent: params.agent,
      cwd: params.cwd,
      codexBootstrap: params.codexBootstrap,
    });
    this.logVerbRouting({
      verb: params.verb,
      agent: params.agent,
      cwd: params.cwd,
      sessionName: params.sessionName,
      codexBootstrap: params.codexBootstrap,
      resolution,
    });
    if (!resolution.rawAgentCommand) {
      return [...prefix, params.agent, ...params.command];
    }
    return [...prefix, "--agent", resolution.rawAgentCommand, ...params.command];
  }

  private async resolveRawAgentCommandDetails(params: {
    agent: string;
    cwd: string;
    codexBootstrap?: AcpxCodexBootstrapState;
  }): Promise<ResolvedAgentCommandDetails> {
    const wantsMcpProxy = Object.keys(this.config.mcpServers).length > 0;
    const wantsCodexBootstrap = Boolean(
      params.codexBootstrap?.model || params.codexBootstrap?.reasoningEffort,
    );
    if (!wantsMcpProxy && !wantsCodexBootstrap) {
      return {
        rawAgentCommand: null,
        cacheHit: false,
        source: "direct-agent",
        wrapper: "none",
      };
    }
    const cacheKey = [
      params.cwd,
      params.agent,
      params.codexBootstrap?.model ?? "",
      params.codexBootstrap?.reasoningEffort ?? "",
      wantsMcpProxy ? "mcp" : "nomcp",
    ].join("::");
    const cached = this.agentCommandCache.get(cacheKey);
    if (cached) {
      return {
        ...cached,
        cacheHit: true,
      };
    }
    const resolvedTarget = await resolveAcpxAgentCommandWithSource({
      acpxCommand: this.config.command,
      cwd: params.cwd,
      agent: params.agent,
      spawnOptions: this.spawnCommandOptions,
    });
    let resolvedCommand = resolvedTarget.command;
    let wrapper: CachedAgentCommandDetails["wrapper"] = "none";
    if (wantsCodexBootstrap && params.codexBootstrap) {
      resolvedCommand = buildCodexBootstrapAgentCommand({
        targetCommand: resolvedCommand,
        bootstrap: params.codexBootstrap,
      });
      wrapper = resolvedCommand.includes(ACPX_CODEX_BOOTSTRAP_WRAPPER_PATH_HINT)
        ? "codex-bootstrap"
        : "none";
    }
    if (wantsMcpProxy) {
      resolvedCommand = buildMcpProxyAgentCommand({
        targetCommand: resolvedCommand,
        mcpServers: toAcpMcpServers(this.config.mcpServers),
      });
      wrapper = resolvedCommand.includes(ACPX_MCP_PROXY_PATH_HINT)
        ? wrapper === "codex-bootstrap"
          ? "codex-bootstrap+mcp-proxy"
          : "mcp-proxy"
        : wrapper;
    }
    const details: CachedAgentCommandDetails = {
      rawAgentCommand: resolvedCommand,
      source: resolvedTarget.source,
      targetCommand: resolvedTarget.command,
      wrapper,
    };
    this.agentCommandCache.set(cacheKey, details);
    return {
      ...details,
      cacheHit: false,
    };
  }

  private async runControlCommand(params: {
    label: string;
    args: string[];
    cwd: string;
    fallbackCode: AcpRuntimeErrorCode;
    ignoreNoSession?: boolean;
    signal?: AbortSignal;
  }): Promise<AcpxJsonObject[]> {
    this.debug(
      [
        "acpx runtime: control spawn",
        `label=${params.label}`,
        `cwd=${this.formatDiagnosticValue(params.cwd)}`,
        `argv=${this.summarizeCommandLine(params.args.join(" "))}`,
      ].join(" "),
    );
    const result = await spawnAndCollect(
      {
        command: this.config.command,
        args: params.args,
        cwd: params.cwd,
      },
      this.spawnCommandOptions,
      {
        signal: params.signal,
      },
    );

    if (result.error) {
      const spawnFailure = resolveSpawnFailure(result.error, params.cwd);
      this.debug(
        `acpx runtime: control spawn error label=${params.label} error=${result.error.message}`,
      );
      if (spawnFailure === "missing-command") {
        this.healthy = false;
        throw new AcpRuntimeError(
          "ACP_BACKEND_UNAVAILABLE",
          `acpx command not found: ${this.config.command}`,
          { cause: result.error },
        );
      }
      if (spawnFailure === "missing-cwd") {
        throw new AcpRuntimeError(
          params.fallbackCode,
          `ACP runtime working directory does not exist: ${params.cwd}`,
          { cause: result.error },
        );
      }
      throw new AcpRuntimeError(params.fallbackCode, result.error.message, { cause: result.error });
    }

    const events = parseJsonLines(result.stdout);
    this.debug(
      [
        "acpx runtime: control completed",
        `label=${params.label}`,
        `code=${result.code ?? "null"}`,
        `eventCount=${events.length}`,
        `stderr=${this.summarizeCommandLine(result.stderr)}`,
      ].join(" "),
    );
    const errorEvent = events.map((event) => toAcpxErrorEvent(event)).find(Boolean) ?? null;
    if (errorEvent) {
      this.debug(
        [
          "acpx runtime: control error event",
          `label=${params.label}`,
          `code=${this.formatDiagnosticValue(errorEvent.code)}`,
          `message=${this.summarizeCommandLine(errorEvent.message)}`,
          `ignoreNoSession=${params.ignoreNoSession === true}`,
        ].join(" "),
      );
      if (params.ignoreNoSession && errorEvent.code === "NO_SESSION") {
        return events;
      }
      throw new AcpRuntimeError(
        params.fallbackCode,
        errorEvent.code ? `${errorEvent.code}: ${errorEvent.message}` : errorEvent.message,
      );
    }

    if ((result.code ?? 0) !== 0) {
      this.debug(
        [
          "acpx runtime: control nonzero exit",
          `label=${params.label}`,
          `code=${result.code ?? "null"}`,
          `stderr=${this.summarizeCommandLine(result.stderr)}`,
        ].join(" "),
      );
      throw new AcpRuntimeError(
        params.fallbackCode,
        formatAcpxExitMessage({
          stderr: result.stderr,
          exitCode: result.code,
        }),
      );
    }
    return events;
  }
}
