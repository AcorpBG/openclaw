#!/usr/bin/env node

import { spawn } from "node:child_process";

const LOG_PREFIX = "[acpx-codex-wrapper]";

function log(message) {
  try {
    process.stderr.write(`${LOG_PREFIX} ${message}\n`);
  } catch {
    // Best-effort diagnostics only.
  }
}

function splitCommandLine(value) {
  const parts = [];
  let current = "";
  let quote = null;
  let escaping = false;

  for (const ch of value) {
    if (escaping) {
      current += ch;
      escaping = false;
      continue;
    }
    if (ch === "\\" && quote !== "'") {
      escaping = true;
      continue;
    }
    if (quote) {
      if (ch === quote) {
        quote = null;
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current.length > 0) {
        parts.push(current);
        current = "";
      }
      continue;
    }
    current += ch;
  }

  if (escaping) {
    current += "\\";
  }
  if (quote) {
    throw new Error("Invalid agent command: unterminated quote");
  }
  if (current.length > 0) {
    parts.push(current);
  }
  if (parts.length === 0) {
    throw new Error("Invalid agent command: empty command");
  }
  return {
    command: parts[0],
    args: parts.slice(1),
  };
}

function decodePayload(argv) {
  const payloadIndex = argv.indexOf("--payload");
  if (payloadIndex < 0) {
    throw new Error("Missing --payload");
  }
  const encoded = argv[payloadIndex + 1];
  if (!encoded) {
    throw new Error("Missing Codex bootstrap payload value");
  }
  const parsed = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Invalid Codex bootstrap payload");
  }
  if (typeof parsed.targetCommand !== "string" || parsed.targetCommand.trim() === "") {
    throw new Error("Codex bootstrap payload missing targetCommand");
  }
  const bootstrap =
    parsed.bootstrap && typeof parsed.bootstrap === "object" && !Array.isArray(parsed.bootstrap)
      ? parsed.bootstrap
      : {};
  const model =
    typeof bootstrap.model === "string" && bootstrap.model.trim() ? bootstrap.model : "";
  const reasoningEffort =
    typeof bootstrap.reasoningEffort === "string" && bootstrap.reasoningEffort.trim()
      ? bootstrap.reasoningEffort
      : "";
  return {
    targetCommand: parsed.targetCommand,
    model: model || undefined,
    reasoningEffort: reasoningEffort || undefined,
  };
}

function buildCliOverrides(params) {
  const args = [];
  if (params.model) {
    args.push("-c", `model=${JSON.stringify(params.model)}`);
  }
  if (params.reasoningEffort) {
    args.push("-c", `model_reasoning_effort=${JSON.stringify(params.reasoningEffort)}`);
  }
  return args;
}

function quoteCommandPart(value) {
  if (value === "") {
    return '""';
  }
  if (/^[A-Za-z0-9_./:@%+=,-]+$/.test(value)) {
    return value;
  }
  return `"${value.replace(/["\\]/g, "\\$&")}"`;
}

function formatCommandLine(command, args) {
  return [command, ...args].map(quoteCommandPart).join(" ");
}

const payload = decodePayload(process.argv.slice(2));
const target = splitCommandLine(payload.targetCommand);
const cliOverrides = buildCliOverrides(payload);
log(
  `decoded payload targetCommand=${formatCommandLine(target.command, target.args)} model=${payload.model || "-"} reasoning=${payload.reasoningEffort || "-"} overrides=${JSON.stringify(cliOverrides)}`,
);
const childArgs = [...target.args, ...cliOverrides];
const child = spawn(target.command, childArgs, {
  stdio: ["pipe", "pipe", "inherit"],
  env: process.env,
});
log(
  `spawn child command=${formatCommandLine(target.command, childArgs)} pid=${child.pid ?? "unknown"}`,
);

if (!child.stdin || !child.stdout) {
  throw new Error("Failed to create Codex bootstrap wrapper stdio pipes");
}

log("pipe stdin->child.stdin start");
process.stdin.on("end", () => {
  log("parent stdin end");
});
process.stdin.on("close", () => {
  log("parent stdin close");
});
process.stdin.on("error", (error) => {
  log(`parent stdin error=${error instanceof Error ? error.message : String(error)}`);
});
process.stdin.pipe(child.stdin);
child.stdin.on("error", () => {
  // Ignore EPIPE when the child exits before stdin flush completes.
  log("child stdin error=EPIPE-or-exit-race");
});
child.stdin.on("close", () => {
  log("child stdin close");
});
log("pipe child.stdout->stdout start");
child.stdout.pipe(process.stdout);
child.stdout.on("end", () => {
  log("child stdout end");
});
child.stdout.on("close", () => {
  log("child stdout close");
});
child.stdout.on("error", (error) => {
  log(`child stdout error=${error instanceof Error ? error.message : String(error)}`);
});

child.on("error", (error) => {
  log(`child error=${error instanceof Error ? error.message : String(error)}`);
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  log(`child exit code=${code ?? "null"} signal=${signal ?? "null"}`);
});

child.on("close", (code, signal) => {
  log(`child close code=${code ?? "null"} signal=${signal ?? "null"}`);
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});

process.on("exit", (code) => {
  log(`wrapper exit code=${code}`);
});
