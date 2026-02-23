import { describe, expect, it } from "vitest";
import {
  connectOk,
  installGatewayTestHooks,
  readConnectChallengeNonce,
  rpcReq,
} from "./test-helpers.js";
import { withServer } from "./test-with-server.js";

installGatewayTestHooks({ scope: "suite" });

type GatewaySocket = Parameters<Parameters<typeof withServer>[0]>[0];

async function createFreshOperatorDevice(scopes: string[], nonce: string) {
  const { randomUUID } = await import("node:crypto");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { buildDeviceAuthPayload } = await import("./device-auth.js");
  const { loadOrCreateDeviceIdentity, publicKeyRawBase64UrlFromPem, signDevicePayload } =
    await import("../infra/device-identity.js");

  const identity = loadOrCreateDeviceIdentity(
    join(tmpdir(), `openclaw-tts-rpc-${randomUUID()}.json`),
  );
  const signedAtMs = Date.now();
  const payload = buildDeviceAuthPayload({
    deviceId: identity.deviceId,
    clientId: "test",
    clientMode: "test",
    role: "operator",
    scopes,
    signedAtMs,
    token: "secret",
    nonce,
  });

  return {
    id: identity.deviceId,
    publicKey: publicKeyRawBase64UrlFromPem(identity.publicKeyPem),
    signature: signDevicePayload(identity.privateKeyPem, payload),
    signedAt: signedAtMs,
    nonce,
  };
}

async function connectOperator(ws: GatewaySocket, scopes: string[]) {
  const nonce = await readConnectChallengeNonce(ws);
  expect(nonce).toBeTruthy();
  await connectOk(ws, {
    token: "secret",
    scopes,
    device: await createFreshOperatorDevice(scopes, String(nonce)),
  });
}

describe("gateway tts rpc", () => {
  it("returns qwen metadata in tts.status", async () => {
    const { writeConfigFile } = await import("../config/config.js");
    await writeConfigFile({
      messages: {
        tts: {
          provider: "qwen3-fastapi",
          qwen3Fastapi: {
            apiKey: "qwen-key",
            baseUrl: "http://127.0.0.1:8000/v1",
            model: "qwen3-tts",
            voice: "Chelsie",
          },
        },
      },
    });

    await withServer(async (ws) => {
      await connectOperator(ws, ["operator.read", "operator.write"]);
      const res = await rpcReq<{
        provider?: string;
        hasQwen3FastapiKey?: boolean;
        qwen3FastapiConfigured?: boolean;
      }>(ws, "tts.status", {});
      expect(res.ok).toBe(true);
      expect(res.payload?.provider).toBe("qwen3-fastapi");
      expect(res.payload?.hasQwen3FastapiKey).toBe(true);
      expect(res.payload?.qwen3FastapiConfigured).toBe(true);
    });
  });

  it("accepts qwen3-fastapi in tts.setProvider and lists it in tts.providers", async () => {
    const { writeConfigFile } = await import("../config/config.js");
    await writeConfigFile({
      messages: {
        tts: {
          qwen3Fastapi: {
            baseUrl: "http://127.0.0.1:8000/v1",
            model: "qwen3-tts",
            voice: "Chelsie",
          },
        },
      },
    });

    await withServer(async (ws) => {
      await connectOperator(ws, ["operator.read", "operator.write"]);

      const setProviderRes = await rpcReq<{ provider?: string }>(ws, "tts.setProvider", {
        provider: "qwen3-fastapi",
      });
      expect(setProviderRes.ok).toBe(true);
      expect(setProviderRes.payload?.provider).toBe("qwen3-fastapi");

      const providersRes = await rpcReq<{
        providers?: Array<{ id?: string; configured?: boolean }>;
      }>(ws, "tts.providers", {});
      expect(providersRes.ok).toBe(true);
      expect(providersRes.payload?.providers?.some((entry) => entry.id === "qwen3-fastapi")).toBe(
        true,
      );
    });
  });
});
