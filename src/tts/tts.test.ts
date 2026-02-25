import { readFileSync } from "node:fs";
import { completeSimple, type AssistantMessage } from "@mariozechner/pi-ai";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getApiKeyForModel } from "../agents/model-auth.js";
import { resolveModel } from "../agents/pi-embedded-runner/model.js";
import type { OpenClawConfig } from "../config/config.js";
import { withEnv } from "../test-utils/env.js";
import * as tts from "./tts.js";

vi.mock("@mariozechner/pi-ai", () => ({
  completeSimple: vi.fn(),
  // Some auth helpers import oauth provider metadata at module load time.
  getOAuthProviders: () => [],
  getOAuthApiKey: vi.fn(async () => null),
}));

vi.mock("../agents/pi-embedded-runner/model.js", () => ({
  resolveModel: vi.fn((provider: string, modelId: string) => ({
    model: {
      provider,
      id: modelId,
      name: modelId,
      api: "openai-completions",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 128000,
      maxTokens: 8192,
    },
    authStorage: { profiles: {} },
    modelRegistry: { find: vi.fn() },
  })),
}));

vi.mock("../agents/model-auth.js", () => ({
  getApiKeyForModel: vi.fn(async () => ({
    apiKey: "test-api-key",
    source: "test",
    mode: "api-key",
  })),
  requireApiKey: vi.fn((auth: { apiKey?: string }) => auth.apiKey ?? ""),
}));

const { _test, resolveTtsConfig, maybeApplyTtsToPayload, getTtsProvider, textToSpeechStream } = tts;

const {
  isValidVoiceId,
  isValidOpenAIVoice,
  isValidOpenAIModel,
  OPENAI_TTS_MODELS,
  OPENAI_TTS_VOICES,
  parseTtsDirectives,
  resolveModelOverridePolicy,
  summarizeText,
  resolveOutputFormat,
  resolveEdgeOutputFormat,
} = _test;

const mockAssistantMessage = (content: AssistantMessage["content"]): AssistantMessage => ({
  role: "assistant",
  content,
  api: "openai-completions",
  provider: "openai",
  model: "gpt-4o-mini",
  usage: {
    input: 1,
    output: 1,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 2,
    cost: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      total: 0,
    },
  },
  stopReason: "stop",
  timestamp: Date.now(),
});

function getFetchRequestBody(fetchMock: { mock: { calls: unknown[][] } }, callIndex: number) {
  const call = fetchMock.mock.calls[callIndex] as [unknown, RequestInit | undefined] | undefined;
  const init = call?.[1];
  const body = init?.body;
  if (typeof body !== "string" || !body.trim()) {
    return {};
  }
  return JSON.parse(body) as Record<string, unknown>;
}

describe("tts", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(completeSimple).mockResolvedValue(
      mockAssistantMessage([{ type: "text", text: "Summary" }]),
    );
  });

  describe("isValidVoiceId", () => {
    it("accepts valid ElevenLabs voice IDs", () => {
      expect(isValidVoiceId("pMsXgVXv3BLzUgSXRplE")).toBe(true);
      expect(isValidVoiceId("21m00Tcm4TlvDq8ikWAM")).toBe(true);
      expect(isValidVoiceId("EXAVITQu4vr4xnSDxMaL")).toBe(true);
    });

    it("accepts voice IDs of varying valid lengths", () => {
      expect(isValidVoiceId("a1b2c3d4e5")).toBe(true);
      expect(isValidVoiceId("a".repeat(40))).toBe(true);
    });

    it("rejects too short voice IDs", () => {
      expect(isValidVoiceId("")).toBe(false);
      expect(isValidVoiceId("abc")).toBe(false);
      expect(isValidVoiceId("123456789")).toBe(false);
    });

    it("rejects too long voice IDs", () => {
      expect(isValidVoiceId("a".repeat(41))).toBe(false);
      expect(isValidVoiceId("a".repeat(100))).toBe(false);
    });

    it("rejects voice IDs with invalid characters", () => {
      expect(isValidVoiceId("pMsXgVXv3BLz-gSXRplE")).toBe(false);
      expect(isValidVoiceId("pMsXgVXv3BLz_gSXRplE")).toBe(false);
      expect(isValidVoiceId("pMsXgVXv3BLz gSXRplE")).toBe(false);
      expect(isValidVoiceId("../../../etc/passwd")).toBe(false);
      expect(isValidVoiceId("voice?param=value")).toBe(false);
    });
  });

  describe("isValidOpenAIVoice", () => {
    it("accepts all valid OpenAI voices", () => {
      for (const voice of OPENAI_TTS_VOICES) {
        expect(isValidOpenAIVoice(voice)).toBe(true);
      }
    });

    it("includes newer OpenAI voices (ballad, cedar, juniper, marin, verse) (#2393)", () => {
      expect(isValidOpenAIVoice("ballad")).toBe(true);
      expect(isValidOpenAIVoice("cedar")).toBe(true);
      expect(isValidOpenAIVoice("juniper")).toBe(true);
      expect(isValidOpenAIVoice("marin")).toBe(true);
      expect(isValidOpenAIVoice("verse")).toBe(true);
    });

    it("rejects invalid voice names", () => {
      withEnv({ OPENAI_TTS_BASE_URL: undefined }, () => {
        expect(isValidOpenAIVoice("invalid")).toBe(false);
        expect(isValidOpenAIVoice("")).toBe(false);
        expect(isValidOpenAIVoice("ALLOY")).toBe(false);
        expect(isValidOpenAIVoice("alloy ")).toBe(false);
        expect(isValidOpenAIVoice(" alloy")).toBe(false);
      });
    });
  });

  describe("isValidOpenAIModel", () => {
    it("accepts supported models", () => {
      expect(isValidOpenAIModel("gpt-4o-mini-tts")).toBe(true);
      expect(isValidOpenAIModel("tts-1")).toBe(true);
      expect(isValidOpenAIModel("tts-1-hd")).toBe(true);
    });

    it("rejects unsupported models", () => {
      withEnv({ OPENAI_TTS_BASE_URL: undefined }, () => {
        expect(isValidOpenAIModel("invalid")).toBe(false);
        expect(isValidOpenAIModel("")).toBe(false);
        expect(isValidOpenAIModel("gpt-4")).toBe(false);
      });
    });
  });

  describe("OPENAI_TTS_MODELS", () => {
    it("contains supported models", () => {
      expect(OPENAI_TTS_MODELS).toContain("gpt-4o-mini-tts");
      expect(OPENAI_TTS_MODELS).toContain("tts-1");
      expect(OPENAI_TTS_MODELS).toContain("tts-1-hd");
      expect(OPENAI_TTS_MODELS).toHaveLength(3);
    });

    it("is a non-empty array", () => {
      expect(Array.isArray(OPENAI_TTS_MODELS)).toBe(true);
      expect(OPENAI_TTS_MODELS.length).toBeGreaterThan(0);
    });
  });

  describe("resolveOutputFormat", () => {
    it("uses Opus for Telegram", () => {
      const output = resolveOutputFormat("telegram");
      expect(output.openai).toBe("opus");
      expect(output.elevenlabs).toBe("opus_48000_64");
      expect(output.extension).toBe(".opus");
      expect(output.voiceCompatible).toBe(true);
    });

    it("uses MP3 for other channels", () => {
      const output = resolveOutputFormat("discord");
      expect(output.openai).toBe("mp3");
      expect(output.elevenlabs).toBe("mp3_44100_128");
      expect(output.extension).toBe(".mp3");
      expect(output.voiceCompatible).toBe(false);
    });
  });

  describe("resolveEdgeOutputFormat", () => {
    const baseCfg: OpenClawConfig = {
      agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
      messages: { tts: {} },
    };

    it("uses default output format when edge output format is not configured", () => {
      const config = resolveTtsConfig(baseCfg);
      expect(resolveEdgeOutputFormat(config)).toBe("audio-24khz-48kbitrate-mono-mp3");
    });

    it("uses configured output format when provided", () => {
      const config = resolveTtsConfig({
        ...baseCfg,
        messages: {
          tts: {
            edge: { outputFormat: "audio-24khz-96kbitrate-mono-mp3" },
          },
        },
      });
      expect(resolveEdgeOutputFormat(config)).toBe("audio-24khz-96kbitrate-mono-mp3");
    });
  });

  describe("parseTtsDirectives", () => {
    it("extracts overrides and strips directives when enabled", () => {
      const policy = resolveModelOverridePolicy({ enabled: true, allowProvider: true });
      const input =
        "Hello [[tts:provider=elevenlabs voiceId=pMsXgVXv3BLzUgSXRplE stability=0.4 speed=1.1]] world\n\n" +
        "[[tts:text]](laughs) Read the song once more.[[/tts:text]]";
      const result = parseTtsDirectives(input, policy);

      expect(result.cleanedText).not.toContain("[[tts:");
      expect(result.ttsText).toBe("(laughs) Read the song once more.");
      expect(result.overrides.provider).toBe("elevenlabs");
      expect(result.overrides.elevenlabs?.voiceId).toBe("pMsXgVXv3BLzUgSXRplE");
      expect(result.overrides.elevenlabs?.voiceSettings?.stability).toBe(0.4);
      expect(result.overrides.elevenlabs?.voiceSettings?.speed).toBe(1.1);
    });

    it("accepts edge as provider override", () => {
      const policy = resolveModelOverridePolicy({ enabled: true, allowProvider: true });
      const input = "Hello [[tts:provider=edge]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.provider).toBe("edge");
    });

    it("accepts qwen3-fastapi as provider override", () => {
      const policy = resolveModelOverridePolicy({ enabled: true, allowProvider: true });
      const input =
        "Hello [[tts:provider=qwen3-fastapi voice=Chelsie model=Qwen/Qwen3-TTS-0.6B]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.provider).toBe("qwen3-fastapi");
      expect(result.overrides.qwen3Fastapi?.voice).toBe("Chelsie");
      expect(result.overrides.qwen3Fastapi?.model).toBe("Qwen/Qwen3-TTS-0.6B");
    });

    it("applies language to qwen3-fastapi overrides when provider is qwen3-fastapi", () => {
      const policy = resolveModelOverridePolicy({ enabled: true, allowProvider: true });
      const input = "Hello [[tts:provider=qwen3-fastapi language=EN]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.qwen3Fastapi?.language).toBe("english");
      expect(result.overrides.elevenlabs?.languageCode).toBeUndefined();
    });

    it("rejects provider override by default while keeping voice overrides enabled", () => {
      const policy = resolveModelOverridePolicy({ enabled: true });
      const input = "Hello [[tts:provider=edge voice=alloy]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.provider).toBeUndefined();
      expect(result.overrides.openai?.voice).toBe("alloy");
    });

    it("keeps text intact when overrides are disabled", () => {
      const policy = resolveModelOverridePolicy({ enabled: false });
      const input = "Hello [[tts:voice=alloy]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.cleanedText).toBe(input);
      expect(result.overrides.provider).toBeUndefined();
    });

    it("parses qwen3-fastapi instructions and stream overrides", () => {
      const policy = resolveModelOverridePolicy({ enabled: true });
      const input = "Hello [[tts:instruct=dramatic stream=on]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.qwen3Fastapi?.instructions).toBe("dramatic");
      expect(result.overrides.qwen3Fastapi?.stream).toBe(true);
    });

    it("blocks instruction and stream overrides when disabled in policy", () => {
      const policy = resolveModelOverridePolicy({
        enabled: true,
        allowInstructions: false,
        allowStream: false,
      });
      const input = "Hello [[tts:instructions=calm stream=true]] world";
      const result = parseTtsDirectives(input, policy);

      expect(result.overrides.qwen3Fastapi?.instructions).toBeUndefined();
      expect(result.overrides.qwen3Fastapi?.stream).toBeUndefined();
    });
  });

  describe("openaiTTS", () => {
    const originalFetch = globalThis.fetch;

    afterEach(() => {
      globalThis.fetch = originalFetch;
    });

    it("sends instructions and stream=true in the first request", async () => {
      const fetchMock = vi
        .fn(async () => ({
          ok: true,
          arrayBuffer: async () => new ArrayBuffer(1),
        }))
        .mockName("fetch");
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      await _test.openaiTTS({
        text: "hello",
        apiKey: "k",
        model: "gpt-4o-mini-tts",
        voice: "alloy",
        instructions: "calm",
        stream: true,
        responseFormat: "mp3",
        timeoutMs: 10_000,
      });

      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.instructions).toBe("calm");
      expect(body.stream).toBe(true);
    });

    it("falls back to non-stream request when stream mode fails", async () => {
      const fetchMock = vi
        .fn()
        .mockResolvedValueOnce({ ok: false, status: 400 })
        .mockResolvedValueOnce({
          ok: true,
          arrayBuffer: async () => new ArrayBuffer(1),
        });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      await _test.openaiTTS({
        text: "hello",
        apiKey: "k",
        model: "gpt-4o-mini-tts",
        voice: "alloy",
        stream: true,
        responseFormat: "mp3",
        timeoutMs: 10_000,
      });

      expect(fetchMock).toHaveBeenCalledTimes(2);
      const firstBody = getFetchRequestBody(
        fetchMock as unknown as { mock: { calls: unknown[][] } },
        0,
      );
      const secondBody = getFetchRequestBody(
        fetchMock as unknown as { mock: { calls: unknown[][] } },
        1,
      );
      expect(firstBody.stream).toBe(true);
      expect(secondBody.stream).toBeUndefined();
    });
  });

  describe("textToSpeechStream", () => {
    const originalFetch = globalThis.fetch;

    afterEach(() => {
      globalThis.fetch = originalFetch;
    });

    it("rejects OpenAI streaming path (qwen3-fastapi-only enhancement)", async () => {
      const fetchMock = vi.fn();
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "openai",
            openai: {
              apiKey: "test-key",
              model: "gpt-4o-mini-tts",
              voice: "alloy",
            },
          },
        },
      };

      const result = await textToSpeechStream({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(false);
      expect(result.error).toContain("streaming disabled");
      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("supports qwen3-fastapi streaming with instructions", async () => {
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        body: stream,
      });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "qwen3-fastapi",
            qwen3Fastapi: {
              baseUrl: "http://127.0.0.1:8000/v1",
              model: "Qwen/Qwen3-TTS-0.6B",
              voice: "Chelsie",
              instructions: "narrator",
              stream: true,
            },
          },
        },
      };

      const result = await textToSpeechStream({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(true);
      expect(result.provider).toBe("qwen3-fastapi");
      expect(result.progressive).toBe(true);
      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.instructions).toBe("narrator");
      expect(body.stream).toBe(true);
      expect(body.response_format).toBe("pcm");
      expect(result.outputFormat).toBe("pcm");
    });
  });

  describe("textToSpeech", () => {
    const originalFetch = globalThis.fetch;

    afterEach(() => {
      globalThis.fetch = originalFetch;
    });

    it("keeps OpenAI file output baseline as mp3", async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => new Uint8Array([1, 2, 3, 4]).buffer,
      });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "openai",
            openai: {
              apiKey: "test-key",
              model: "gpt-4o-mini-tts",
              voice: "alloy",
            },
          },
        },
      };

      const result = await tts.textToSpeech({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(true);
      expect(result.provider).toBe("openai");
      expect(result.outputFormat).toBe("mp3");
      expect(result.audioPath?.endsWith(".mp3")).toBe(true);
      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.stream).toBeUndefined();
      expect(body.response_format).toBe("mp3");
    });

    it("uses qwen3-fastapi configured non-stream responseFormat/language", async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => new Uint8Array([1, 2, 3, 4]).buffer,
      });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "qwen3-fastapi",
            qwen3Fastapi: {
              baseUrl: "http://127.0.0.1:8000/v1",
              model: "Qwen/Qwen3-TTS-0.6B",
              voice: "Chelsie",
              responseFormat: "opus",
              language: "english",
              stream: false,
            },
          },
        },
      };

      const result = await tts.textToSpeech({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(true);
      expect(result.provider).toBe("qwen3-fastapi");
      expect(result.outputFormat).toBe("opus");
      expect(result.audioPath?.endsWith(".opus")).toBe(true);
      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.stream).toBeUndefined();
      expect(body.response_format).toBe("opus");
      expect(body.language).toBe("english");
    });

    it("accepts legacy qwen3Fastapi.languageCode and emits language in request", async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => new Uint8Array([1, 2, 3, 4]).buffer,
      });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "qwen3-fastapi",
            qwen3Fastapi: {
              baseUrl: "http://127.0.0.1:8000/v1",
              model: "Qwen/Qwen3-TTS-0.6B",
              voice: "Chelsie",
              responseFormat: "opus",
              languageCode: "en",
              stream: false,
            },
          },
        },
      };

      const result = await tts.textToSpeech({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(true);
      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.language).toBe("english");
    });

    it("wraps qwen3-fastapi stream PCM output into WAV for file output", async () => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => new Uint8Array([1, 2, 3, 4]).buffer,
      });
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
        messages: {
          tts: {
            provider: "qwen3-fastapi",
            qwen3Fastapi: {
              baseUrl: "http://127.0.0.1:8000/v1",
              model: "Qwen/Qwen3-TTS-0.6B",
              voice: "Chelsie",
              stream: true,
            },
          },
        },
      };

      const result = await tts.textToSpeech({
        text: "hello",
        cfg,
      });

      expect(result.success).toBe(true);
      expect(result.provider).toBe("qwen3-fastapi");
      expect(result.outputFormat).toBe("wav");
      expect(result.audioPath?.endsWith(".wav")).toBe(true);
      const body = getFetchRequestBody(fetchMock as unknown as { mock: { calls: unknown[][] } }, 0);
      expect(body.stream).toBe(true);
      expect(body.response_format).toBe("pcm");
      const wavBytes = readFileSync(result.audioPath!);
      expect(wavBytes.subarray(0, 4).toString("ascii")).toBe("RIFF");
      expect(wavBytes.subarray(8, 12).toString("ascii")).toBe("WAVE");
    });
  });

  describe("summarizeText", () => {
    const baseCfg: OpenClawConfig = {
      agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
      messages: { tts: {} },
    };
    const baseConfig = resolveTtsConfig(baseCfg);

    it("summarizes text and returns result with metrics", async () => {
      const mockSummary = "This is a summarized version of the text.";
      vi.mocked(completeSimple).mockResolvedValue(
        mockAssistantMessage([{ type: "text", text: mockSummary }]),
      );

      const longText = "A".repeat(2000);
      const result = await summarizeText({
        text: longText,
        targetLength: 1500,
        cfg: baseCfg,
        config: baseConfig,
        timeoutMs: 30_000,
      });

      expect(result.summary).toBe(mockSummary);
      expect(result.inputLength).toBe(2000);
      expect(result.outputLength).toBe(mockSummary.length);
      expect(result.latencyMs).toBeGreaterThanOrEqual(0);
      expect(completeSimple).toHaveBeenCalledTimes(1);
    });

    it("calls the summary model with the expected parameters", async () => {
      await summarizeText({
        text: "Long text to summarize",
        targetLength: 500,
        cfg: baseCfg,
        config: baseConfig,
        timeoutMs: 30_000,
      });

      const callArgs = vi.mocked(completeSimple).mock.calls[0];
      expect(callArgs?.[1]?.messages?.[0]?.role).toBe("user");
      expect(callArgs?.[2]?.maxTokens).toBe(250);
      expect(callArgs?.[2]?.temperature).toBe(0.3);
      expect(getApiKeyForModel).toHaveBeenCalledTimes(1);
    });

    it("uses summaryModel override when configured", async () => {
      const cfg: OpenClawConfig = {
        agents: { defaults: { model: { primary: "anthropic/claude-opus-4-5" } } },
        messages: { tts: { summaryModel: "openai/gpt-4.1-mini" } },
      };
      const config = resolveTtsConfig(cfg);
      await summarizeText({
        text: "Long text to summarize",
        targetLength: 500,
        cfg,
        config,
        timeoutMs: 30_000,
      });

      expect(resolveModel).toHaveBeenCalledWith("openai", "gpt-4.1-mini", undefined, cfg);
    });

    it("rejects targetLength below minimum (100)", async () => {
      await expect(
        summarizeText({
          text: "text",
          targetLength: 99,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).rejects.toThrow("Invalid targetLength: 99");
    });

    it("rejects targetLength above maximum (10000)", async () => {
      await expect(
        summarizeText({
          text: "text",
          targetLength: 10001,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).rejects.toThrow("Invalid targetLength: 10001");
    });

    it("accepts targetLength at boundaries", async () => {
      await expect(
        summarizeText({
          text: "text",
          targetLength: 100,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).resolves.toBeDefined();
      await expect(
        summarizeText({
          text: "text",
          targetLength: 10000,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).resolves.toBeDefined();
    });

    it("throws error when no summary is returned", async () => {
      vi.mocked(completeSimple).mockResolvedValue(mockAssistantMessage([]));

      await expect(
        summarizeText({
          text: "text",
          targetLength: 500,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).rejects.toThrow("No summary returned");
    });

    it("throws error when summary content is empty", async () => {
      vi.mocked(completeSimple).mockResolvedValue(
        mockAssistantMessage([{ type: "text", text: "   " }]),
      );

      await expect(
        summarizeText({
          text: "text",
          targetLength: 500,
          cfg: baseCfg,
          config: baseConfig,
          timeoutMs: 30_000,
        }),
      ).rejects.toThrow("No summary returned");
    });
  });

  describe("getTtsProvider", () => {
    const baseCfg: OpenClawConfig = {
      agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
      messages: { tts: {} },
    };

    it("prefers OpenAI when qwen3-fastapi is not configured and OpenAI key exists", () => {
      withEnv(
        {
          OPENAI_API_KEY: "test-openai-key",
          ELEVENLABS_API_KEY: undefined,
          XI_API_KEY: undefined,
        },
        () => {
          const config = resolveTtsConfig(baseCfg);
          const provider = getTtsProvider(config, "/tmp/tts-prefs-openai.json");
          expect(provider).toBe("openai");
        },
      );
    });

    it("prefers ElevenLabs when OpenAI is missing and ElevenLabs key exists", () => {
      withEnv(
        {
          OPENAI_API_KEY: undefined,
          ELEVENLABS_API_KEY: "test-elevenlabs-key",
          XI_API_KEY: undefined,
        },
        () => {
          const config = resolveTtsConfig(baseCfg);
          const provider = getTtsProvider(config, "/tmp/tts-prefs-elevenlabs.json");
          expect(provider).toBe("elevenlabs");
        },
      );
    });

    it("prefers qwen3-fastapi when configured", () => {
      withEnv(
        {
          OPENAI_API_KEY: "test-openai-key",
          ELEVENLABS_API_KEY: undefined,
          XI_API_KEY: undefined,
          QWEN3_FASTAPI_BASE_URL: "http://127.0.0.1:8000/v1",
        },
        () => {
          const config = resolveTtsConfig({
            ...baseCfg,
            messages: {
              tts: {
                qwen3Fastapi: {
                  model: "Qwen/Qwen3-TTS-0.6B",
                  voice: "Chelsie",
                },
              },
            },
          });
          const provider = getTtsProvider(config, "/tmp/tts-prefs-qwen.json");
          expect(provider).toBe("qwen3-fastapi");
        },
      );
    });

    it("falls back to Edge when no API keys are present", () => {
      withEnv(
        {
          OPENAI_API_KEY: undefined,
          ELEVENLABS_API_KEY: undefined,
          XI_API_KEY: undefined,
        },
        () => {
          const config = resolveTtsConfig(baseCfg);
          const provider = getTtsProvider(config, "/tmp/tts-prefs-edge.json");
          expect(provider).toBe("edge");
        },
      );
    });
  });

  describe("maybeApplyTtsToPayload", () => {
    const baseCfg: OpenClawConfig = {
      agents: { defaults: { model: { primary: "openai/gpt-4o-mini" } } },
      messages: {
        tts: {
          auto: "inbound",
          provider: "openai",
          openai: { apiKey: "test-key", model: "gpt-4o-mini-tts", voice: "alloy" },
        },
      },
    };

    const withMockedAutoTtsFetch = async (
      run: (fetchMock: ReturnType<typeof vi.fn>) => Promise<void>,
    ) => {
      const prevPrefs = process.env.OPENCLAW_TTS_PREFS;
      process.env.OPENCLAW_TTS_PREFS = `/tmp/tts-test-${Date.now()}.json`;
      const originalFetch = globalThis.fetch;
      const fetchMock = vi.fn(async () => ({
        ok: true,
        arrayBuffer: async () => new ArrayBuffer(1),
      }));
      globalThis.fetch = fetchMock as unknown as typeof fetch;
      try {
        await run(fetchMock);
      } finally {
        globalThis.fetch = originalFetch;
        process.env.OPENCLAW_TTS_PREFS = prevPrefs;
      }
    };

    const taggedCfg: OpenClawConfig = {
      ...baseCfg,
      messages: {
        ...baseCfg.messages!,
        tts: { ...baseCfg.messages!.tts, auto: "tagged" },
      },
    };

    it("skips auto-TTS when inbound audio gating is on and the message is not audio", async () => {
      await withMockedAutoTtsFetch(async (fetchMock) => {
        const payload = { text: "Hello world" };
        const result = await maybeApplyTtsToPayload({
          payload,
          cfg: baseCfg,
          kind: "final",
          inboundAudio: false,
        });

        expect(result).toBe(payload);
        expect(fetchMock).not.toHaveBeenCalled();
      });
    });

    it("skips auto-TTS when markdown stripping leaves text too short", async () => {
      await withMockedAutoTtsFetch(async (fetchMock) => {
        const payload = { text: "### **bold**" };
        const result = await maybeApplyTtsToPayload({
          payload,
          cfg: baseCfg,
          kind: "final",
          inboundAudio: true,
        });

        expect(result).toBe(payload);
        expect(fetchMock).not.toHaveBeenCalled();
      });
    });

    it("attempts auto-TTS when inbound audio gating is on and the message is audio", async () => {
      await withMockedAutoTtsFetch(async (fetchMock) => {
        const result = await maybeApplyTtsToPayload({
          payload: { text: "Hello world" },
          cfg: baseCfg,
          kind: "final",
          inboundAudio: true,
        });

        expect(result.mediaUrl).toBeDefined();
        expect(fetchMock).toHaveBeenCalledTimes(1);
      });
    });

    it("skips auto-TTS in tagged mode unless a tts tag is present", async () => {
      await withMockedAutoTtsFetch(async (fetchMock) => {
        const payload = { text: "Hello world" };
        const result = await maybeApplyTtsToPayload({
          payload,
          cfg: taggedCfg,
          kind: "final",
        });

        expect(result).toBe(payload);
        expect(fetchMock).not.toHaveBeenCalled();
      });
    });

    it("runs auto-TTS in tagged mode when tags are present", async () => {
      await withMockedAutoTtsFetch(async (fetchMock) => {
        const result = await maybeApplyTtsToPayload({
          payload: { text: "[[tts:text]]Hello world[[/tts:text]]" },
          cfg: taggedCfg,
          kind: "final",
        });

        expect(result.mediaUrl).toBeDefined();
        expect(fetchMock).toHaveBeenCalledTimes(1);
      });
    });

    it("does not apply instruction/stream defaults from openai config", async () => {
      const cfg: OpenClawConfig = {
        ...baseCfg,
        messages: {
          tts: {
            ...baseCfg.messages!.tts,
            openai: {
              ...baseCfg.messages!.tts!.openai!,
            },
          },
        },
      };

      await withMockedAutoTtsFetch(async (fetchMock) => {
        await maybeApplyTtsToPayload({
          payload: { text: "Hello this is long enough for tts" },
          cfg,
          kind: "final",
          inboundAudio: true,
        });

        const body = getFetchRequestBody(
          fetchMock as unknown as { mock: { calls: unknown[][] } },
          0,
        );
        expect(body.instructions).toBeUndefined();
        expect(body.stream).toBeUndefined();
      });
    });

    it("uses qwen3-fastapi instructions + stream defaults and allows directive stream override", async () => {
      const cfg: OpenClawConfig = {
        ...baseCfg,
        messages: {
          tts: {
            ...baseCfg.messages!.tts,
            provider: "qwen3-fastapi",
            openai: {
              ...baseCfg.messages!.tts!.openai!,
              apiKey: undefined,
            },
            qwen3Fastapi: {
              baseUrl: "http://127.0.0.1:8000/v1",
              model: "Qwen/Qwen3-TTS-0.6B",
              voice: "Chelsie",
              instructions: "calm",
              stream: true,
            },
          },
        },
      };

      await withMockedAutoTtsFetch(async (fetchMock) => {
        await maybeApplyTtsToPayload({
          payload: {
            text: "[[tts:provider=qwen3-fastapi stream=off]]Hello this is long enough for tts",
          },
          cfg,
          kind: "final",
          inboundAudio: true,
        });

        expect(String(fetchMock.mock.calls[0]?.[0])).toBe("http://127.0.0.1:8000/v1/audio/speech");
        const body = getFetchRequestBody(
          fetchMock as unknown as { mock: { calls: unknown[][] } },
          0,
        );
        expect(body.instructions).toBe("calm");
        expect(body.stream).toBeUndefined();
      });
    });
  });
});
