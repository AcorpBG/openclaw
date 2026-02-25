import { describe, expect, it } from "vitest";
import type { TtsConfig } from "../../config/types.js";
import { mergeTtsConfig } from "./manager.js";

describe("mergeTtsConfig", () => {
  it("merges qwen3Fastapi overrides like other provider subtrees", () => {
    const base: TtsConfig = {
      provider: "qwen3-fastapi",
      qwen3Fastapi: {
        apiKey: "base-key",
        baseUrl: "http://localhost:8000/v1",
        model: "qwen3-tts",
        voice: "Chelsie",
        stream: false,
      },
    };
    const override: TtsConfig = {
      qwen3Fastapi: {
        voice: "Dylan",
        stream: true,
      },
    };

    const merged = mergeTtsConfig(base, override);
    expect(merged.qwen3Fastapi?.apiKey).toBe("base-key");
    expect(merged.qwen3Fastapi?.baseUrl).toBe("http://localhost:8000/v1");
    expect(merged.qwen3Fastapi?.model).toBe("qwen3-tts");
    expect(merged.qwen3Fastapi?.voice).toBe("Dylan");
    expect(merged.qwen3Fastapi?.stream).toBe(true);
  });
});
