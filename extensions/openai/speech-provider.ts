import type { SpeechProviderPlugin } from "openclaw/plugin-sdk/core";
import {
  OPENAI_TTS_OUTPUT_FORMATS,
  OPENAI_TTS_VOICES,
  openaiTTS,
  openaiTTSStream,
} from "openclaw/plugin-sdk/speech";

type OpenAIOutputFormat = (typeof OPENAI_TTS_OUTPUT_FORMATS)[number];
type OpenAIFileOutputFormat = Exclude<OpenAIOutputFormat, "pcm">;

function resolveSynthesisFormat(
  req: Parameters<SpeechProviderPlugin["synthesize"]>[0],
): OpenAIFileOutputFormat {
  if (req.target === "voice-note") {
    return "opus";
  }
  const override = req.overrides?.openai?.outputFormat;
  if (override) {
    return assertFileSynthesisFormat(override);
  }
  const configured = req.config.openai.responseFormat;
  if (configured) {
    return assertFileSynthesisFormat(configured);
  }
  return "mp3";
}

function assertFileSynthesisFormat(format: OpenAIOutputFormat): OpenAIFileOutputFormat {
  if (format === "pcm") {
    throw new Error("OpenAI pcm output is only supported for telephony synthesis");
  }
  return format;
}

function inferFileExtension(outputFormat: OpenAIFileOutputFormat): string {
  switch (outputFormat) {
    case "wav":
      return ".wav";
    case "opus":
      return ".opus";
    case "mp3":
      return ".mp3";
    case "aac":
      return ".aac";
  }
}

function toSpeechResult(params: {
  audioBuffer: Buffer;
  outputFormat: OpenAIFileOutputFormat;
  target: Parameters<SpeechProviderPlugin["synthesize"]>[0]["target"];
}) {
  return {
    audioBuffer: params.audioBuffer,
    outputFormat: params.outputFormat,
    fileExtension: inferFileExtension(params.outputFormat),
    voiceCompatible: params.target === "voice-note" && params.outputFormat === "opus",
  };
}

export function buildOpenAISpeechProvider(): SpeechProviderPlugin {
  return {
    id: "openai",
    label: "OpenAI",
    listVoices: async () => OPENAI_TTS_VOICES.map((voice) => ({ id: voice, name: voice })),
    isConfigured: ({ config }) => Boolean(config.openai.apiKey || process.env.OPENAI_API_KEY),
    synthesize: async (req) => {
      const apiKey = req.config.openai.apiKey || process.env.OPENAI_API_KEY;
      if (!apiKey) {
        throw new Error("OpenAI API key missing");
      }
      const responseFormat = resolveSynthesisFormat(req);
      const audioBuffer = await openaiTTS({
        text: req.text,
        apiKey,
        baseUrl: req.config.openai.baseUrl,
        model: req.overrides?.openai?.model ?? req.config.openai.model,
        voice: req.overrides?.openai?.voice ?? req.config.openai.voice,
        speed: req.overrides?.openai?.speed ?? req.config.openai.speed,
        instructions: req.config.openai.instructions,
        responseFormat,
        timeoutMs: req.config.timeoutMs,
      });
      return toSpeechResult({
        audioBuffer,
        outputFormat: responseFormat,
        target: req.target,
      });
    },
    synthesizeStream: async (req) => {
      const apiKey = req.config.openai.apiKey || process.env.OPENAI_API_KEY;
      if (!apiKey) {
        throw new Error("OpenAI API key missing");
      }
      const responseFormat = resolveSynthesisFormat(req);
      const audio = await openaiTTSStream({
        text: req.text,
        apiKey,
        baseUrl: req.config.openai.baseUrl,
        model: req.overrides?.openai?.model ?? req.config.openai.model,
        voice: req.overrides?.openai?.voice ?? req.config.openai.voice,
        speed: req.overrides?.openai?.speed ?? req.config.openai.speed,
        instructions: req.config.openai.instructions,
        responseFormat,
        timeoutMs: req.config.timeoutMs,
      });
      return {
        audioStream: audio.audioStream,
        outputFormat: responseFormat,
        fileExtension: inferFileExtension(responseFormat),
        voiceCompatible: req.target === "voice-note" && responseFormat === "opus",
        abort: audio.abort,
      };
    },
    synthesizeTelephony: async (req) => {
      const apiKey = req.config.openai.apiKey || process.env.OPENAI_API_KEY;
      if (!apiKey) {
        throw new Error("OpenAI API key missing");
      }
      const outputFormat = "pcm";
      const sampleRate = 24_000;
      const audioBuffer = await openaiTTS({
        text: req.text,
        apiKey,
        baseUrl: req.config.openai.baseUrl,
        model: req.config.openai.model,
        voice: req.config.openai.voice,
        speed: req.config.openai.speed,
        instructions: req.config.openai.instructions,
        responseFormat: outputFormat,
        timeoutMs: req.config.timeoutMs,
      });
      return { audioBuffer, outputFormat, sampleRate };
    },
  };
}
