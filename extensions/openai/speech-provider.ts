import type { SpeechProviderPlugin } from "openclaw/plugin-sdk/core";
import {
  OPENAI_TTS_MODELS,
  OPENAI_TTS_OUTPUT_FORMATS,
  OPENAI_TTS_VOICES,
  openaiTTS,
  openaiTTSStream,
} from "openclaw/plugin-sdk/speech";

const OPENAI_FILE_EXTENSIONS = {
  wav: ".wav",
  opus: ".opus",
  mp3: ".mp3",
  aac: ".aac",
  pcm: ".wav",
} as const;

type OpenAIOutputFormat = (typeof OPENAI_TTS_OUTPUT_FORMATS)[number];

function resolveSynthesisFormat(
  req: Parameters<SpeechProviderPlugin["synthesize"]>[0],
): OpenAIOutputFormat {
  const override = req.overrides?.openai?.outputFormat;
  if (override) {
    return override;
  }
  const configured = req.config.openai.responseFormat;
  if (configured) {
    return configured;
  }
  return req.target === "voice-note" ? "opus" : "mp3";
}

function toSpeechResult(params: {
  audioBuffer: Buffer;
  outputFormat: OpenAIOutputFormat;
  target: Parameters<SpeechProviderPlugin["synthesize"]>[0]["target"];
}) {
  return {
    audioBuffer: params.audioBuffer,
    outputFormat: params.outputFormat,
    fileExtension: OPENAI_FILE_EXTENSIONS[params.outputFormat],
    voiceCompatible: params.target === "voice-note" && params.outputFormat === "opus",
  };
}

export function buildOpenAISpeechProvider(): SpeechProviderPlugin {
  return {
    id: "openai",
    label: "OpenAI",
    models: OPENAI_TTS_MODELS,
    voices: OPENAI_TTS_VOICES,
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
        fileExtension: OPENAI_FILE_EXTENSIONS[responseFormat],
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
