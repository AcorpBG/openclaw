// Shared speech-provider implementation helpers for bundled and third-party plugins.

export type { SpeechProviderPlugin } from "../plugins/types.js";
export type { SpeechVoiceOption } from "../tts/provider-types.js";

export {
  edgeTTS,
  elevenLabsTTS,
  inferEdgeExtension,
  OPENAI_TTS_MODELS,
  OPENAI_TTS_OUTPUT_FORMATS,
  OPENAI_TTS_VOICES,
  openaiTTS,
  openaiTTSStream,
  parseTtsDirectives,
} from "../tts/tts-core.js";

export { resolvePreferredOpenClawTmpDir } from "../infra/tmp-openclaw-dir.js";
export { isVoiceCompatibleAudio } from "../media/audio.js";
