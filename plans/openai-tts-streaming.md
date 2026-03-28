# OpenAI-Compatible TTS Streaming Plan

Scope:

- Keep the blast radius limited to TTS.
- Support OpenAI-compatible TTS output formats: `wav`, `opus`, `mp3`, `aac`, `pcm`.
- Make Discord voice the first streaming consumer without rewriting the existing buffered reply-attachment path.

Plan:

1. Add a new streaming TTS runtime path alongside the existing buffered path.
2. Extend the speech provider contract with optional streaming support.
3. Implement OpenAI streaming in the shared TTS core helper.
4. Add OpenAI provider-side format selection for buffered and streamed synthesis.
5. Switch Discord voice playback to the new stream path.
6. Preserve all existing non-Discord buffered TTS surfaces.
7. Optionally expose OpenAI output-format config on `messages.tts.openai`.
8. Keep provider fallback behavior intact.
9. Add targeted tests for TTS, OpenAI provider, and Discord voice playback.

Non-goals:

- No rewrite of auto-TTS reply delivery.
- No Discord voice-message attachment changes.
- No telephony pipeline changes.
- No provider-wide format unification for ElevenLabs or Microsoft in this pass.
