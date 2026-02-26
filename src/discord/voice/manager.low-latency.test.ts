import { Readable } from "node:stream";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

const { textToSpeechMock, textToSpeechStreamMock } = vi.hoisted(() => ({
  textToSpeechMock: vi.fn(),
  textToSpeechStreamMock: vi.fn(),
}));
const { createAudioResourceMock } = vi.hoisted(() => ({
  createAudioResourceMock: vi.fn((input: unknown) => ({ input })),
}));
const { playDiscordPcmStreamMock } = vi.hoisted(() => ({
  playDiscordPcmStreamMock: vi.fn(async () => ({ aborted: false })),
}));

vi.mock("@discordjs/voice", () => ({
  AudioPlayerStatus: { Playing: "playing", Idle: "idle" },
  EndBehaviorType: { AfterSilence: "AfterSilence" },
  VoiceConnectionStatus: {
    Ready: "ready",
    Disconnected: "disconnected",
    Destroyed: "destroyed",
    Signalling: "signalling",
    Connecting: "connecting",
  },
  createAudioPlayer: vi.fn(),
  createAudioResource: createAudioResourceMock,
  entersState: vi.fn(async () => undefined),
  joinVoiceChannel: vi.fn(),
}));

vi.mock("../../tts/tts.js", () => ({
  resolveTtsConfig: vi.fn(() => ({ modelOverrides: {} })),
  textToSpeech: textToSpeechMock,
  textToSpeechStream: textToSpeechStreamMock,
}));

vi.mock("./stream-playback.js", () => ({
  playDiscordPcmStream: playDiscordPcmStreamMock,
}));

let managerModule: typeof import("./manager.js");

function createManager(lowLatency: Record<string, unknown> | undefined) {
  return new managerModule.DiscordVoiceManager({
    client: {} as never,
    cfg: {},
    discordConfig: {
      voice: lowLatency ? ({ lowLatency } as never) : {},
    },
    accountId: "default",
    runtime: {} as never,
  });
}

function createEntry() {
  return {
    guildId: "g1",
    channelId: "c1",
    player: {
      stop: vi.fn(),
      play: vi.fn(),
      state: { status: "idle" },
    },
    playbackQueue: Promise.resolve(),
    activePlaybackAbortController: null,
    playbackGeneration: 0,
  } as {
    guildId: string;
    channelId: string;
    player: {
      stop: ReturnType<typeof vi.fn>;
      play: ReturnType<typeof vi.fn>;
      state: { status: string };
    };
    playbackQueue: Promise<void>;
    activePlaybackAbortController: AbortController | null;
    playbackGeneration: number;
  };
}

describe("DiscordVoiceManager low-latency playback", () => {
  beforeAll(async () => {
    managerModule = await import("./manager.js");
  });

  beforeEach(() => {
    textToSpeechMock.mockReset().mockResolvedValue({ success: true, audioPath: "/tmp/voice.mp3" });
    textToSpeechStreamMock
      .mockReset()
      .mockResolvedValue({ success: true, audioStream: Readable.from([Buffer.alloc(4)]) });
    playDiscordPcmStreamMock.mockClear().mockResolvedValue({ aborted: false });
    createAudioResourceMock.mockClear();
  });

  it("uses buffered playback when low-latency streaming is disabled", async () => {
    const manager = createManager({ enabled: false, ttsStream: true });
    const entry = createEntry();
    const playReplyText = (
      manager as unknown as {
        playReplyText: (params: {
          entry: unknown;
          speakText: string;
          ttsCfg: Record<string, unknown>;
          directiveOverrides: Record<string, unknown>;
        }) => Promise<void>;
      }
    ).playReplyText;

    await playReplyText.call(manager, {
      entry,
      speakText: "hello",
      ttsCfg: {},
      directiveOverrides: {},
    });
    await entry.playbackQueue;

    expect(textToSpeechStreamMock).not.toHaveBeenCalled();
    expect(textToSpeechMock).toHaveBeenCalledTimes(1);
    expect(createAudioResourceMock).toHaveBeenCalledWith("/tmp/voice.mp3");
  });

  it("uses streaming playback when low-latency streaming is enabled", async () => {
    const manager = createManager({ enabled: true, ttsStream: true });
    const entry = createEntry();
    const playReplyText = (
      manager as unknown as {
        playReplyText: (params: {
          entry: unknown;
          speakText: string;
          ttsCfg: Record<string, unknown>;
          directiveOverrides: Record<string, unknown>;
        }) => Promise<void>;
      }
    ).playReplyText;

    await playReplyText.call(manager, {
      entry,
      speakText: "hello",
      ttsCfg: {},
      directiveOverrides: {},
    });
    await entry.playbackQueue;

    expect(textToSpeechStreamMock).toHaveBeenCalledTimes(1);
    expect(playDiscordPcmStreamMock).toHaveBeenCalledTimes(1);
    expect(textToSpeechMock).not.toHaveBeenCalled();
  });

  it("falls back to buffered playback when stream fails and fallback is enabled", async () => {
    textToSpeechStreamMock.mockResolvedValueOnce({ success: false, error: "stream failed" });
    const manager = createManager({ enabled: true, ttsStream: true, fallbackBuffered: true });
    const entry = createEntry();
    const playReplyText = (
      manager as unknown as {
        playReplyText: (params: {
          entry: unknown;
          speakText: string;
          ttsCfg: Record<string, unknown>;
          directiveOverrides: Record<string, unknown>;
        }) => Promise<void>;
      }
    ).playReplyText;

    await playReplyText.call(manager, {
      entry,
      speakText: "hello",
      ttsCfg: {},
      directiveOverrides: {},
    });
    await entry.playbackQueue;

    expect(textToSpeechMock).toHaveBeenCalledTimes(1);
    expect(createAudioResourceMock).toHaveBeenCalledWith("/tmp/voice.mp3");
  });

  it("skips buffered fallback when stream fails and fallback is disabled", async () => {
    textToSpeechStreamMock.mockResolvedValueOnce({ success: false, error: "stream failed" });
    const manager = createManager({ enabled: true, ttsStream: true, fallbackBuffered: false });
    const entry = createEntry();
    const playReplyText = (
      manager as unknown as {
        playReplyText: (params: {
          entry: unknown;
          speakText: string;
          ttsCfg: Record<string, unknown>;
          directiveOverrides: Record<string, unknown>;
        }) => Promise<void>;
      }
    ).playReplyText;

    await playReplyText.call(manager, {
      entry,
      speakText: "hello",
      ttsCfg: {},
      directiveOverrides: {},
    });
    await entry.playbackQueue;

    expect(textToSpeechMock).not.toHaveBeenCalled();
  });

  it("resolves low-latency defaults with flags off", () => {
    expect(managerModule.resolveDiscordVoiceLowLatencyConfig(undefined)).toEqual({
      enabled: false,
      llmChunking: false,
      ttsStream: false,
      maxBufferedMs: 6000,
      chunkMaxChars: 140,
      idleFlushMs: 250,
      fallbackBuffered: true,
    });
  });
});
