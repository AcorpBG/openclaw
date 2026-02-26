import { PassThrough } from "node:stream";
import type { AudioPlayer } from "@discordjs/voice";
import { describe, expect, it, vi } from "vitest";
import { playDiscordPcmStream } from "./stream-playback.js";

const { createAudioResourceMock, entersStateMock } = vi.hoisted(() => ({
  createAudioResourceMock: vi.fn((input: unknown) => ({ input })),
  entersStateMock: vi.fn(async () => undefined),
}));

vi.mock("@discordjs/voice", () => ({
  AudioPlayerStatus: { Playing: "playing", Idle: "idle" },
  StreamType: { Raw: "raw" },
  createAudioResource: createAudioResourceMock,
  entersState: entersStateMock,
}));

function createPlayer() {
  const play = vi.fn();
  const stop = vi.fn();
  return {
    player: {
      play,
      stop,
    } as unknown as AudioPlayer,
    play,
    stop,
  };
}

describe("playDiscordPcmStream", () => {
  it("returns aborted=true and stops player when aborted", async () => {
    const pcm = new PassThrough();
    const abort = new AbortController();
    const { player, stop } = createPlayer();
    const pending = playDiscordPcmStream({
      player,
      pcmStream: pcm,
      abortSignal: abort.signal,
      inputSampleRate: 24_000,
      inputChannels: 1,
    });

    pcm.write(Buffer.alloc(4));
    abort.abort();
    const result = await pending;

    expect(result.aborted).toBe(true);
    expect(stop).toHaveBeenCalled();
  });

  it("throws on stream pipeline errors", async () => {
    const pcm = new PassThrough();
    const { player } = createPlayer();
    const pending = playDiscordPcmStream({
      player,
      pcmStream: pcm,
      inputSampleRate: 24_000,
      inputChannels: 1,
    });

    pcm.emit("error", new Error("boom"));
    await expect(pending).rejects.toThrow("boom");
  });
});
