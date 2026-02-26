import { PassThrough, Transform, type Readable } from "node:stream";
import {
  AudioPlayerStatus,
  StreamType,
  createAudioResource,
  entersState,
  type AudioPlayer,
} from "@discordjs/voice";

const DISCORD_SAMPLE_RATE = 48_000;
const DISCORD_CHANNELS = 2;
const PCM_SAMPLE_BYTES = 2;
const DEFAULT_MAX_BUFFERED_MS = 6_000;
const MIN_BUFFERED_MS = 200;
const PLAYBACK_READY_TIMEOUT_MS = 15_000;
const PLAYBACK_IDLE_TIMEOUT_MS = 60_000;

function resolveMaxBufferedBytes(maxBufferedMs?: number): number {
  const boundedMs = Math.max(MIN_BUFFERED_MS, maxBufferedMs ?? DEFAULT_MAX_BUFFERED_MS);
  const bytesPerSecond = DISCORD_SAMPLE_RATE * DISCORD_CHANNELS * PCM_SAMPLE_BYTES;
  return Math.max(32_768, Math.floor((boundedMs / 1_000) * bytesPerSecond));
}

class PcmTransform extends Transform {
  private carry: Buffer = Buffer.alloc(0);
  private readonly frameBytes: number;

  constructor(
    private readonly inputSampleRate: number,
    private readonly inputChannels: 1 | 2,
  ) {
    super();
    this.frameBytes = inputChannels * PCM_SAMPLE_BYTES;
    if (inputSampleRate !== 24_000 && inputSampleRate !== 48_000) {
      throw new Error(
        `unsupported PCM sample rate for Discord stream playback: ${inputSampleRate}`,
      );
    }
  }

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    try {
      const fullChunk = this.carry.length > 0 ? Buffer.concat([this.carry, chunk]) : chunk;
      const usableLength = fullChunk.length - (fullChunk.length % this.frameBytes);
      this.carry =
        usableLength < fullChunk.length ? fullChunk.subarray(usableLength) : Buffer.alloc(0);
      if (usableLength === 0) {
        callback();
        return;
      }
      const input = fullChunk.subarray(0, usableLength);
      const inputFrames = Math.floor(input.length / this.frameBytes);
      const upsampleFactor = this.inputSampleRate === DISCORD_SAMPLE_RATE ? 1 : 2;
      const outputFrames = inputFrames * upsampleFactor;
      const output = Buffer.allocUnsafe(outputFrames * DISCORD_CHANNELS * PCM_SAMPLE_BYTES);
      let outputOffset = 0;
      for (let frame = 0; frame < inputFrames; frame += 1) {
        const base = frame * this.frameBytes;
        const left = input.readInt16LE(base);
        const right = this.inputChannels === 2 ? input.readInt16LE(base + 2) : left;
        for (let i = 0; i < upsampleFactor; i += 1) {
          output.writeInt16LE(left, outputOffset);
          output.writeInt16LE(right, outputOffset + 2);
          outputOffset += 4;
        }
      }
      this.push(output);
      callback();
    } catch (err) {
      callback(err as Error);
    }
  }
}

export async function playDiscordPcmStream(params: {
  player: AudioPlayer;
  pcmStream: Readable;
  abortSignal?: AbortSignal;
  inputSampleRate?: number;
  inputChannels?: 1 | 2;
  maxBufferedMs?: number;
}): Promise<{ aborted: boolean }> {
  const inputSampleRate = params.inputSampleRate ?? 24_000;
  const inputChannels = params.inputChannels ?? 1;
  const maxBufferedBytes = resolveMaxBufferedBytes(params.maxBufferedMs);

  const pcmTransform = new PcmTransform(inputSampleRate, inputChannels);
  const output = new PassThrough({
    highWaterMark: maxBufferedBytes,
  });
  const resource = createAudioResource(output, {
    inputType: StreamType.Raw,
  });

  let aborted = params.abortSignal?.aborted === true;
  let settled = false;
  const cleanup = (stopPlayer: boolean) => {
    if (settled) {
      return;
    }
    settled = true;
    if (stopPlayer) {
      params.player.stop(true);
    }
    params.pcmStream.unpipe(pcmTransform);
    pcmTransform.unpipe(output);
    params.pcmStream.destroy();
    pcmTransform.destroy();
    output.destroy();
  };

  const abortListener = () => {
    aborted = true;
    cleanup(true);
  };
  params.abortSignal?.addEventListener("abort", abortListener);

  const pipeDone = new Promise<{ error?: Error }>((resolve) => {
    let done = false;
    const settle = (error?: Error) => {
      if (done) {
        return;
      }
      done = true;
      resolve(error ? { error } : {});
    };
    params.pcmStream.on("error", (err) => settle(err));
    pcmTransform.on("error", (err) => settle(err));
    output.on("error", (err) => settle(err));
    output.on("finish", () => settle());
    output.on("close", () => {
      if (aborted) {
        settle();
      }
    });
    params.pcmStream.pipe(pcmTransform).pipe(output);
  });

  const abortDone = params.abortSignal
    ? new Promise<void>((resolve) => {
        params.abortSignal?.addEventListener(
          "abort",
          () => {
            aborted = true;
            resolve();
          },
          { once: true },
        );
      })
    : null;

  try {
    if (!aborted) {
      params.player.play(resource);
      await entersState(params.player, AudioPlayerStatus.Playing, PLAYBACK_READY_TIMEOUT_MS).catch(
        () => undefined,
      );
      await (abortDone ? Promise.race([pipeDone, abortDone]) : pipeDone);
      const { error } = await pipeDone;
      if (error && !aborted) {
        throw error;
      }
      if (!aborted) {
        await entersState(params.player, AudioPlayerStatus.Idle, PLAYBACK_IDLE_TIMEOUT_MS).catch(
          () => undefined,
        );
      }
    }
    return { aborted };
  } finally {
    params.abortSignal?.removeEventListener("abort", abortListener);
    cleanup(false);
  }
}
