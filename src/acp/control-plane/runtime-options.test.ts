import { describe, expect, it } from "vitest";
import {
  extractAdvertisedRuntimeConfigOptionKeys,
  resolveCompatibleRuntimeConfigOptionKey,
} from "./runtime-options.js";

describe("runtime-options", () => {
  it("preserves backend-advertised key casing when resolving semantic thinking overrides", () => {
    expect(
      resolveCompatibleRuntimeConfigOptionKey({
        semanticOption: "thinking",
        capabilities: {
          configOptionKeys: ["model", "Reasoning_Effort"],
        },
      }),
    ).toBe("Reasoning_Effort");
  });

  it("merges capability and runtime status config keys before resolving semantic overrides", () => {
    expect(
      extractAdvertisedRuntimeConfigOptionKeys({
        capabilities: {
          configOptionKeys: ["model"],
        },
        runtimeStatus: {
          details: {
            configOptions: [{ id: "Reasoning_Effort" }],
            result: {
              configOptions: [{ id: "temperature" }],
            },
          },
        },
      }),
    ).toEqual(["model", "Reasoning_Effort", "temperature"]);

    expect(
      resolveCompatibleRuntimeConfigOptionKey({
        semanticOption: "thinking",
        capabilities: {
          configOptionKeys: ["model"],
        },
        runtimeStatus: {
          details: {
            configOptions: [{ id: "Reasoning_Effort" }],
          },
        },
      }),
    ).toBe("Reasoning_Effort");
  });
});
