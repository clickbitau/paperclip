import { describe, expect, it, vi } from "vitest";
import { runStartupHeartbeatRecovery } from "./startup-recovery.js";

describe("nonfatal startup recovery", () => {
  const logger = () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn() });
  it("keeps the server available and stops admission when native ownership recovery fails", async () => {
    const log = logger();
    const heartbeat = { recoverNativeRunsAfterRestart: vi.fn().mockRejectedValue(new Error("bad ownership")),
      resumeQueuedRuns: vi.fn() };
    await expect(runStartupHeartbeatRecovery(heartbeat as never, log as never)).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledOnce();
    expect(heartbeat.resumeQueuedRuns).not.toHaveBeenCalled();
  });
  it("logs failed recovery steps and still reaches the stale-lock sweep", async () => {
    const log = logger();
    const heartbeat = new Proxy({ recoverNativeRunsAfterRestart: vi.fn().mockResolvedValue({ dispositions: [] }),
      sweepStaleIssueLocks: vi.fn().mockResolvedValue({ cleared: 0 }) }, {
      get(target, key) { return key in target ? target[key as keyof typeof target] : vi.fn().mockRejectedValue(new Error("bad row")); }
    });
    await expect(runStartupHeartbeatRecovery(heartbeat as never, log as never)).resolves.toBeUndefined();
    expect(heartbeat.sweepStaleIssueLocks).toHaveBeenCalledOnce();
    expect(log.error).toHaveBeenCalled();
  });
});
