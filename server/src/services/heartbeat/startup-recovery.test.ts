import { describe, expect, it, vi } from "vitest";
import { createDb } from "@paperclipai/db";
import { createDeliveryWorkCoordinator } from "../delivery-work-coordinator.js";
import { registerStartupGoalRecoveryRetry, runStartupHeartbeatRecovery } from "./startup-recovery.js";

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
  it("retries failed goal work through the shared coordinator and becomes quiet after success", async () => {
    const log = logger();
    const coordinator = createDeliveryWorkCoordinator({ owner: createDb("postgres://test:test@127.0.0.1:1/unused"), canRun: () => true, onError: log.error });
    let actions = 0;
    let goals = 0;
    const heartbeat = new Proxy({
      recoverNativeRunsAfterRestart: vi.fn().mockResolvedValue({ dispositions: [] }),
      recoverPendingSessionGoalActions: vi.fn(async () => {
        actions++;
        if (actions <= 2) throw new Error("temporary database outage");
        return { enqueued: 1, invalid: 0 };
      }),
      recoverActiveSessionGoals: vi.fn(async () => {
        goals++;
        if (goals === 1) throw new Error("temporary database outage");
        return { enqueued: 1 };
      }),
    }, { get(target, key) {
      return key in target ? target[key as keyof typeof target] : vi.fn().mockResolvedValue({});
    } });
    try {
      await runStartupHeartbeatRecovery(heartbeat as never, log, registerStartupGoalRecoveryRetry(coordinator, log, 10));
      await vi.waitFor(() => expect(actions).toBe(3));
      expect(goals).toBe(2);
      expect(coordinator.nextWakeAt()).toBeNull();
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(actions).toBe(3);
      expect(goals).toBe(2);
    } finally {
      await coordinator.stop();
    }
  });

});
