import type { createDeliveryWorkCoordinator } from "../delivery-work-coordinator.js";
import { DELIVERY_QUEUES } from "../delivery-work-notifications.js";
import { timeStartupRecovery } from "../../startup-recovery-timing.js";
import type { Logger } from "pino";
import type { heartbeatService } from "../heartbeat.js";

type StartupRecoveryHeartbeat = Pick<
  ReturnType<typeof heartbeatService>,
  | "recoverNativeRunsAfterRestart"
  | "reconcileHotRestartAdoption"
  | "reapOrphanedRuns"
  | "promoteDueScheduledRetries"
  | "resumeQueuedRuns"
  | "recoverPendingSessionGoalActions"
  | "recoverActiveSessionGoals"
  | "reconcileStrandedAssignedIssues"
  | "reconcileResolvedDependencyWakes"
  | "reconcileTaskWatchdogs"
  | "scanSilentActiveRuns"
  | "sweepStaleIssueLocks"
>;

type StartupRecoveryLogger = Pick<Logger, "info" | "warn" | "error">;

// Retain only failed startup obligations. The shared coordinator owns retry
// deadlines, drain/resume and shutdown; successful recovery becomes quiet.
export function registerStartupGoalRecoveryRetry(
  coordinator: ReturnType<typeof createDeliveryWorkCoordinator>,
  logger: StartupRecoveryLogger,
  retryMs = 30_000,
) {
  const pending = new Map<string, () => Promise<unknown>>();
  const worker = coordinator.register(DELIVERY_QUEUES.startupGoals, {
    retryMs,
    hasPending: async () => pending.size > 0,
    run: async (signal) => {
      for (const [name, run] of pending) {
        if (signal.aborted) return;
        try {
          await run();
          pending.delete(name);
        } catch (err) {
          logger.error({ err, step: name }, "startup goal recovery retry failed; work remains pending");
        }
      }
    },
  });
  return (name: string, run: () => Promise<unknown>) => {
    pending.set(name, run);
    worker.wake();
  };
}

// Startup recovery reads every open issue and run, so one bad row must not
// stop the server from booting. If native ownership recovery fails, skip
// further startup admission to avoid starting a provider twice. Keep the
// server available for health and diagnosis. Later steps are isolated: the
// next step runs. Failed goal recovery is retained by the shared retry worker;
// other phases have their existing periodic recovery backstops.
export async function runStartupHeartbeatRecovery(
  heartbeat: StartupRecoveryHeartbeat,
  logger: StartupRecoveryLogger,
  retryGoalRecovery?: (name: string, run: () => Promise<unknown>) => void,
) {
  try {
    const nativeRecovery = await timeStartupRecovery("native_recovery", () => heartbeat.recoverNativeRunsAfterRestart());
    if (nativeRecovery.dispositions.length > 0) {
      logger.info(
        {
          restartKind: nativeRecovery.restartKind,
          claims: nativeRecovery.claims.map((claim) => ({
            runId: claim.runId,
            disposition: claim.kind,
            controllerGeneration: claim.controllerGeneration,
          })),
          awaitingEvidenceRunIds:
            nativeRecovery.awaitingEvidenceRunIds,
          blockedRunIds: nativeRecovery.blockedRunIds,
        },
        "startup native runner restart recovery classified",
      );
    }
  } catch (err) {
    logger.error(
      { err },
      "startup native runner restart recovery failed closed",
    );
    return;
  }
  try {
    const hotRestart = await timeStartupRecovery("hot_restart", () => heartbeat.reconcileHotRestartAdoption());
    if (hotRestart.mode === "reported") {
      logger.info(
        hotRestart,
        "startup hot-restart adoption reconciliation complete",
      );
    }
  } catch (err) {
    logger.error(
      { err },
      "startup hot-restart adoption reconciliation failed - orphan reaper will serve as degraded backstop",
    );
  }

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const result = await timeStartupRecovery("orphan_reaping", () => heartbeat.reapOrphanedRuns());
      logger.info(
        { reaped: result.reaped, runIds: result.runIds },
        "startup reap of orphaned heartbeat runs complete",
      );
      break;
    } catch (err) {
      if (attempt < 2) {
        logger.warn({ err, attempt }, "startup reap failed, retrying");
      } else {
        logger.error(
          { err },
          "startup reap of orphaned heartbeat runs failed after retry - periodic reaper will serve as degraded backstop",
        );
      }
    }
  }

  const step = async <T>(name: string, run: () => Promise<T>): Promise<T | null> => {
    try {
      return await run();
    } catch (err) {
      logger.error(
        { err, step: name },
        "startup heartbeat recovery step failed; continuing with the next step",
      );
      if (name === "recoverPendingSessionGoalActions" || name === "recoverActiveSessionGoals") {
        retryGoalRecovery?.(name, run);
      }
      return null;
    }
  };

  const promotion = await step("promoteDueScheduledRetries", () =>
    timeStartupRecovery("scheduled_retries", () => heartbeat.promoteDueScheduledRetries()));
  await step("resumeQueuedRuns", () => timeStartupRecovery("queued_runs", () => heartbeat.resumeQueuedRuns()));
  const recoveredGoalActions = await step("recoverPendingSessionGoalActions", () =>
    timeStartupRecovery("goal_actions", () => heartbeat.recoverPendingSessionGoalActions()));
  if (
    recoveredGoalActions &&
    (recoveredGoalActions.enqueued > 0 || recoveredGoalActions.invalid > 0)
  ) {
    logger.warn(
      recoveredGoalActions,
      "startup session-goal action outbox recovery reconciled pending controls",
    );
  }
  const recoveredGoals = await step("recoverActiveSessionGoals", () =>
    timeStartupRecovery("active_goals", () => heartbeat.recoverActiveSessionGoals()));
  if (recoveredGoals && recoveredGoals.enqueued > 0) {
    logger.warn(
      recoveredGoals,
      "startup session-goal recovery resumed durable agent goals",
    );
  }
  const reconciled = await step("reconcileStrandedAssignedIssues", () =>
    heartbeat.reconcileStrandedAssignedIssues());
  if (
    (promotion?.promoted ?? 0) > 0 ||
    (reconciled &&
      (reconciled.assignmentDispatched > 0 ||
        reconciled.dispatchRequeued > 0 ||
        reconciled.continuationRequeued > 0 ||
        reconciled.successfulRunHandoffEscalated > 0 ||
        reconciled.successfulRunHandoffRetried > 0 ||
        reconciled.escalated > 0 ||
        reconciled.failed > 0))
  ) {
    logger.warn(
      {
        promotedScheduledRetries: promotion?.promoted ?? 0,
        promotedScheduledRetryRunIds: promotion?.runIds ?? [],
        ...reconciled,
      },
      "startup heartbeat recovery changed assigned issue state",
    );
  }

  const dependencyWakesReconciled = await step("reconcileResolvedDependencyWakes", () =>
    heartbeat.reconcileResolvedDependencyWakes());
  if (dependencyWakesReconciled && dependencyWakesReconciled.healed > 0) {
    logger.warn(
      { ...dependencyWakesReconciled },
      "startup dependency-wake reconciliation restored task execution paths",
    );
  }

  const taskWatchdogsReconciled = await step("reconcileTaskWatchdogs", () =>
    heartbeat.reconcileTaskWatchdogs());
  if (taskWatchdogsReconciled && taskWatchdogsReconciled.triggered > 0) {
    logger.warn(
      { ...taskWatchdogsReconciled },
      "startup task-watchdog reconciliation triggered watchdog work",
    );
  }

  const scanned = await step("scanSilentActiveRuns", () =>
    heartbeat.scanSilentActiveRuns());
  if (scanned && (scanned.created > 0 || scanned.escalated > 0)) {
    logger.warn({ ...scanned }, "startup active-run output watchdog created review work");
  }

  const swept = await step("sweepStaleIssueLocks", () =>
    heartbeat.sweepStaleIssueLocks());
  if (swept && swept.cleared > 0) {
    logger.warn({ ...swept }, "startup stale-lock sweeper cleared issue locks");
  }
}
