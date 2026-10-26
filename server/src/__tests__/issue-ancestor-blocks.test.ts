import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents,
  companies,
  createDb,
  issueRecoveryActions,
  issueRelations,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";
import { runStartupHeartbeatRecovery } from "../services/heartbeat/startup-recovery.ts";
import { issueService } from "../services/issues.ts";
import { logger as appLogger } from "../middleware/logger.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres ancestor-block tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

describeEmbeddedPostgres("ancestor blocks descendant", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("issue-ancestor-blocks-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.execute(sql`truncate table ${companies} cascade`);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedTree() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const prefix = `A${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`;
    await db.insert(companies).values({
      id: companyId,
      name: "Ancestor Block Co",
      status: "active",
      issuePrefix: prefix,
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Ancestor Block Agent",
      role: "engineer",
      status: "idle",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });
    let issueNumber = 0;
    const insertIssue = async (title: string, parentId: string | null, status: "todo" | "in_progress" | "blocked" = "todo") => {
      const id = randomUUID();
      issueNumber += 1;
      await db.insert(issues).values({
        id,
        companyId,
        title,
        status,
        priority: "medium",
        assigneeAgentId: agentId,
        parentId,
        issueNumber,
        identifier: `${prefix}-${issueNumber}`,
      });
      return id;
    };
    const grandparentId = await insertIssue("Grandparent", null);
    const parentId = await insertIssue("Parent", grandparentId);
    const childId = await insertIssue("Child", parentId);
    const otherId = await insertIssue("Unrelated", null);
    return { companyId, agentId, grandparentId, parentId, childId, otherId, insertIssue };
  }

  async function blockersOf(issueId: string) {
    const rows = await db
      .select({ blockerIssueId: issueRelations.issueId })
      .from(issueRelations)
      .where(and(eq(issueRelations.relatedIssueId, issueId), eq(issueRelations.type, "blocks")));
    return rows.map((row) => row.blockerIssueId).sort();
  }

  it("rejects a parent or higher ancestor as a blocker with a 422", async () => {
    const { parentId, grandparentId, childId } = await seedTree();
    const svc = issueService(db);

    await expect(svc.update(childId, { blockedByIssueIds: [parentId] })).rejects.toMatchObject({
      status: 422,
      message: expect.stringContaining("cannot be blocked by its own parent or ancestor"),
    });
    await expect(svc.update(childId, { blockedByIssueIds: [grandparentId] })).rejects.toMatchObject({
      status: 422,
    });
    expect(await blockersOf(childId)).toEqual([]);
  });

  it("still lets a child block its parent and unrelated issues block each other", async () => {
    const { parentId, childId, otherId } = await seedTree();
    const svc = issueService(db);

    await svc.update(parentId, { blockedByIssueIds: [childId] });
    await svc.update(childId, { blockedByIssueIds: [otherId] });

    expect(await blockersOf(parentId)).toEqual([childId]);
    expect(await blockersOf(childId)).toEqual([otherId]);
  });

  it("rejects moving an issue under its own blocker or beneath it", async () => {
    const { otherId, grandparentId, insertIssue } = await seedTree();
    const svc = issueService(db);
    const leafId = await insertIssue("Leaf under unrelated", otherId);
    // The grandparent blocks the unrelated issue's child.
    await svc.update(leafId, { blockedByIssueIds: [grandparentId] });

    // Moving the unrelated issue under the grandparent's tree would make the
    // blocker an ancestor of the blocked leaf.
    const { parentId } = await db
      .select({ parentId: issues.id })
      .from(issues)
      .where(eq(issues.parentId, grandparentId))
      .then((rows) => rows[0]!);
    await expect(svc.update(otherId, { parentId })).rejects.toMatchObject({ status: 422 });
    await expect(svc.update(otherId, { parentId: grandparentId })).rejects.toMatchObject({ status: 422 });
    const [unchanged] = await db.select({ parentId: issues.parentId }).from(issues).where(eq(issues.id, otherId));
    expect(unchanged?.parentId).toBeNull();

    // Dropping the blocker in the same update makes the move valid.
    await svc.update(leafId, { blockedByIssueIds: [] });
    await svc.update(otherId, { parentId: grandparentId });
  });

  it("startup recovery skips an issue whose seeded ancestor block would close a cycle", async () => {
    const { companyId, agentId, parentId, childId } = await seedTree();
    // Legacy bad data written before the write-time check: the parent blocks
    // its own child. Recovery sees the blocked child as healthy work and tries
    // to block the parent on it, which closes a cycle and used to throw out of
    // startup recovery and exit the server.
    await db.insert(issueRelations).values({
      companyId,
      issueId: parentId,
      relatedIssueId: childId,
      type: "blocks",
    });
    await db.update(issues).set({ status: "in_progress" }).where(eq(issues.id, parentId));
    const [action] = await db
      .insert(issueRecoveryActions)
      .values({
        companyId,
        sourceIssueId: parentId,
        kind: "stranded_assigned_issue",
        status: "active",
        ownerType: "agent",
        ownerAgentId: agentId,
        previousOwnerAgentId: agentId,
        returnOwnerAgentId: agentId,
        cause: "process_lost",
        fingerprint: `ancestor-block:${parentId}`,
        evidence: { latestRunId: null },
        nextAction: "Recover the stranded parent",
        wakePolicy: { type: "bounded_recovery_owner", ownerAgentId: agentId, attempt: 1, maxAttempts: 5 },
        attemptCount: 1,
        maxAttempts: 5,
      })
      .returning();

    const heartbeat = heartbeatService(db);
    const serviceErrors = vi.spyOn(appLogger, "error");
    const result = await heartbeat.reconcileStrandedAssignedIssues();
    expect(result.failed).toBeGreaterThanOrEqual(1);
    expect(serviceErrors).toHaveBeenCalledWith(
      expect.objectContaining({ issueId: parentId }),
      expect.stringContaining("skipping it"),
    );

    // The whole startup sequence completes: no step throws, so the server boots.
    const startupLogger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    await expect(runStartupHeartbeatRecovery(heartbeat, startupLogger as never)).resolves.toBeUndefined();
    expect(startupLogger.error).not.toHaveBeenCalledWith(
      expect.anything(),
      "startup heartbeat recovery step failed; continuing with the next step",
    );
    serviceErrors.mockRestore();

    // The bad relation is left for an operator; nothing else was rewritten.
    expect(await blockersOf(childId)).toEqual([parentId]);
    expect(await blockersOf(parentId)).toEqual([]);
    const [persisted] = await db
      .select({ status: issueRecoveryActions.status })
      .from(issueRecoveryActions)
      .where(eq(issueRecoveryActions.id, action!.id));
    expect(persisted?.status).toBe("active");
  });
});
