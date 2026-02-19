import { eq, desc, and, sql } from "drizzle-orm";
import { db } from "./db";
import {
  runs, sourceUrls, communities, leaders, leads,
  type Run, type InsertRun,
  type SourceUrl, type InsertSourceUrl,
  type Community, type InsertCommunity,
  type Leader, type InsertLeader,
  type Lead, type InsertLead,
} from "@shared/schema";

export interface IStorage {
  createRun(data: InsertRun): Promise<Run>;
  getRun(id: number): Promise<Run | undefined>;
  listRuns(): Promise<Run[]>;
  updateRun(id: number, data: Partial<Run>): Promise<Run | undefined>;
  incrementApifySpend(runId: number, costUsd: number): Promise<void>;

  createSourceUrl(data: InsertSourceUrl): Promise<SourceUrl>;
  createSourceUrls(data: InsertSourceUrl[]): Promise<void>;
  getSourceUrlsByRun(runId: number): Promise<SourceUrl[]>;
  getNewSourceUrlsByRun(runId: number): Promise<SourceUrl[]>;
  updateSourceUrl(id: number, data: Partial<SourceUrl>): Promise<void>;
  countSourceUrlsByRun(runId: number): Promise<number>;

  createCommunity(data: InsertCommunity): Promise<Community>;
  findCommunityByWebsite(website: string): Promise<Community | undefined>;

  createLeader(data: InsertLeader): Promise<Leader>;

  createLead(data: InsertLead): Promise<Lead>;
  findLeadByEmail(email: string): Promise<Lead | undefined>;
  findLeadByWebsite(website: string): Promise<Lead | undefined>;
  findLeadByNameAndLocation(name: string, location: string): Promise<Lead | undefined>;
  updateLead(id: number, data: Partial<Lead>): Promise<void>;
  listLeads(): Promise<Lead[]>;
  listLeadsByRun(runId: number): Promise<Lead[]>;
  listLeadsByRunAndStatus(runId: number, status: string): Promise<Lead[]>;
  listLeadsByStatus(status: string): Promise<Lead[]>;
  countLeadsByRunAndStatus(runId: number, status: string): Promise<number>;
  countLeadsByRunWithEmail(runId: number): Promise<number>;
  deleteLeadsByRun(runId: number): Promise<void>;
  deleteSourceUrlsByRun(runId: number): Promise<void>;
}

export class DatabaseStorage implements IStorage {
  async createRun(data: InsertRun): Promise<Run> {
    const [run] = await db.insert(runs).values(data).returning();
    return run;
  }

  async getRun(id: number): Promise<Run | undefined> {
    const [run] = await db.select().from(runs).where(eq(runs.id, id));
    return run;
  }

  async listRuns(): Promise<Run[]> {
    return db.select().from(runs).orderBy(desc(runs.createdAt));
  }

  async updateRun(id: number, data: Partial<Run>): Promise<Run | undefined> {
    const [run] = await db.update(runs).set(data).where(eq(runs.id, id)).returning();
    return run;
  }

  async incrementApifySpend(runId: number, costUsd: number): Promise<void> {
    if (costUsd <= 0) return;
    await db.update(runs)
      .set({ apifySpendUsd: sql`COALESCE(${runs.apifySpendUsd}, 0) + ${costUsd}` })
      .where(eq(runs.id, runId));
  }

  async createSourceUrl(data: InsertSourceUrl): Promise<SourceUrl> {
    const [su] = await db.insert(sourceUrls).values(data).returning();
    return su;
  }

  async createSourceUrls(data: InsertSourceUrl[]): Promise<void> {
    if (data.length === 0) return;
    const batchSize = 100;
    for (let i = 0; i < data.length; i += batchSize) {
      await db.insert(sourceUrls).values(data.slice(i, i + batchSize));
    }
  }

  async getSourceUrlsByRun(runId: number): Promise<SourceUrl[]> {
    return db.select().from(sourceUrls).where(eq(sourceUrls.runId, runId));
  }

  async getNewSourceUrlsByRun(runId: number): Promise<SourceUrl[]> {
    return db.select().from(sourceUrls).where(
      and(eq(sourceUrls.runId, runId), eq(sourceUrls.fetchStatus, "new"))
    );
  }

  async updateSourceUrl(id: number, data: Partial<SourceUrl>): Promise<void> {
    await db.update(sourceUrls).set(data).where(eq(sourceUrls.id, id));
  }

  async countSourceUrlsByRun(runId: number): Promise<number> {
    const [result] = await db.select({ count: sql<number>`count(*)::int` }).from(sourceUrls).where(eq(sourceUrls.runId, runId));
    return result?.count || 0;
  }

  async createCommunity(data: InsertCommunity): Promise<Community> {
    const [c] = await db.insert(communities).values(data).returning();
    return c;
  }

  async findCommunityByWebsite(website: string): Promise<Community | undefined> {
    const [c] = await db.select().from(communities).where(eq(communities.website, website));
    return c;
  }

  async createLeader(data: InsertLeader): Promise<Leader> {
    const [l] = await db.insert(leaders).values(data).returning();
    return l;
  }

  async createLead(data: InsertLead): Promise<Lead> {
    const [l] = await db.insert(leads).values(data).returning();
    return l;
  }

  async findLeadByEmail(email: string): Promise<Lead | undefined> {
    const [l] = await db.select().from(leads).where(eq(leads.email, email));
    return l;
  }

  async findLeadByWebsite(website: string): Promise<Lead | undefined> {
    const [l] = await db.select().from(leads).where(eq(leads.website, website));
    return l;
  }

  async findLeadByNameAndLocation(name: string, location: string): Promise<Lead | undefined> {
    const [l] = await db.select().from(leads).where(
      and(eq(leads.communityName, name), eq(leads.location, location))
    );
    return l;
  }

  async updateLead(id: number, data: Partial<Lead>): Promise<void> {
    await db.update(leads).set(data).where(eq(leads.id, id));
  }

  async listLeads(): Promise<Lead[]> {
    return db.select().from(leads).orderBy(desc(leads.score));
  }

  async listLeadsByRun(runId: number): Promise<Lead[]> {
    return db.select().from(leads).where(eq(leads.runId, runId)).orderBy(desc(leads.score));
  }

  async listLeadsByRunAndStatus(runId: number, status: string): Promise<Lead[]> {
    return db.select().from(leads).where(
      and(eq(leads.runId, runId), eq(leads.status, status))
    ).orderBy(desc(leads.score));
  }

  async listLeadsByStatus(status: string): Promise<Lead[]> {
    return db.select().from(leads).where(eq(leads.status, status)).orderBy(desc(leads.score));
  }

  async countLeadsByRunAndStatus(runId: number, status: string): Promise<number> {
    const [result] = await db.select({ count: sql<number>`count(*)::int` }).from(leads).where(
      and(eq(leads.runId, runId), eq(leads.status, status))
    );
    return result?.count || 0;
  }

  async countLeadsByRunWithEmail(runId: number): Promise<number> {
    const [result] = await db.select({ count: sql<number>`count(*)::int` }).from(leads).where(
      and(eq(leads.runId, runId), sql`${leads.email} IS NOT NULL AND ${leads.email} != ''`)
    );
    return result?.count || 0;
  }

  async deleteLeadsByRun(runId: number): Promise<void> {
    await db.delete(leads).where(eq(leads.runId, runId));
  }

  async deleteSourceUrlsByRun(runId: number): Promise<void> {
    await db.delete(sourceUrls).where(eq(sourceUrls.runId, runId));
  }
}

export const storage = new DatabaseStorage();
