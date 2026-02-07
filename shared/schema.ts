import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, timestamp, jsonb, serial } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const runs = pgTable("runs", {
  id: serial("id").primaryKey(),
  status: text("status").notNull().default("queued"),
  progress: integer("progress").notNull().default(0),
  step: text("step").default(""),
  logs: text("logs").default(""),
  params: jsonb("params").$type<RunParams>(),
  startedAt: timestamp("started_at"),
  finishedAt: timestamp("finished_at"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  urlsDiscovered: integer("urls_discovered").default(0),
  leadsExtracted: integer("leads_extracted").default(0),
  qualified: integer("qualified").default(0),
  watchlist: integer("watchlist").default(0),
});

export const sourceUrls = pgTable("source_urls", {
  id: serial("id").primaryKey(),
  url: text("url").notNull(),
  domain: text("domain").default(""),
  source: text("source").notNull().default("google"),
  discoveredAt: timestamp("discovered_at").defaultNow().notNull(),
  fetchedAt: timestamp("fetched_at"),
  fetchStatus: text("fetch_status").notNull().default("new"),
  runId: integer("run_id").references(() => runs.id),
});

export const communities = pgTable("communities", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  type: text("type").default("other"),
  description: text("description").default(""),
  location: text("location").default(""),
  website: text("website").default(""),
  ownedChannels: jsonb("owned_channels").$type<Record<string, string>>().default({}),
  eventCadence: jsonb("event_cadence").$type<Record<string, any>>().default({}),
  audienceSignals: jsonb("audience_signals").$type<Record<string, any>>().default({}),
  sourceUrls: jsonb("source_urls_list").$type<string[]>().default([]),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
});

export const leaders = pgTable("leaders", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  role: text("role").default(""),
  email: text("email").default(""),
  phone: text("phone").default(""),
  linkedin: text("linkedin").default(""),
  sourceUrl: text("source_url").default(""),
  communityId: integer("community_id").references(() => communities.id),
});

export const leads = pgTable("leads", {
  id: serial("id").primaryKey(),
  leadType: text("lead_type").notNull().default("community"),
  communityName: text("community_name").default(""),
  communityType: text("community_type").default(""),
  leaderName: text("leader_name").default(""),
  leaderRole: text("leader_role").default(""),
  location: text("location").default(""),
  website: text("website").default(""),
  email: text("email").default(""),
  phone: text("phone").default(""),
  linkedin: text("linkedin").default(""),
  ownedChannels: jsonb("owned_channels").$type<Record<string, string>>().default({}),
  monetizationSignals: jsonb("monetization_signals").$type<Record<string, any>>().default({}),
  engagementSignals: jsonb("engagement_signals").$type<Record<string, any>>().default({}),
  tripFitSignals: jsonb("trip_fit_signals").$type<Record<string, any>>().default({}),
  score: integer("score").default(0),
  scoreBreakdown: jsonb("score_breakdown").$type<ScoreBreakdown>(),
  status: text("status").notNull().default("watchlist"),
  firstSeenAt: timestamp("first_seen_at").defaultNow().notNull(),
  lastSeenAt: timestamp("last_seen_at").defaultNow().notNull(),
  raw: jsonb("raw").$type<Record<string, any>>().default({}),
  runId: integer("run_id").references(() => runs.id),
  communityId: integer("community_id").references(() => communities.id),
  leaderId: integer("leader_id").references(() => leaders.id),
});

export const runParamsSchema = z.object({
  seedKeywords: z.array(z.string()).min(1, "At least one keyword is required"),
  seedGeos: z.array(z.string()).default([]),
  communityTypes: z.array(z.string()).min(1).default(["church", "run_club", "hiking", "social_club"]),
  intentTerms: z.array(z.string()).min(1).default(["retreat", "trip", "group travel"]),
  sources: z.record(z.boolean()).default({}),
  threshold: z.number().min(0).max(100).default(65),
  maxDiscoveredUrls: z.number().min(1).max(5000).default(200),
  maxGoogleResultsPerQuery: z.number().min(1).max(100).default(10),
  maxCrawlPagesPerSite: z.number().min(1).max(10).default(3),
});

export type RunParams = z.infer<typeof runParamsSchema>;

export interface RunParamsLegacy {
  seedKeywords: string[];
  seedGeos: string[];
  communityTypes: string[];
  intentTerms: string[];
  sources: Record<string, boolean>;
  threshold: number;
  maxDiscoveredUrls: number;
  maxGoogleResultsPerQuery: number;
  maxCrawlPagesPerSite: number;
}

export interface ScoreBreakdown {
  nicheIdentity: number;
  trustLeadership: number;
  engagement: number;
  monetization: number;
  ownedChannels: number;
  tripFit: number;
  penalties: number;
  total: number;
}

export const insertRunSchema = createInsertSchema(runs).omit({ id: true, createdAt: true });
export const insertSourceUrlSchema = createInsertSchema(sourceUrls).omit({ id: true, discoveredAt: true });
export const insertCommunitySchema = createInsertSchema(communities).omit({ id: true, createdAt: true, updatedAt: true });
export const insertLeaderSchema = createInsertSchema(leaders).omit({ id: true });
export const insertLeadSchema = createInsertSchema(leads).omit({ id: true, firstSeenAt: true, lastSeenAt: true });

export type InsertRun = z.infer<typeof insertRunSchema>;
export type Run = typeof runs.$inferSelect;
export type InsertSourceUrl = z.infer<typeof insertSourceUrlSchema>;
export type SourceUrl = typeof sourceUrls.$inferSelect;
export type InsertCommunity = z.infer<typeof insertCommunitySchema>;
export type Community = typeof communities.$inferSelect;
export type InsertLeader = z.infer<typeof insertLeaderSchema>;
export type Leader = typeof leaders.$inferSelect;
export type InsertLead = z.infer<typeof insertLeadSchema>;
export type Lead = typeof leads.$inferSelect;

export const COMMUNITY_TYPES = [
  { value: "church", label: "Churches / Ministries" },
  { value: "run_club", label: "Run Clubs" },
  { value: "hiking", label: "Hiking / Outdoors" },
  { value: "social_club", label: "Social Clubs" },
  { value: "book_club", label: "Book Clubs" },
  { value: "professional", label: "Professional Orgs" },
  { value: "alumni", label: "Alumni Chapters" },
  { value: "nonprofit", label: "Nonprofits" },
  { value: "fitness", label: "Fitness Studios" },
  { value: "coworking", label: "Coworking" },
  { value: "other", label: "Other" },
] as const;

export const INTENT_TERMS = [
  "retreat",
  "trip",
  "travel",
  "pilgrimage",
  "mission trip",
  "group travel",
  "conference travel",
  "tours",
  "excursions",
] as const;

export const SOURCE_CONNECTORS = [
  { key: "google", label: "Google Discovery", required: true },
  { key: "meetup", label: "Meetup" },
  { key: "eventbrite", label: "Eventbrite" },
  { key: "website", label: "Website Crawl" },
  { key: "youtube", label: "YouTube" },
  { key: "substack", label: "Substack" },
  { key: "patreon", label: "Patreon" },
  { key: "reddit", label: "Reddit" },
  { key: "facebook_page", label: "Facebook Pages (public)" },
] as const;

export const DEFAULT_RUN_PARAMS: RunParams = {
  seedKeywords: ["community group", "local club", "church group"],
  seedGeos: [],
  communityTypes: ["church", "run_club", "hiking", "social_club"],
  intentTerms: ["retreat", "trip", "group travel"],
  sources: {
    google: true,
    meetup: true,
    eventbrite: true,
    website: true,
    youtube: false,
    substack: false,
    patreon: false,
    reddit: false,
    facebook_page: false,
  },
  threshold: 65,
  maxDiscoveredUrls: 200,
  maxGoogleResultsPerQuery: 10,
  maxCrawlPagesPerSite: 3,
};
