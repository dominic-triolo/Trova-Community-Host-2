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

export const AVAILABLE_SOURCES = [
  { id: "meetup", label: "Meetup Groups", description: "Community groups with member counts" },
  { id: "youtube", label: "YouTube Channels", description: "Channels with subscriber data" },
  { id: "reddit", label: "Reddit Communities", description: "Subreddits with member counts" },
  { id: "eventbrite", label: "Eventbrite Events", description: "Event organizers with followers" },
  { id: "facebook", label: "Facebook Groups", description: "Public groups with member counts" },
  { id: "google", label: "Google Search + Websites", description: "Generic website discovery" },
] as const;

export type SourceId = "meetup" | "youtube" | "reddit" | "eventbrite" | "facebook" | "google";
export const DEFAULT_ENABLED_SOURCES: SourceId[] = ["meetup", "youtube", "reddit", "eventbrite", "google"];

export const runParamsSchema = z.object({
  seedKeywords: z.array(z.string()).min(1, "At least one keyword is required"),
  seedGeos: z.array(z.string()).default([]),
  threshold: z.number().min(0).max(100).default(65),
  maxDiscoveredUrls: z.number().min(1).max(5000).default(200),
  maxGoogleResultsPerQuery: z.number().min(1).max(100).default(10),
  enabledSources: z.array(z.enum(["meetup", "youtube", "reddit", "eventbrite", "facebook", "google"])).min(1, "At least one source must be selected").default(DEFAULT_ENABLED_SOURCES),
});

export type RunParams = z.infer<typeof runParamsSchema>;

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

export const RECOMMENDED_KEYWORDS = [
  { label: "Church retreat groups", keyword: "church group retreat travel" },
  { label: "Run clubs", keyword: "run club group travel" },
  { label: "Hiking clubs", keyword: "hiking club group trip" },
  { label: "Social clubs", keyword: "social club group outing" },
  { label: "Book clubs", keyword: "book club retreat weekend" },
  { label: "Alumni groups", keyword: "alumni chapter group travel" },
  { label: "Fitness communities", keyword: "CrossFit yoga fitness retreat" },
  { label: "Professional networks", keyword: "professional association group travel" },
  { label: "Women's groups", keyword: "women's group retreat travel" },
  { label: "Volunteer orgs", keyword: "nonprofit volunteer group trip" },
  { label: "Cycling clubs", keyword: "cycling club group tour" },
  { label: "Adventure clubs", keyword: "adventure club outdoor trip" },
  { label: "Photography groups", keyword: "photography club travel workshop" },
  { label: "Wine & food clubs", keyword: "wine club food tour travel" },
  { label: "Meetup organizers", keyword: "meetup organizer group travel" },
  { label: "Coworking retreats", keyword: "coworking retreat remote work trip" },
] as const;

export const DEFAULT_RUN_PARAMS: RunParams = {
  seedKeywords: [],
  seedGeos: [],
  threshold: 65,
  maxDiscoveredUrls: 200,
  maxGoogleResultsPerQuery: 10,
  enabledSources: [...DEFAULT_ENABLED_SOURCES],
};
