import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, timestamp, jsonb, serial, real } from "drizzle-orm/pg-core";
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
  leadsWithEmail: integer("leads_with_email").default(0),
  apifySpendUsd: real("apify_spend_usd").default(0),
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
  status: text("status").notNull().default("new"),
  firstSeenAt: timestamp("first_seen_at").defaultNow().notNull(),
  lastSeenAt: timestamp("last_seen_at").defaultNow().notNull(),
  apolloEnrichedAt: timestamp("apollo_enriched_at"),
  apolloInputHash: text("apollo_input_hash"),
  emailValidation: text("email_validation").default(""),
  source: text("source").default(""),
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
  { id: "patreon", label: "Patreon Creators", description: "Creators with patron counts & tiers" },
  { id: "podcast", label: "Podcasters", description: "Podcast hosts with episode counts & RSS emails" },
  { id: "google", label: "Google Search + Websites", description: "Generic website discovery" },
] as const;

export type SourceId = "meetup" | "youtube" | "reddit" | "eventbrite" | "facebook" | "patreon" | "podcast" | "google";
export const DEFAULT_ENABLED_SOURCES: SourceId[] = ["patreon"];

export const TEMPORARILY_DISABLED_SOURCES: SourceId[] = ["meetup", "youtube", "reddit", "eventbrite", "google"];

export const AVAILABLE_ENRICHMENTS = [
  { id: "apollo", label: "Apollo.io", description: "Contact lookup by name, domain & LinkedIn URL (uses API credits)" },
] as const;

export type EnrichmentId = "apollo";

export const runParamsSchema = z.object({
  seedKeywords: z.array(z.string()).min(1, "At least one keyword is required"),
  seedGeos: z.array(z.string()).default([]),
  maxDiscoveredUrls: z.number().min(1).max(500).default(200),
  maxGoogleResultsPerQuery: z.number().min(1).max(100).default(10),
  enabledSources: z.array(z.enum(["meetup", "youtube", "reddit", "eventbrite", "facebook", "patreon", "podcast", "google"])).min(1, "At least one source must be selected").default(DEFAULT_ENABLED_SOURCES),
  minMemberCount: z.number().min(0).default(0),
  maxMemberCount: z.number().min(0).default(0),
  minPostCount: z.number().min(0).default(0),
  minEpisodeCount: z.number().min(0).default(0),
  podcastCountry: z.string().default("US"),
  enableApollo: z.boolean().default(true),
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
  { label: "Travel creators", keywords: ["travel creator", "adventure trips", "group travel"] },
  { label: "Hiking & outdoors", keywords: ["hiking", "backpacking", "outdoor adventure"] },
  { label: "Yoga & wellness", keywords: ["yoga", "wellness retreat", "mindfulness"] },
  { label: "Fitness coaches", keywords: ["fitness coach", "personal trainer", "workout"] },
  { label: "Photography", keywords: ["photography", "photo tours", "travel photography"] },
  { label: "Women's community", keywords: ["women community", "sisterhood", "women empowerment"] },
  { label: "Run clubs", keywords: ["running", "marathon", "trail running"] },
  { label: "Cycling", keywords: ["cycling", "bike touring", "gravel cycling"] },
  { label: "Food & wine", keywords: ["food travel", "wine tours", "culinary"] },
  { label: "Surf & water sports", keywords: ["surfing", "diving", "water sports"] },
  { label: "Digital nomads", keywords: ["digital nomad", "remote work", "travel lifestyle"] },
  { label: "Spiritual leaders", keywords: ["spiritual retreat", "pilgrimage", "faith ministry"] },
  { label: "Art & creativity", keywords: ["art retreat", "creative workshop", "painting"] },
  { label: "Book clubs", keywords: ["book club", "reading community", "literary"] },
  { label: "Solo travelers", keywords: ["solo travel", "group trips", "adventure travel"] },
  { label: "Climbing", keywords: ["climbing", "mountaineering", "bouldering"] },
  { label: "Camping & van life", keywords: ["camping", "van life", "road trip"] },
  { label: "Nature & wildlife", keywords: ["wildlife safari", "birdwatching", "nature"] },
] as const;

export const FB_RECOMMENDED_KEYWORDS = [
  { label: "Hiking groups", keywords: ["hiking group", "hiking club", "trail hiking"] },
  { label: "Run clubs", keywords: ["run club", "running group", "marathon training"] },
  { label: "Church groups", keywords: ["church group", "young adults ministry", "faith community"] },
  { label: "Adventure travel", keywords: ["adventure travel group", "group travel", "travel club"] },
  { label: "Women's groups", keywords: ["women's group", "women's hiking", "women's travel"] },
  { label: "Yoga & wellness", keywords: ["yoga group", "wellness community", "meditation group"] },
  { label: "Cycling clubs", keywords: ["cycling club", "bike group", "cycling group"] },
  { label: "Outdoor adventure", keywords: ["outdoor adventure club", "camping group", "backpacking group"] },
  { label: "Social clubs", keywords: ["social club", "meetup group", "networking group"] },
  { label: "Fitness groups", keywords: ["fitness group", "CrossFit community", "workout group"] },
  { label: "Book clubs", keywords: ["book club", "reading group", "literary club"] },
  { label: "Alumni groups", keywords: ["alumni group", "alumni network", "alumni association"] },
  { label: "Surf & water sports", keywords: ["surf club", "diving group", "kayaking group"] },
  { label: "Photography clubs", keywords: ["photography club", "photo walk group", "camera club"] },
  { label: "Food & wine", keywords: ["food group", "wine club", "supper club"] },
] as const;

export const PODCAST_RECOMMENDED_KEYWORDS = [
  { label: "Travel & adventure", keywords: ["travel podcast", "adventure travel podcast", "group travel"] },
  { label: "Fitness & wellness", keywords: ["fitness podcast", "wellness podcast", "health podcast"] },
  { label: "Hiking & outdoors", keywords: ["hiking podcast", "outdoor adventure podcast", "trails"] },
  { label: "Yoga & mindfulness", keywords: ["yoga podcast", "mindfulness podcast", "meditation"] },
  { label: "Running & endurance", keywords: ["running podcast", "marathon podcast", "trail running"] },
  { label: "Cycling", keywords: ["cycling podcast", "bike touring", "gravel cycling podcast"] },
  { label: "Women's empowerment", keywords: ["women empowerment podcast", "women community", "sisterhood"] },
  { label: "Photography", keywords: ["photography podcast", "travel photography", "photo tours"] },
  { label: "Food & culinary", keywords: ["food podcast", "culinary travel", "wine podcast"] },
  { label: "Spiritual & faith", keywords: ["spiritual podcast", "faith podcast", "ministry podcast"] },
  { label: "Solo travel", keywords: ["solo travel podcast", "backpacking podcast", "nomad podcast"] },
  { label: "Surf & water sports", keywords: ["surfing podcast", "diving podcast", "water sports"] },
  { label: "Camping & van life", keywords: ["camping podcast", "van life podcast", "road trip"] },
  { label: "Nature & wildlife", keywords: ["nature podcast", "wildlife podcast", "birdwatching"] },
  { label: "Book clubs", keywords: ["book club podcast", "reading podcast", "literary podcast"] },
] as const;

export const DEFAULT_RUN_PARAMS: RunParams = {
  seedKeywords: [],
  seedGeos: ["United States"],
  maxDiscoveredUrls: 200,
  maxGoogleResultsPerQuery: 10,
  enabledSources: [...DEFAULT_ENABLED_SOURCES],
  minMemberCount: 0,
  maxMemberCount: 0,
  minPostCount: 0,
  minEpisodeCount: 0,
  podcastCountry: "US",
  enableApollo: true,
};
