import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, timestamp, jsonb, serial, real, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const PIPELINE_STEPS = {
  DISCOVERY: "discovery",
  FB_GOOGLE_BRIDGE: "fb_google_bridge",
  RSS_FEEDS: "rss_feeds",
  LINK_AGGREGATORS_1: "link_aggregators_1",
  YOUTUBE_ABOUT: "youtube_about",
  INSTAGRAM_BIOS: "instagram_bios",
  TWITTER_BIOS: "twitter_bios",
  LINK_AGGREGATORS_2: "link_aggregators_2",
  GOOGLE_CONTACT_SEARCH: "google_contact_search",
  SLUG_DOMAIN_PROBE: "slug_domain_probe",
  WEBSITE_CRAWL: "website_crawl",
  GOOGLE_URL_DISCOVERY: "google_url_discovery",
  WEBSITE_EXTRACTION: "website_extraction",
  LEAD_CREATION: "lead_creation",
  APOLLO: "apollo",
  LEADS_FINDER: "leads_finder",
  EMAIL_VALIDATION: "email_validation",
  SCORING: "scoring",
} as const;

export type PipelineStep = typeof PIPELINE_STEPS[keyof typeof PIPELINE_STEPS];

export const PIPELINE_STEP_LABELS: Record<PipelineStep, string> = {
  [PIPELINE_STEPS.DISCOVERY]: "Platform Discovery",
  [PIPELINE_STEPS.FB_GOOGLE_BRIDGE]: "Facebook Google Bridge",
  [PIPELINE_STEPS.RSS_FEEDS]: "RSS Feed Extraction",
  [PIPELINE_STEPS.LINK_AGGREGATORS_1]: "Link Aggregator Scrape",
  [PIPELINE_STEPS.YOUTUBE_ABOUT]: "YouTube About Pages",
  [PIPELINE_STEPS.INSTAGRAM_BIOS]: "Instagram Bio Scrape",
  [PIPELINE_STEPS.TWITTER_BIOS]: "Twitter Bio Scrape",
  [PIPELINE_STEPS.LINK_AGGREGATORS_2]: "Link Aggregator Scrape (Pass 2)",
  [PIPELINE_STEPS.GOOGLE_CONTACT_SEARCH]: "Google Contact Search",
  [PIPELINE_STEPS.SLUG_DOMAIN_PROBE]: "Slug Domain Probe",
  [PIPELINE_STEPS.WEBSITE_CRAWL]: "Website Contact Crawl",
  [PIPELINE_STEPS.GOOGLE_URL_DISCOVERY]: "Google URL Discovery",
  [PIPELINE_STEPS.WEBSITE_EXTRACTION]: "Website Data Extraction",
  [PIPELINE_STEPS.LEAD_CREATION]: "Lead Creation",
  [PIPELINE_STEPS.APOLLO]: "Apollo Enrichment",
  [PIPELINE_STEPS.LEADS_FINDER]: "Leads Finder Enrichment",
  [PIPELINE_STEPS.EMAIL_VALIDATION]: "Email Validation",
  [PIPELINE_STEPS.SCORING]: "Scoring",
};

export const runs = pgTable("runs", {
  id: serial("id").primaryKey(),
  status: text("status").notNull().default("queued"),
  progress: integer("progress").notNull().default(0),
  step: text("step").default(""),
  logs: text("logs").default(""),
  params: jsonb("params").$type<RunParams>(),
  lastCompletedStep: text("last_completed_step").default(""),
  startedAt: timestamp("started_at"),
  finishedAt: timestamp("finished_at"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  urlsDiscovered: integer("urls_discovered").default(0),
  leadsExtracted: integer("leads_extracted").default(0),
  leadsWithEmail: integer("leads_with_email").default(0),
  leadsWithValidEmail: integer("leads_with_valid_email").default(0),
  apifySpendUsd: real("apify_spend_usd").default(0),
  isAutonomous: boolean("is_autonomous").default(false),
  budgetUsd: real("budget_usd").default(0),
  budgetAllocation: jsonb("budget_allocation").$type<BudgetAllocation>(),
  emailTarget: integer("email_target").default(0),
  podcastEnabled: boolean("podcast_enabled").default(true),
  checkpoint: jsonb("checkpoint").$type<PipelineCheckpoint>(),
  completedSubSteps: text("completed_sub_steps").array().default([]),
  lastHeartbeat: timestamp("last_heartbeat"),
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
  firstName: text("first_name").default(""),
  researchSummary: text("research_summary").default(""),
  hubspotStatus: text("hubspot_status").default(""),
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
  { id: "substack", label: "Substack Writers", description: "Newsletter writers with subscriber bases & public emails" },
  { id: "mighty", label: "Mighty Networks", description: "Community builders with engaged, paying members" },
  { id: "linkedin", label: "LinkedIn Groups", description: "Professional community leaders & alumni network organizers" },
  { id: "google", label: "Google Search + Websites", description: "Generic website discovery" },
] as const;

export type SourceId = "meetup" | "youtube" | "reddit" | "eventbrite" | "facebook" | "patreon" | "podcast" | "substack" | "mighty" | "linkedin" | "google";
export const DEFAULT_ENABLED_SOURCES: SourceId[] = ["patreon"];

export const TEMPORARILY_DISABLED_SOURCES: SourceId[] = ["youtube", "reddit", "eventbrite", "google"];

export const AVAILABLE_ENRICHMENTS = [
  { id: "apollo", label: "Apollo.io", description: "Contact lookup by name, domain & LinkedIn URL (uses API credits)" },
] as const;

export type EnrichmentId = "apollo";

export const runParamsSchema = z.object({
  seedKeywords: z.array(z.string()).min(1, "At least one keyword is required"),
  seedGeos: z.array(z.string()).default([]),
  maxDiscoveredUrls: z.number().min(1).max(500).default(200),
  maxGoogleResultsPerQuery: z.number().min(1).max(100).default(10),
  enabledSources: z.array(z.enum(["meetup", "youtube", "reddit", "eventbrite", "facebook", "patreon", "podcast", "substack", "mighty", "linkedin", "google"])).min(1, "At least one source must be selected").default(DEFAULT_ENABLED_SOURCES),
  minMemberCount: z.number().min(0).default(0),
  maxMemberCount: z.number().min(0).default(0),
  minPostCount: z.number().min(0).default(0),
  minEpisodeCount: z.number().min(0).default(0),
  podcastCountry: z.string().default("US"),
  enableApollo: z.boolean().default(true),
});

export type RunParams = z.infer<typeof runParamsSchema>;

export interface PlatformAllocation {
  platform: SourceId;
  maxLeads: number;
  estimatedCostUsd: number;
  costPerLead: number;
}

export interface PipelineCheckpoint {
  platformLeads: any[];
  completedSubSteps: string[];
  apifySpendAtCheckpoint: number;
  intraStepProgress?: Record<string, number[]>;
}

export interface BudgetAllocation {
  totalBudgetUsd: number;
  discoveryBudgetUsd: number;
  enrichmentBudgetUsd: number;
  platforms: PlatformAllocation[];
  estimatedTotalLeads: number;
  estimatedEmailRate: number;
  estimatedEmails: number;
  estimatedValidEmails: number;
  estimatedValidEmailRate: number;
}

export const PLATFORM_COST_PER_LEAD: Record<string, number> = {
  patreon: 0.03,
  facebook: 0.01,
  podcast: 0.03,
  substack: 0.01,
  meetup: 0.01,
  mighty: 0.01,
  linkedin: 0.01,
};

export const PLATFORM_EMAIL_YIELD: Record<string, number> = {
  patreon: 0.35,
  facebook: 0.15,
  podcast: 0.55,
  substack: 0.40,
  meetup: 0.20,
  mighty: 0.30,
  linkedin: 0.25,
};

export const PLATFORM_VALID_EMAIL_RATE: Record<string, number> = {
  patreon: 0.50,
  facebook: 0.36,
  podcast: 0.45,
  substack: 0.21,
  meetup: 0.40,
  mighty: 0.35,
  linkedin: 0.45,
};

export const KEYWORD_PLATFORM_MAP: Record<string, SourceId[]> = {
  "podcast": ["podcast"],
  "patreon": ["patreon"],
  "newsletter": ["substack"],
  "substack": ["substack"],
  "facebook group": ["facebook"],
  "fb group": ["facebook"],
  "linkedin group": ["linkedin"],
  "church": ["facebook", "patreon", "meetup"],
  "ministry": ["facebook", "patreon"],
  "faith": ["facebook", "patreon", "meetup"],
  "run club": ["facebook", "patreon", "meetup"],
  "running": ["facebook", "patreon", "podcast", "meetup", "linkedin"],
  "hiking": ["facebook", "patreon", "podcast", "meetup"],
  "cycling": ["facebook", "patreon", "podcast", "meetup"],
  "alumni": ["facebook", "meetup", "linkedin"],
  "social club": ["facebook", "meetup"],
  "meetup": ["meetup"],
  "yoga": ["patreon", "meetup", "podcast", "mighty"],
  "fitness": ["patreon", "meetup", "podcast", "mighty", "linkedin"],
  "outdoor": ["facebook", "meetup", "patreon", "mighty"],
  "photography": ["meetup", "patreon", "podcast"],
  "book club": ["meetup", "facebook", "mighty"],
  "tech": ["meetup", "mighty", "linkedin"],
  "networking": ["meetup", "facebook", "mighty", "linkedin"],
  "community": ["mighty", "facebook", "meetup"],
  "coaching": ["mighty", "patreon", "podcast", "linkedin"],
  "wellness": ["mighty", "patreon", "podcast"],
  "leadership": ["mighty", "facebook", "linkedin"],
  "membership": ["mighty", "patreon"],
  "professional": ["linkedin", "meetup"],
  "industry": ["linkedin"],
  "association": ["linkedin", "facebook"],
};

export const autonomousParamsSchema = z.object({
  seedKeywords: z.array(z.string()).min(1, "At least one keyword is required"),
  budgetUsd: z.number().min(0.5).max(20).optional(),
  emailTarget: z.number().min(1).max(500).optional(),
  podcastEnabled: z.boolean().default(true),
  seedGeos: z.array(z.string()).default(["United States"]),
});

export type AutonomousParams = z.infer<typeof autonomousParamsSchema>;

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

export const hostProfiles = pgTable("host_profiles", {
  id: serial("id").primaryKey(),
  hubspotContactId: text("hubspot_contact_id").notNull(),
  email: text("email").default(""),
  name: text("name").default(""),
  confirmedTrips: integer("confirmed_trips").default(0),
  totalDeals: integer("total_deals").default(0),
  traits: jsonb("traits").$type<HostTraits>(),
  matchedLeadId: integer("matched_lead_id"),
  syncedAt: timestamp("synced_at").defaultNow().notNull(),
});

export interface HostTraits {
  communityType?: string;
  source?: string;
  audienceSize?: number;
  platformCount?: number;
  platforms?: string[];
  hasWebsite?: boolean;
  hasNewsletter?: boolean;
  hasYoutube?: boolean;
  hasPodcast?: boolean;
  hasInstagram?: boolean;
  hasLinkedin?: boolean;
  monetizationSignalCount?: number;
  tripFitSignalCount?: number;
  engagementSignalCount?: number;
  score?: number;
  nicheIdentity?: number;
  trustLeadership?: number;
  engagement?: number;
  monetization?: number;
  ownedChannels?: number;
  tripFit?: number;
  keywords?: string[];
  jobTitle?: string;
  company?: string;
  location?: string;
  dealNames?: string[];
  website?: string;
}

export const scoringWeights = pgTable("scoring_weights", {
  id: serial("id").primaryKey(),
  weights: jsonb("weights").$type<LearnedWeights>(),
  sampleSize: integer("sample_size").default(0),
  topHostCount: integer("top_host_count").default(0),
  computedAt: timestamp("computed_at").defaultNow().notNull(),
  insights: jsonb("insights").$type<ScoringInsights>(),
});

export interface LearnedWeights {
  nicheIdentity: number;
  trustLeadership: number;
  engagement: number;
  monetization: number;
  ownedChannels: number;
  tripFit: number;
}

export interface ScoringInsights {
  topTraits: { trait: string; prevalence: number }[];
  topPlatforms: { platform: string; count: number }[];
  topCommunityTypes: { type: string; count: number }[];
  avgAudienceSize: number;
  avgPlatformCount: number;
  avgScore: number;
  suggestedKeywords: { keyword: string; score: number }[];
  topJobTitles?: { title: string; count: number }[];
  topLocations?: { location: string; count: number }[];
  topDealKeywords?: { keyword: string; count: number }[];
  topCompanies?: { company: string; count: number }[];
  avgConfirmedTrips?: number;
}

export const insertHostProfileSchema = createInsertSchema(hostProfiles).omit({ id: true, syncedAt: true });
export type InsertHostProfile = z.infer<typeof insertHostProfileSchema>;
export type HostProfile = typeof hostProfiles.$inferSelect;

export const insertScoringWeightsSchema = createInsertSchema(scoringWeights).omit({ id: true, computedAt: true });
export type InsertScoringWeights = z.infer<typeof insertScoringWeightsSchema>;
export type ScoringWeightsRow = typeof scoringWeights.$inferSelect;

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

export const SUBSTACK_RECOMMENDED_KEYWORDS = [
  { label: "Travel & adventure", keywords: ["travel", "adventure travel", "group travel"] },
  { label: "Hiking & outdoors", keywords: ["hiking", "outdoor adventure", "trails"] },
  { label: "Yoga & wellness", keywords: ["yoga", "wellness", "mindfulness retreat"] },
  { label: "Fitness coaching", keywords: ["fitness", "personal training", "workout"] },
  { label: "Women's community", keywords: ["women community", "sisterhood", "women empowerment"] },
  { label: "Running & endurance", keywords: ["running", "marathon", "trail running"] },
  { label: "Cycling", keywords: ["cycling", "bike touring", "gravel cycling"] },
  { label: "Food & wine", keywords: ["food travel", "wine", "culinary"] },
  { label: "Photography", keywords: ["photography", "travel photography", "photo tours"] },
  { label: "Spiritual & faith", keywords: ["spiritual", "faith", "ministry"] },
  { label: "Book clubs", keywords: ["book club", "reading", "literary"] },
  { label: "Solo travel", keywords: ["solo travel", "backpacking", "nomad"] },
  { label: "Nature & wildlife", keywords: ["nature", "wildlife", "birdwatching"] },
  { label: "Surf & water sports", keywords: ["surfing", "diving", "water sports"] },
  { label: "Art & creativity", keywords: ["art", "creative writing", "painting"] },
] as const;

export const MIGHTY_RECOMMENDED_KEYWORDS = [
  { label: "Yoga & wellness", keywords: ["yoga community", "wellness coaching", "mindfulness group"] },
  { label: "Fitness coaching", keywords: ["fitness community", "personal training", "workout group"] },
  { label: "Travel & adventure", keywords: ["travel community", "adventure group", "group travel"] },
  { label: "Women's community", keywords: ["women community", "sisterhood", "women empowerment"] },
  { label: "Running & endurance", keywords: ["running community", "marathon", "endurance"] },
  { label: "Hiking & outdoors", keywords: ["hiking community", "outdoor adventure", "nature group"] },
  { label: "Coaching & leadership", keywords: ["coaching", "leadership", "mentorship"] },
  { label: "Health & nutrition", keywords: ["health community", "nutrition", "plant-based"] },
  { label: "Photography", keywords: ["photography community", "photo club", "camera"] },
  { label: "Faith & spiritual", keywords: ["faith community", "spiritual", "church group"] },
  { label: "Cycling", keywords: ["cycling community", "bike group", "cycling club"] },
  { label: "Book clubs", keywords: ["book club", "reading community", "literary group"] },
  { label: "Creative arts", keywords: ["art community", "creative", "painting"] },
  { label: "Membership community", keywords: ["membership", "paid community", "online community"] },
  { label: "Surfing & water sports", keywords: ["surf community", "diving", "water sports"] },
] as const;

export const LINKEDIN_RECOMMENDED_KEYWORDS = [
  { label: "Professional networking", keywords: ["professional networking group", "business networking", "industry professionals"] },
  { label: "Alumni networks", keywords: ["alumni group", "alumni network", "alumni association"] },
  { label: "Leadership & coaching", keywords: ["leadership group", "executive coaching", "professional development"] },
  { label: "Travel & adventure", keywords: ["travel group", "adventure travel", "group travel"] },
  { label: "Hiking & outdoors", keywords: ["hiking group", "outdoor adventure", "trail hiking"] },
  { label: "Fitness & wellness", keywords: ["fitness group", "wellness community", "health professionals"] },
  { label: "Running & endurance", keywords: ["running group", "marathon", "trail running"] },
  { label: "Yoga & mindfulness", keywords: ["yoga community", "mindfulness", "wellness retreat"] },
  { label: "Women's professional", keywords: ["women professionals", "women in business", "women leaders"] },
  { label: "Cycling", keywords: ["cycling group", "bike club", "cycling community"] },
  { label: "Photography", keywords: ["photography group", "photographers network", "photo club"] },
  { label: "Food & wine", keywords: ["food lovers", "wine group", "culinary professionals"] },
  { label: "Tech & innovation", keywords: ["tech group", "technology professionals", "startup community"] },
  { label: "Nonprofit & social impact", keywords: ["nonprofit group", "social impact", "community leaders"] },
  { label: "Outdoor recreation", keywords: ["outdoor recreation", "camping group", "nature lovers"] },
] as const;

export const MEETUP_RECOMMENDED_KEYWORDS = [
  { label: "Hiking & outdoors", keywords: ["hiking group", "outdoor adventure", "trail hiking"] },
  { label: "Run clubs", keywords: ["run club", "running group", "marathon training"] },
  { label: "Yoga & wellness", keywords: ["yoga group", "wellness community", "meditation group"] },
  { label: "Cycling clubs", keywords: ["cycling club", "bike group", "cycling group"] },
  { label: "Travel & adventure", keywords: ["adventure travel", "group travel", "travel club"] },
  { label: "Photography", keywords: ["photography club", "photo walk", "camera club"] },
  { label: "Book clubs", keywords: ["book club", "reading group", "literary club"] },
  { label: "Women's groups", keywords: ["women's group", "women's hiking", "women's adventure"] },
  { label: "Fitness groups", keywords: ["fitness group", "CrossFit", "workout group"] },
  { label: "Social & networking", keywords: ["social club", "networking group", "friends meetup"] },
  { label: "Climbing", keywords: ["climbing group", "bouldering", "mountaineering club"] },
  { label: "Camping & van life", keywords: ["camping group", "backpacking group", "van life"] },
  { label: "Food & wine", keywords: ["food group", "wine tasting", "supper club"] },
  { label: "Surf & water sports", keywords: ["surf club", "diving group", "kayaking group"] },
  { label: "Dance & movement", keywords: ["dance group", "salsa meetup", "dance community"] },
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
