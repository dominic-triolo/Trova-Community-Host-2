import type { ScoreBreakdown, LearnedWeights } from "@shared/schema";

let cachedWeights: LearnedWeights | null = null;
let weightsCacheTime = 0;
const CACHE_TTL_MS = 5 * 60 * 1000;

export async function loadLearnedWeights(): Promise<LearnedWeights | null> {
  if (cachedWeights && Date.now() - weightsCacheTime < CACHE_TTL_MS) {
    return cachedWeights;
  }
  try {
    const { getLatestWeights } = await import("./hubspot-sync");
    cachedWeights = await getLatestWeights();
    weightsCacheTime = Date.now();
    return cachedWeights;
  } catch {
    return null;
  }
}

export function clearWeightsCache() {
  cachedWeights = null;
  weightsCacheTime = 0;
}

export interface ScoringInput {
  name: string;
  description: string;
  type: string;
  location: string;
  website: string;
  email: string;
  phone: string;
  linkedin: string;
  ownedChannels: Record<string, string>;
  monetizationSignals: Record<string, any>;
  engagementSignals: Record<string, any>;
  tripFitSignals: Record<string, any>;
  leaderName: string;
  memberCount?: number;
  subscriberCount?: number;
  raw: Record<string, any>;
  emailValidation?: string;
}

const NICHE_KEYWORDS = [
  "young adults", "women's ministry", "men's group", "run club", "book club",
  "alumni chapter", "members", "community", "hiking", "cycling", "fitness",
  "social club", "networking", "professional", "church", "ministry",
  "nonprofit", "rotary", "lions", "crossfit", "yoga", "coworking",
];

const TRUST_KEYWORDS = [
  "welcoming", "inclusive", "community", "safe", "supportive", "family",
  "fellowship", "together", "belong", "connect",
];

const MONETIZATION_KEYWORDS = [
  "register", "tickets", "membership", "join", "dues", "support",
  "donate", "donation", "subscribe", "paid", "fee", "contribution",
];

const TRIP_KEYWORDS = [
  "retreat", "pilgrimage", "mission trip", "tour", "annual trip",
  "conference", "travel", "excursion", "group trip", "adventure",
  "outdoor", "weekend", "gathering", "annual event", "summit",
  "getaway", "outing", "expedition",
];

const DEFAULT_WEIGHTS: LearnedWeights = {
  nicheIdentity: 20,
  trustLeadership: 15,
  engagement: 20,
  monetization: 15,
  ownedChannels: 20,
  tripFit: 10,
};

function textScore(text: string, keywords: string[], maxScore: number): number {
  const lower = text.toLowerCase();
  let hits = 0;
  for (const kw of keywords) {
    if (lower.includes(kw)) hits++;
  }
  const ratio = Math.min(hits / Math.max(keywords.length * 0.3, 1), 1);
  return Math.round(ratio * maxScore);
}

function audienceSizeScore(memberCount: number, subscriberCount: number): number {
  const count = Math.max(memberCount, subscriberCount);
  if (count >= 10000) return 8;
  if (count >= 5000) return 7;
  if (count >= 1000) return 6;
  if (count >= 500) return 5;
  if (count >= 200) return 4;
  if (count >= 50) return 3;
  if (count > 0) return 1;
  return 0;
}

export function scoreLead(input: ScoringInput, learnedWeights?: LearnedWeights | null): ScoreBreakdown {
  const w = learnedWeights || cachedWeights || DEFAULT_WEIGHTS;
  const fullText = [input.name, input.description, input.type, JSON.stringify(input.raw)].join(" ");

  const nicheIdentity = textScore(fullText, NICHE_KEYWORDS, w.nicheIdentity);

  let trustLeadership = 0;
  if (input.leaderName) trustLeadership += 4;
  if (input.email) trustLeadership += 3;
  if (input.phone) trustLeadership += 2;
  if (input.linkedin) trustLeadership += 2;
  trustLeadership += textScore(fullText, TRUST_KEYWORDS, 4);
  const institutionalTypes = ["church", "nonprofit", "alumni", "professional"];
  if (institutionalTypes.includes(input.type)) trustLeadership += 2;
  trustLeadership = Math.min(trustLeadership, w.trustLeadership);

  let engagement = 0;
  const es = input.engagementSignals || {};
  if (es.event_count_90d && es.event_count_90d > 0) engagement += Math.min(es.event_count_90d * 2, 8);
  if (es.recurring) engagement += 4;
  if (es.has_calendar) engagement += 2;
  if (es.attendance_proxy) engagement += 2;

  const memberCount = input.memberCount || es.member_count || 0;
  const subscriberCount = input.subscriberCount || es.subscriber_count || 0;
  const sizeBonus = audienceSizeScore(memberCount, subscriberCount);
  engagement += sizeBonus;
  engagement = Math.min(engagement, w.engagement);

  let monetization = 0;
  const ms = input.monetizationSignals || {};
  if (Object.keys(ms).length > 0) monetization += 5;
  monetization += textScore(fullText, MONETIZATION_KEYWORDS, 10);
  monetization = Math.min(monetization, w.monetization);

  let ownedChannels = 0;
  const channels = input.ownedChannels || {};
  const channelCount = Object.keys(channels).length;
  ownedChannels += Math.min(channelCount * 4, 16);
  if (channels.newsletter || channels.email_list) ownedChannels += 2;
  if (channels.youtube) ownedChannels += 2;
  ownedChannels = Math.min(ownedChannels, w.ownedChannels);

  let tripFit = textScore(fullText, TRIP_KEYWORDS, 7);
  const tf = input.tripFitSignals || {};
  if (tf.professionals) tripFit += 1;
  if (tf.alumni) tripFit += 1;
  if (tf.paid_membership) tripFit += 1;
  tripFit = Math.min(tripFit, w.tripFit);

  let penalties = 0;
  if (!input.email && !input.website && !input.phone) penalties -= 10;
  if (engagement === 0 && Object.keys(es).length === 0 && memberCount === 0) penalties -= 5;
  if (nicheIdentity < 3) penalties -= 5;

  if (input.emailValidation === "valid") penalties += 5;
  else if (input.emailValidation === "invalid") penalties -= 8;
  else if (input.emailValidation === "catch-all") penalties -= 2;

  const total = Math.max(0, Math.min(100,
    nicheIdentity + trustLeadership + engagement + monetization + ownedChannels + tripFit + penalties
  ));

  return {
    nicheIdentity,
    trustLeadership,
    engagement,
    monetization,
    ownedChannels,
    tripFit,
    penalties,
    total,
  };
}
