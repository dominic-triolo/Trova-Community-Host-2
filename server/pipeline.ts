import { storage } from "./storage";
import { runActorAndGetResults, runActorWithWallClockTimeout } from "./apify";
import { scoreLead } from "./scoring";
import { apolloPersonMatch, isApolloAvailable, extractDomainFromUrl as apolloExtractDomain, isEnrichableDomain as apolloIsEnrichable } from "./apollo";
import { extractDomainFromUrl, isEnrichableDomain } from "./hunter";
import { verifyEmailBatch, mapResultToValidation } from "./millionverifier";
import { checkEmailsInHubspot, isHubspotConfigured } from "./hubspot";
import { log } from "./index";
import type { RunParams, InsertSourceUrl, InsertLead, InsertLeader, PipelineStep, BudgetAllocation, PipelineCheckpoint } from "@shared/schema";
import { PIPELINE_STEPS } from "@shared/schema";

export const activeRunIds = new Set<number>();
export const cancelledRunIds = new Set<number>();
export const resumingRunIds = new Set<number>();

const HEARTBEAT_INTERVAL_MS = 60_000;

function startHeartbeat(runId: number): () => void {
  const update = () => {
    storage.updateRun(runId, { lastHeartbeat: new Date() }).catch(() => {});
  };
  update();
  const interval = setInterval(update, HEARTBEAT_INTERVAL_MS);
  return () => clearInterval(interval);
}

async function getRunBudgetInfo(runId: number): Promise<{ isAutonomous: boolean; budgetUsd: number; spentUsd: number }> {
  const run = await storage.getRun(runId);
  if (!run) return { isAutonomous: false, budgetUsd: 0, spentUsd: 0 };
  return {
    isAutonomous: run.isAutonomous || false,
    budgetUsd: run.budgetUsd || 0,
    spentUsd: run.apifySpendUsd || 0,
  };
}

async function isBudgetExhausted(runId: number, estimatedNextCost: number = 0): Promise<boolean> {
  const { isAutonomous, budgetUsd, spentUsd } = await getRunBudgetInfo(runId);
  if (!isAutonomous || budgetUsd <= 0) return false;
  return (spentUsd + estimatedNextCost) > budgetUsd * 1.05;
}

async function isEmailTargetReached(runId: number): Promise<boolean> {
  const run = await storage.getRun(runId);
  if (!run || !run.emailTarget || run.emailTarget <= 0) return false;
  const currentEmails = run.leadsWithEmail || 0;
  return currentEmails >= run.emailTarget;
}

async function isValidEmailTargetReached(runId: number): Promise<boolean> {
  const run = await storage.getRun(runId);
  if (!run || !run.emailTarget || run.emailTarget <= 0) return false;
  const validEmails = run.leadsWithValidEmail || 0;
  return validEmails >= run.emailTarget;
}

async function markStepComplete(runId: number, step: PipelineStep): Promise<void> {
  await storage.updateRun(runId, { lastCompletedStep: step });
}

async function saveCheckpoint(runId: number, platformLeads: PlatformLead[], subStep: string): Promise<void> {
  try {
    const run = await storage.getRun(runId);
    const existingSteps = (run?.completedSubSteps as string[]) || [];
    const updatedSteps = existingSteps.includes(subStep) ? existingSteps : [...existingSteps, subStep];
    const checkpoint: PipelineCheckpoint = {
      platformLeads,
      completedSubSteps: updatedSteps,
      apifySpendAtCheckpoint: run?.apifySpendUsd || 0,
    };
    await storage.updateRun(runId, {
      checkpoint,
      completedSubSteps: updatedSteps,
    });
  } catch (err: any) {
    log(`[WARN] Failed to save checkpoint for run ${runId} at ${subStep}: ${err.message}`, "pipeline");
  }
}

async function loadCheckpoint(runId: number): Promise<PipelineCheckpoint | null> {
  const run = await storage.getRun(runId);
  if (!run?.checkpoint) return null;
  return run.checkpoint as PipelineCheckpoint;
}

async function clearCheckpoint(runId: number): Promise<void> {
  await storage.updateRun(runId, { checkpoint: null, completedSubSteps: [] });
}

async function saveIntraStepProgress(runId: number, stepName: string, completedBatchNums: number[]): Promise<void> {
  try {
    const run = await storage.getRun(runId);
    const checkpoint = (run?.checkpoint as PipelineCheckpoint) || null;
    if (!checkpoint) return;
    const progress = checkpoint.intraStepProgress || {};
    progress[stepName] = completedBatchNums;
    checkpoint.intraStepProgress = progress;
    await storage.updateRun(runId, { checkpoint });
  } catch (err: any) {
    log(`[WARN] Failed to save intra-step progress for ${stepName}: ${err.message}`, "pipeline");
  }
}

async function loadIntraStepProgress(runId: number, stepName: string): Promise<Set<number>> {
  try {
    const run = await storage.getRun(runId);
    const checkpoint = (run?.checkpoint as PipelineCheckpoint) || null;
    if (!checkpoint?.intraStepProgress?.[stepName]) return new Set();
    return new Set(checkpoint.intraStepProgress[stepName]);
  } catch {
    return new Set();
  }
}

class RunCancelledError extends Error {
  constructor(runId: number) {
    super(`Run ${runId} was cancelled by user`);
    this.name = "RunCancelledError";
  }
}

const BLOCKED_EMAIL_DOMAINS = ['patreon.com', 'example.com', 'sentry.io', 'cloudflare.com', 'w3.org', 'schema.org', 'googleapis.com', 'gstatic.com'];

function isBlockedEmail(email: string): boolean {
  if (!email) return true;
  const lower = email.toLowerCase().trim();
  return BLOCKED_EMAIL_DOMAINS.some((d) => lower.endsWith(`@${d}`));
}

function cleanEmail(email: string | undefined): string {
  if (!email) return "";
  const cleaned = email.replace(/^u003e/i, "").trim();
  if (isBlockedEmail(cleaned)) return "";
  return cleaned;
}

function extractDomain(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export function extractHighConfidenceFirstName(leaderName: string): string {
  if (!leaderName || leaderName.trim().length < 3) return "";
  const cleaned = leaderName.replace(/[\n\r]+/g, " ").trim();
  const words = cleaned.split(/\s+/);
  if (words.length !== 2) return "";
  if (!words.every(w => /^[A-Z][a-z]+$/.test(w))) return "";
  if (words[0].length < 3 || words[1].length < 2) return "";
  const nonPersonWords = new Set([
    "the", "and", "for", "with", "of", "in", "on", "now", "us", "we", "our", "your", "my",
    "join", "sign", "contact", "click", "subscribe", "welcome", "about", "home", "get", "buy",
    "shop", "learn", "read", "view", "see", "not", "all", "new", "out", "up", "off", "how",
    "just", "big", "old", "hot", "top", "pro", "fun", "red", "zen", "one", "two",
    "bike", "trail", "bay", "run", "hike", "surf", "camp", "yoga", "trek", "wild", "fit",
    "club", "team", "group", "community", "network", "collective", "tribe", "hub", "crew",
    "outdoor", "outdoors", "adventure", "adventures", "touring", "running", "hiking", "cycling",
    "travel", "fitness", "wellness", "coaching", "podcast", "creative", "digital", "online",
    "wales", "texas", "mesa", "paso", "area", "north", "south", "east", "west", "mountain",
    "church", "ministry", "faith", "sacred", "divine", "global", "world", "international",
    "studio", "media", "productions", "academy", "school", "foundation", "institute",
    "video", "games", "spirit", "forest", "house", "random", "beer", "tape", "duct", "comfy",
    "cozy", "witch", "penguin", "bird", "songs", "prof", "extremely", "accurate", "gooners",
    "university", "college", "city", "town", "county", "state", "lake", "river", "park",
    "daily", "weekly", "super", "ultra", "mega", "mini", "tiny", "magic", "golden", "silver",
    "little", "true", "pure", "bold", "bright", "dark", "deep", "high", "long", "great", "grand",
    "lost", "secret", "hidden", "ancient", "royal", "holy", "free", "love", "peace",
    "privacy", "policy", "terms", "login", "account", "logout", "register", "password",
    "best", "lonely", "planet", "midlife", "marathoner", "backpacker", "backpacking", "around",
    "halfway", "anywhere", "everywhere", "somewhere", "nowhere", "bigfoot",
    "reader", "writer", "blogger", "vlogger", "gamer", "maker", "builder", "seeker",
    "explorer", "nomad", "wanderer", "rambling", "roaming", "drifting",
    "solo", "solo", "happy", "crazy", "funny", "lucky", "lazy",
    "mile", "miles", "step", "steps", "page", "pages", "word", "words",
    "improved", "health", "treasure", "books", "book", "flourish", "bedouins",
    "simple", "modern", "classic", "essential", "ultimate", "basic", "complete",
    "extra", "bonus", "special", "general", "total", "full", "real",
    "rare", "earth", "sanctuary", "asia", "nasty", "backpack", "member", "directory",
    "traveling", "suwanee", "take", "family", "families", "friends", "sisters", "brothers",
    "nature", "animals", "sky", "ocean", "moon", "sun", "star", "fire", "water",
    "morning", "evening", "night", "sunrise", "sunset", "dawn", "dusk",
    "southern", "northern", "eastern", "western", "central", "upper", "lower",
    "iron", "steel", "stone", "rock", "sand", "ice", "snow", "rain", "storm",
    "bear", "wolf", "fox", "hawk", "eagle", "deer", "elk", "moose",
    "meditation", "mind", "body", "soul", "heart", "zen", "karma", "dharma",
  ]);
  if (words.some(w => nonPersonWords.has(w.toLowerCase()))) return "";
  return words[0];
}

const SOURCE_DISPLAY: Record<string, string> = {
  patreon: "Patreon", facebook: "Facebook Groups", podcast: "Podcast", substack: "Substack",
  meetup: "Meetup", mighty: "Mighty Networks", linkedin: "LinkedIn Groups", youtube: "YouTube",
  reddit: "Reddit", eventbrite: "Eventbrite", google: "Google Search",
};

export function generateResearchSummary(data: {
  communityName?: string;
  communityType?: string;
  leaderName?: string;
  description?: string;
  source?: string;
  website?: string;
  location?: string;
  ownedChannels?: Record<string, string>;
  engagementSignals?: Record<string, any>;
  monetizationSignals?: Record<string, any>;
  tripFitSignals?: Record<string, any>;
  raw?: Record<string, any>;
}): string {
  const parts: string[] = [];

  if (data.communityName) {
    const src = SOURCE_DISPLAY[data.source || ""] || data.source || "";
    const typePart = data.communityType && data.communityType !== "other" ? ` (${data.communityType})` : "";
    parts.push(`${data.communityName}${typePart}${src ? ` on ${src}` : ""}.`);
  }

  if (data.leaderName) {
    const cleanedLeader = data.leaderName.replace(/[\n\r]+/g, " ").trim();
    if (cleanedLeader && cleanedLeader.length > 2 && !/^(join|sign|contact|click|subscribe)/i.test(cleanedLeader)) {
      parts.push(`Led by ${cleanedLeader}.`);
    }
  }

  if (data.description) {
    let desc = data.description
      .replace(/<[^>]*>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&quot;/gi, '"')
      .replace(/\s+/g, " ")
      .trim();
    if (desc.length > 300) desc = desc.substring(0, 297) + "...";
    if (desc.length > 20) parts.push(desc);
  }

  const eng = data.engagementSignals || {};
  const audienceParts: string[] = [];
  if (eng.member_count) audienceParts.push(`${Number(eng.member_count).toLocaleString()} members`);
  if (eng.patron_count) audienceParts.push(`${Number(eng.patron_count).toLocaleString()} patrons`);
  if (eng.subscriber_count) audienceParts.push(`${Number(eng.subscriber_count).toLocaleString()} subscribers`);
  if (eng.episode_count) audienceParts.push(`${eng.episode_count} episodes`);
  if (eng.instagram_followers) audienceParts.push(`${Number(eng.instagram_followers).toLocaleString()} Instagram followers`);
  if (eng.twitter_followers) audienceParts.push(`${Number(eng.twitter_followers).toLocaleString()} Twitter followers`);
  if (eng.total_videos) audienceParts.push(`${eng.total_videos} YouTube videos`);
  if (audienceParts.length > 0) parts.push(`Audience: ${audienceParts.join(", ")}.`);

  const channels = data.ownedChannels || {};
  const platforms: string[] = [];
  if (channels.youtube) platforms.push("YouTube");
  if (channels.instagram) platforms.push("Instagram");
  if (channels.twitter) platforms.push("Twitter/X");
  if (channels.facebook) platforms.push("Facebook");
  if (channels.tiktok) platforms.push("TikTok");
  if (channels.linkedin || (data as any).linkedin) platforms.push("LinkedIn");
  if (channels.substack) platforms.push("Substack");
  if (channels.podcast || channels.rss) platforms.push("Podcast");
  if (channels.discord) platforms.push("Discord");
  if (channels.patreon) platforms.push("Patreon");
  if (platforms.length > 0) parts.push(`Active on: ${platforms.join(", ")}.`);

  const mon = data.monetizationSignals || {};
  const monParts: string[] = [];
  if (mon.sponsored) monParts.push("sponsorships");
  if (mon.merch) monParts.push("merchandise");
  if (mon.courses) monParts.push("courses");
  if (mon.membership || mon.patreon) monParts.push("paid membership");
  if (mon.established) monParts.push("established creator");
  const raw = data.raw || {};
  if (raw.monthlyEarnings || raw.earnings) {
    const e = raw.monthlyEarnings || raw.earnings;
    if (typeof e === "string" && e.includes("$")) monParts.push(`earning ${e}/mo`);
    else if (typeof e === "number" && e > 0) monParts.push(`earning $${e}/mo`);
  }
  if (monParts.length > 0) parts.push(`Monetization: ${monParts.join(", ")}.`);

  if (data.website) parts.push(`Website: ${data.website}`);
  if (data.location) parts.push(`Location: ${data.location}.`);

  const trip = data.tripFitSignals || {};
  const tripParts: string[] = [];
  if (trip.travel_interest) tripParts.push("travel interest");
  if (trip.group_experience) tripParts.push("group experience");
  if (trip.retreat_experience) tripParts.push("retreat experience");
  if (trip.active_events) tripParts.push("active events");
  if (tripParts.length > 0) parts.push(`Trip fit signals: ${tripParts.join(", ")}.`);

  if (raw.genre) parts.push(`Genre: ${raw.genre}.`);

  return parts.join(" ").trim();
}

function extractRealNameFromAbout(aboutText: string, fallbackName: string): string | null {
  if (!aboutText || aboutText.length < 10) return null;

  const text = aboutText.substring(0, 1500);

  const singleNamePatterns: Array<{ re: RegExp; group1: number; group2?: number }> = [
    { re: /(?:I'm|I am|my name is|my name's)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})/, group1: 1 },
    { re: /(?:Hi[!,.]?\s+I'm|Hey[!,.]?\s+I'm|Hello[!,.]?\s+I'm)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})/, group1: 1 },
    { re: /(?:me,\s*)([A-Z][a-z]+\s+[A-Z][a-z]+)/, group1: 1 },
    { re: /(?:created by|hosted by|run by)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)/i, group1: 1 },
  ];

  const pairPatterns: Array<{ re: RegExp; g1: number; g2: number }> = [
    { re: /[Ww]e are\s+([A-Z][a-z]+),?\s+([A-Z][a-z]+)/, g1: 1, g2: 2 },
    { re: /[Ww]e're\s+([A-Z][a-z]+)\s+(?:and|&)\s+([A-Z][a-z]+)/, g1: 1, g2: 2 },
    { re: /(?:our family\s*[-–—:]\s*)([A-Z][a-z]+)\s+\([^)]*\),?\s*([A-Z][a-z]+)/i, g1: 1, g2: 2 },
    { re: /(?:I am|I'm)\s+([A-Z][a-z]+)\s+(?:and|&)\s+(?:my\s+\w+\s+)?([A-Z][a-z]+)/i, g1: 1, g2: 2 },
  ];

  for (const { re, g1, g2 } of pairPatterns) {
    const match = text.match(re);
    if (match && match[g1] && match[g2]) {
      const n1 = match[g1].trim();
      const n2 = match[g2].trim();
      if (/^[A-Z][a-z]+$/.test(n1) && /^[A-Z][a-z]+$/.test(n2)) {
        return `${n1} and ${n2}`;
      }
    }
  }

  for (const { re, group1 } of singleNamePatterns) {
    const match = text.match(re);
    if (match) {
      const name = match[group1]?.trim();
      if (name && name.length <= 40) {
        const words = name.split(/\s+/);
        const looksLikeName = words.every(w => /^[A-Z][a-z]+$/.test(w));
        if (looksLikeName && words.length >= 1 && words[0].length >= 2) return name;
      }
    }
  }

  return extractNameFromBrandName(fallbackName);
}

function extractNameFromBrandName(brandName: string): string | null {
  if (!brandName || brandName.length < 3) return null;

  const nonNameWords = new Set(["the", "and", "for", "with", "of", "in", "on", "at", "to", "by", "from", "or", "a", "an", "yoga", "travel", "fitness", "wellness", "adventure", "adventures", "outdoor", "hiking", "running", "cycling", "cooking", "photography", "music", "art", "creative", "studio", "media", "production", "productions", "film", "films", "therapy", "coaching", "training", "education", "academy", "school", "club", "community", "ministries", "church", "podcast", "show", "channel", "blog", "vlog", "vlogs", "game", "games", "gaming", "network", "digital", "online", "exclusive", "content", "movement", "collective", "cooperative", "project", "foundation", "initiative", "experience", "guide", "tour", "cruise", "sailing", "mother", "goddess", "father", "five", "parks", "ride", "tiger", "golden", "silver", "love", "peace", "magic", "wild", "free", "little", "brother", "sister", "friend", "fire", "water", "moon", "star", "sun", "sky", "mountain", "river", "ocean", "desert", "forest", "garden", "world", "earth", "spirit", "soul", "heart", "mind", "body", "sacred", "holy", "divine", "royal", "ancient", "hidden", "secret", "lost", "true", "pure", "bold", "bright", "dark", "deep", "high", "long", "great", "grand", "super", "mega", "ultra", "mini", "tiny", "big", "red", "blue", "green", "black", "white", "total", "full", "real", "best", "top", "pro", "new", "old", "hot", "cool", "fun", "yin", "zen"]);

  const businessSuffixes = /\s+(?:Yoga|Travel|Fitness|Wellness|Adventures?|Outdoor|Hiking|Running|Cycling|Cooking|Photography|Music|Art|Creative|Studio|Media|Productions?|Films?|Therapy|Coaching|Training|Education|Academy|School|Club|Community|Ministries|Church|Podcast|Show|Channel|Blog|Vlogs?|Games?|Gaming|Network|Digital|Online|Exclusive|Content|Movement|Collective|Cooperative|Project|Foundation|Initiative|Experience|Guides?|Tours?|Cruise|Sailing|Workout|Nidra|Courses?|Cooperative)(?:\s.*)?$/i;

  const stripped = brandName.replace(businessSuffixes, "").trim();
  if (stripped && stripped !== brandName) {
    const words = stripped.split(/\s+/);
    if (words.length >= 2 && words.length <= 4) {
      const allCapitalized = words.every(w => /^[A-Z][a-z]+$/.test(w));
      const noneAreNonNameWords = !words.some(w => nonNameWords.has(w.toLowerCase()));
      if (allCapitalized && noneAreNonNameWords) return stripped;
    }
  }

  const words = brandName.split(/\s+/);
  if (words.length >= 2 && words.length <= 3) {
    const allCapitalized = words.every(w => /^[A-Z][a-z]+$/.test(w));
    const noneAreNonNameWords = !words.some(w => nonNameWords.has(w.toLowerCase()));
    if (allCapitalized && noneAreNonNameWords) return brandName;
  }

  return null;
}

function extractLinkAggregatorUrls(aboutText: string, channels: Record<string, string>): string[] {
  const urls: string[] = [];
  const aggregatorHosts = ["linktr.ee", "beacons.ai", "linkin.bio", "linkpop.com", "hoo.be", "campsite.bio", "lnk.bio", "tap.bio", "solo.to", "bio.link", "carrd.co"];

  const urlRegex = /https?:\/\/[^\s"'<>,)}\]]+/g;
  const allText = `${aboutText || ""} ${Object.values(channels).join(" ")}`;
  const matches = allText.match(urlRegex) || [];
  for (const url of matches) {
    try {
      const host = new URL(url).hostname.replace(/^www\./, "");
      if (aggregatorHosts.some(h => host.includes(h))) {
        urls.push(url.split("?")[0]);
      }
    } catch {}
  }

  return Array.from(new Set(urls));
}

function classifyUrl(url: string): string {
  const domain = extractDomain(url);
  if (domain.includes("meetup.com")) return "meetup";
  if (domain.includes("eventbrite.com")) return "eventbrite";
  if (domain.includes("youtube.com") || domain.includes("youtu.be")) return "youtube";
  if (domain.includes("substack.com")) return "substack";
  if (domain.includes("patreon.com")) return "patreon";
  if (domain.includes("reddit.com")) return "reddit";
  if (domain.includes("facebook.com")) return "facebook_page";
  return "website";
}

function appendLog(existing: string, line: string): string {
  const timestamp = new Date().toLocaleTimeString("en-US", { hour12: false });
  return `${existing}[${timestamp}] ${line}\n`;
}

function buildGoogleQueries(params: RunParams): string[] {
  const queries: string[] = [];
  const { seedKeywords, seedGeos } = params;
  const geos = seedGeos.length > 0 ? seedGeos : [""];

  for (const kw of seedKeywords) {
    for (const geo of geos) {
      const geoStr = geo ? ` ${geo}` : "";
      queries.push(`${kw}${geoStr}`);
      for (const extra of ["contact us", "about our team", "join our community", "membership"]) {
        queries.push(`${kw} ${extra}${geoStr}`);
      }
    }
  }

  const unique = Array.from(new Set(queries));
  return unique.slice(0, 200);
}

function cleanExtractedEmail(raw: string): string | null {
  let e = raw.trim();
  const atIdx = e.indexOf("@");
  if (atIdx < 1) return null;
  let local = e.substring(0, atIdx);
  let domain = e.substring(atIdx + 1);
  local = local.replace(/^[^a-zA-Z]+/, "");
  if (!local || !domain || !domain.includes(".")) return null;
  domain = domain.replace(/\.*$/, "");
  domain = domain.replace(/\.([A-Z][a-zA-Z]*)$/, "");
  const lastDotIdx = domain.lastIndexOf(".");
  if (lastDotIdx >= 0) {
    const afterLastDot = domain.substring(lastDotIdx + 1);
    const camelMatch = afterLastDot.match(/^([a-z]{2,10})([A-Z][a-zA-Z]*)$/);
    if (camelMatch) {
      domain = domain.substring(0, lastDotIdx + 1) + camelMatch[1];
    }
  }
  const dotParts = domain.split(".");
  const tld = dotParts[dotParts.length - 1].toLowerCase();
  if (tld.length > 10) {
    const truncated = tld.match(/^([a-z]{2,6})/);
    if (truncated) {
      dotParts[dotParts.length - 1] = truncated[1];
      domain = dotParts.join(".");
    } else {
      return null;
    }
  }
  const result = `${local}@${domain}`.toLowerCase();
  const badExtensions = [".png", ".jpg", ".gif", ".jpeg", ".svg", ".webp", ".css", ".js", ".php", ".html", ".htm", ".xml", ".json", ".pdf", ".doc", ".zip"];
  if (badExtensions.some(ext => result.endsWith(ext))) return null;
  if (!/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,10}$/.test(result)) return null;
  return result;
}

function extractEmailsFromText(text: string): string[] {
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const matches = text.match(emailRegex) || [];
  const cleaned: string[] = [];
  for (const raw of matches) {
    const clean = cleanExtractedEmail(raw);
    if (clean) cleaned.push(clean);
  }
  return Array.from(new Set(cleaned));
}

function extractObfuscatedEmails(text: string): string[] {
  const results: string[] = [];
  const patterns = [
    /([a-zA-Z0-9._%+-]+)\s*\[\s*at\s*\]\s*([a-zA-Z0-9.-]+)\s*\[\s*dot\s*\]\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*\(\s*at\s*\)\s*([a-zA-Z0-9.-]+)\s*\(\s*dot\s*\)\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*\{\s*at\s*\}\s*([a-zA-Z0-9.-]+)\s*\{\s*dot\s*\}\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s+at\s+([a-zA-Z0-9.-]+)\s+dot\s+([a-zA-Z]{2,})\b/gi,
    /([a-zA-Z0-9._%+-]+)\s*\[at\]\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*\(at\)\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*@\s+([a-zA-Z0-9.-]+)\s*\.\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s+@\s*([a-zA-Z0-9.-]+)\s*\.\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*\[\s*@\s*\]\s*([a-zA-Z0-9.-]+)\s*\.\s*([a-zA-Z]{2,})/gi,
    /([a-zA-Z0-9._%+-]+)\s*AT\s*([a-zA-Z0-9.-]+)\s*DOT\s*([a-zA-Z]{2,})/g,
    /([a-zA-Z0-9._%+-]+)\s*(?:\/at\/|_at_|-at-)\s*([a-zA-Z0-9.-]+)\s*(?:\/dot\/|_dot_|-dot-)\s*([a-zA-Z]{2,})/gi,
  ];
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      if (match[3]) {
        const email = `${match[1].trim()}@${match[2].trim()}.${match[3].trim()}`.toLowerCase();
        if (!isBlockedEmail(email)) results.push(email);
      } else if (match[2]) {
        const email = `${match[1].trim()}@${match[2].trim()}`.toLowerCase();
        if (!isBlockedEmail(email)) results.push(email);
      }
    }
  }
  return Array.from(new Set(results));
}

function isPlausibleDomain(domain: string): boolean {
  const clean = domain.toLowerCase().replace(/^www\./, "").split("/")[0];
  const parts = clean.split(".");
  if (parts.length < 2) return false;
  const name = parts[0];
  const tld = parts[parts.length - 1];
  if (name.length < 2) return false;
  if (/^\d+$/.test(name)) return false;
  if (name.length <= 3 && !/\/(contact|about|team|email)/i.test(domain)) return false;
  const ccTlds = ["de", "fr", "es", "it", "nl", "se", "no", "fi", "dk", "ch", "at", "be", "nz", "in", "uk", "ca", "au", "us"];
  if (ccTlds.includes(tld) && name.length < 5 && !name.includes("-")) return false;
  const commonWords = ["the", "and", "for", "with", "from", "that", "this", "your", "our", "their", "have", "been", "were", "are", "was", "will", "can", "has", "had", "not", "but", "all", "its", "his", "her", "she", "him", "who", "get", "got", "let", "may", "new", "now", "old", "see", "way", "day", "too", "use", "say", "ring", "being", "come", "each", "make", "like", "long", "look", "many", "some", "than", "them", "then", "very", "when", "just", "know", "take", "want", "did", "here", "much", "also", "back", "only", "even", "most", "other", "after", "year", "give", "over", "such", "where", "circle", "namaste", "welcome", "hello", "please", "thanks", "thank", "about", "great", "today", "world", "peace", "happy", "travel", "yoga", "life", "love", "best", "free", "home", "work", "more", "open", "last", "live", "full", "real", "part", "done", "ever"];
  if (commonWords.includes(name)) return false;
  return true;
}

function extractBareDomainUrls(text: string): string[] {
  const bareDomainRegex = /(?<![/@a-zA-Z0-9])(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:com|org|net|co|io|me|info|biz|us|uk|ca|au|de|fr|es|it|nl|se|no|fi|dk|ch|at|be|nz|in|co\.uk|com\.au|co\.nz)(?:\/[^\s"'<>,)}\]]*)?)/gi;
  const matches = text.match(bareDomainRegex) || [];
  const imageExts = [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"];
  return Array.from(new Set(matches.map(m => m.trim())))
    .filter(m => !imageExts.some(ext => m.toLowerCase().endsWith(ext)))
    .filter(m => !m.includes("@"))
    .filter(m => isPlausibleDomain(m))
    .map(m => m.startsWith("http") ? m : `https://${m}`);
}

function extractPhonesFromText(text: string): string[] {
  const phoneRegex = /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
  return Array.from(new Set(text.match(phoneRegex) || []));
}

function detectOwnedChannels(text: string, url: string): Record<string, string> {
  const channels: Record<string, string> = {};
  const lower = text.toLowerCase();

  if (lower.includes("newsletter") || lower.includes("subscribe to our")) channels.newsletter = "detected";
  if (lower.includes("youtube.com") || lower.includes("youtube channel")) channels.youtube = "detected";
  if (lower.includes("podcast") || lower.includes("apple podcasts") || lower.includes("spotify")) channels.podcast = "detected";
  if (lower.includes("patreon.com")) channels.patreon = "detected";
  if (lower.includes("substack.com")) channels.substack = "detected";
  if (lower.includes("discord.gg") || lower.includes("discord.com")) channels.discord = "detected";
  if (lower.includes("slack.com")) channels.slack = "detected";
  if (lower.includes("membership") || lower.includes("join us") || lower.includes("become a member")) channels.membership = "detected";

  return channels;
}

function detectMonetization(text: string): Record<string, any> {
  const signals: Record<string, any> = {};
  const lower = text.toLowerCase();

  if (lower.includes("donate") || lower.includes("donation") || lower.includes("give")) signals.donations = true;
  if (lower.includes("membership") || lower.includes("dues")) signals.membership = true;
  if (lower.includes("tickets") || lower.includes("register") || lower.includes("paid event")) signals.paid_events = true;
  if (lower.includes("retreat") || lower.includes("conference")) signals.retreats = true;
  if (lower.includes("patreon")) signals.patreon = true;

  return signals;
}

function detectEngagement(text: string): Record<string, any> {
  const signals: Record<string, any> = {};
  const lower = text.toLowerCase();

  if (lower.includes("calendar") || lower.includes("schedule") || lower.includes("upcoming events")) signals.has_calendar = true;
  if (lower.includes("weekly") || lower.includes("monthly") || lower.includes("every")) signals.recurring = true;
  if (lower.includes("member") && /\d+/.test(text)) {
    const match = text.match(/(\d+)\s*members/i);
    if (match) signals.attendance_proxy = parseInt(match[1]);
  }

  return signals;
}

function detectTripFit(text: string): Record<string, any> {
  const signals: Record<string, any> = {};
  const lower = text.toLowerCase();

  if (lower.includes("professional") || lower.includes("career")) signals.professionals = true;
  if (lower.includes("alumni")) signals.alumni = true;
  if (lower.includes("membership fee") || lower.includes("paid membership") || lower.includes("annual dues")) signals.paid_membership = true;

  return signals;
}

function detectCommunityType(text: string): string {
  const lower = text.toLowerCase();
  if (lower.includes("church") || lower.includes("ministry") || lower.includes("bible")) return "church";
  if (lower.includes("run club") || lower.includes("running")) return "run_club";
  if (lower.includes("hik")) return "hiking";
  if (lower.includes("book club") || lower.includes("reading group")) return "book_club";
  if (lower.includes("alumni")) return "alumni";
  if (lower.includes("nonprofit") || lower.includes("non-profit") || lower.includes("charity")) return "nonprofit";
  if (lower.includes("crossfit") || lower.includes("yoga") || lower.includes("fitness")) return "fitness";
  if (lower.includes("cowork")) return "coworking";
  if (lower.includes("professional") || lower.includes("association") || lower.includes("rotary") || lower.includes("lions club")) return "professional";
  if (lower.includes("social") || lower.includes("meetup") || lower.includes("club")) return "social_club";
  return "other";
}

interface PlatformLead {
  source: string;
  communityName: string;
  communityType: string;
  description: string;
  location: string;
  website: string;
  email: string;
  phone: string;
  leaderName: string;
  memberCount: number;
  subscriberCount: number;
  ownedChannels: Record<string, string>;
  monetizationSignals: Record<string, any>;
  engagementSignals: Record<string, any>;
  tripFitSignals: Record<string, any>;
  raw: Record<string, any>;
}

function mergeCrossPlatformLeads(leads: PlatformLead[]): number {
  let mergedCount = 0;
  const toRemove = new Set<number>();

  function normalizeForMatch(s: string): string {
    return s.toLowerCase().replace(/[^a-z0-9]/g, "").trim();
  }

  function extractSlug(url: string, platform: string): string {
    try {
      const u = new URL(url);
      const path = u.pathname.replace(/\/+$/, "").split("/").filter(Boolean);
      return path[path.length - 1]?.toLowerCase() || "";
    } catch { return ""; }
  }

  for (let i = 0; i < leads.length; i++) {
    if (toRemove.has(i)) continue;
    const primary = leads[i];

    for (let j = i + 1; j < leads.length; j++) {
      if (toRemove.has(j)) continue;
      const candidate = leads[j];
      if (primary.source === candidate.source) continue;

      let isMatch = false;

      if (primary.email && candidate.email && primary.email.toLowerCase() === candidate.email.toLowerCase()) {
        isMatch = true;
      }

      if (!isMatch && primary.leaderName && candidate.leaderName) {
        const n1 = normalizeForMatch(primary.leaderName);
        const n2 = normalizeForMatch(candidate.leaderName);
        if (n1.length >= 4 && n1 === n2) isMatch = true;
      }

      if (!isMatch && primary.website && candidate.website) {
        const d1 = extractDomain(primary.website)?.toLowerCase();
        const d2 = extractDomain(candidate.website)?.toLowerCase();
        if (d1 && d2 && d1 === d2) isMatch = true;
      }

      if (!isMatch) {
        const channels1 = primary.ownedChannels || {};
        const channels2 = candidate.ownedChannels || {};
        for (const platform of ["youtube", "instagram", "twitter", "linkedin", "patreon"]) {
          const url1 = channels1[platform];
          const url2 = channels2[platform];
          if (url1 && url2) {
            const slug1 = extractSlug(url1, platform);
            const slug2 = extractSlug(url2, platform);
            if (slug1 && slug2 && slug1 === slug2) { isMatch = true; break; }
          }
        }
      }

      if (isMatch) {
        if (!primary.email && candidate.email) primary.email = candidate.email;
        if (!primary.leaderName && candidate.leaderName) primary.leaderName = candidate.leaderName;
        if (!primary.website && candidate.website) primary.website = candidate.website;
        if (!primary.phone && candidate.phone) primary.phone = candidate.phone;
        if (!primary.location && candidate.location) primary.location = candidate.location;
        if (!primary.description && candidate.description) primary.description = candidate.description;

        if (candidate.memberCount > (primary.memberCount || 0)) primary.memberCount = candidate.memberCount;
        if (candidate.subscriberCount > (primary.subscriberCount || 0)) primary.subscriberCount = candidate.subscriberCount;

        const mergedChannels = { ...primary.ownedChannels };
        for (const [key, val] of Object.entries(candidate.ownedChannels || {})) {
          if (!mergedChannels[key] && val) mergedChannels[key] = val;
        }
        primary.ownedChannels = mergedChannels;

        primary.engagementSignals = { ...candidate.engagementSignals, ...primary.engagementSignals };
        primary.monetizationSignals = { ...candidate.monetizationSignals, ...primary.monetizationSignals };
        primary.tripFitSignals = { ...candidate.tripFitSignals, ...primary.tripFitSignals };

        primary.source = `${primary.source}+${candidate.source}`;

        toRemove.add(j);
        mergedCount++;
      }
    }
  }

  if (toRemove.size > 0) {
    const indices = Array.from(toRemove).sort((a, b) => b - a);
    for (const idx of indices) {
      leads.splice(idx, 1);
    }
  }

  return mergedCount;
}

function expandMeetupKeywords(keywords: string[], geos: string[], maxQueries: number = 100): string[] {
  const synonymMap: Record<string, string[]> = {
    "hiking": ["hiking group", "hiking club", "trail hiking", "hiking meetup"],
    "running": ["run club", "running group", "running club", "marathon training"],
    "yoga": ["yoga group", "yoga class", "yoga community", "yoga meetup"],
    "cycling": ["cycling group", "cycling club", "bike group", "bike club"],
    "fitness": ["fitness group", "workout group", "CrossFit community", "gym group"],
    "photography": ["photography club", "photo walk", "camera club", "photography group"],
    "book club": ["book club", "reading group", "literary club"],
    "travel": ["travel group", "adventure travel", "group travel", "travel club"],
    "climbing": ["climbing group", "climbing club", "bouldering group", "mountaineering"],
    "camping": ["camping group", "camping club", "backpacking group"],
    "outdoor": ["outdoor group", "outdoor club", "outdoor adventure", "nature group"],
    "church": ["church group", "faith community", "young adults ministry"],
    "women": ["women's group", "women's hiking", "women's adventure", "women's travel"],
    "social": ["social club", "social group", "friends meetup", "networking group"],
    "food": ["food group", "supper club", "wine tasting", "culinary group"],
    "surf": ["surf club", "surfing group", "water sports group"],
    "dance": ["dance group", "salsa meetup", "dance community"],
  };

  const TOP_US_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Denver", "Seattle", "Portland", "Nashville", "Atlanta",
    "Miami", "Boston", "Minneapolis", "Charlotte", "San Francisco",
  ];

  const expanded: string[] = [];
  const seen = new Set<string>();
  const addQuery = (q: string) => {
    if (!seen.has(q) && expanded.length < maxQueries) {
      seen.add(q);
      expanded.push(q);
    }
  };

  for (const kw of keywords) {
    addQuery(`site:meetup.com "${kw}"`);

    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms) {
      addQuery(`site:meetup.com "${syn}"`);
    }

    addQuery(`site:meetup.com ${kw} group`);
    addQuery(`site:meetup.com ${kw} club`);
    addQuery(`site:meetup.com ${kw} community`);
  }

  const allSearchTerms = [...keywords];
  for (const kw of keywords) {
    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms) allSearchTerms.push(syn);
  }

  const geoCities = geos.length > 0 ? geos.slice(0, 20) : TOP_US_CITIES;
  for (const term of allSearchTerms) {
    if (expanded.length >= maxQueries) break;
    for (const geo of geoCities) {
      addQuery(`site:meetup.com "${term}" "${geo}"`);
    }
  }

  return expanded;
}

function parseMeetupMembersFromSnippet(text: string): number {
  const patterns = [
    /(\d[\d,]*)\s*members?/i,
    /(\d[\d,]*)\s*attendees?/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return parseInt(match[1].replace(/,/g, ""), 10) || 0;
    }
  }
  return 0;
}

async function scrapeMeetupGroups(
  runId: number,
  keywords: string[],
  geos: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenGroupUrls = new Set<string>();
  const minMembers = filters.minMemberCount || 0;
  const maxMembers = filters.maxMemberCount || 0;

  const googleQueries = expandMeetupKeywords(keywords, geos);
  await appendAndSave(`Meetup: expanded ${keywords.length} keyword(s) into ${googleQueries.length} Google queries (target: ${maxItems} groups)`);
  const batchSize = 10;

  for (let i = 0; i < googleQueries.length; i += batchSize) {
    if (leads.length >= maxItems) break;
    const batch = googleQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`Meetup (via Google): batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(googleQueries.length / batchSize)} (${leads.length}/${maxItems} found so far)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 3,
        resultsPerPage: 20,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      let filteredOut = 0;
      for (const item of items) {
        if (leads.length >= maxItems) break;

        const organicResults = item.organicResults || [];
        for (const result of organicResults) {
          if (leads.length >= maxItems) break;

          const rawUrl = result.url || result.link || "";
          if (!rawUrl.includes("meetup.com/")) continue;
          if (rawUrl.includes("/events/") || rawUrl.includes("/messages/") || rawUrl.includes("/photos/")) continue;

          const urlObj = (() => { try { return new URL(rawUrl); } catch { return null; } })();
          if (!urlObj) continue;
          const pathParts = urlObj.pathname.split("/").filter(Boolean);
          if (pathParts.length === 0) continue;

          const groupSlug = pathParts[0];
          if (["find", "topics", "cities", "apps", "about", "pro", "lp", "blog", "help"].includes(groupSlug)) continue;

          const normalizedUrl = `https://www.meetup.com/${groupSlug}/`;
          if (seenGroupUrls.has(normalizedUrl)) continue;
          seenGroupUrls.add(normalizedUrl);

          const title = result.title || "";
          const snippet = result.description || result.snippet || "";
          const fullText = `${title} ${snippet}`;

          const groupName = title
            .replace(/\s*\|\s*Meetup$/i, "")
            .replace(/\s*[-–—]\s*Meetup$/i, "")
            .replace(/\s*Meetup\s*$/i, "")
            .replace(/\s*\(.*?\)\s*$/, "")
            .trim();

          if (!groupName || groupName.length < 3) continue;

          const memberCount = parseMeetupMembersFromSnippet(fullText);

          if (minMembers > 0 && memberCount > 0 && memberCount < minMembers) { filteredOut++; continue; }
          if (maxMembers > 0 && memberCount > 0 && memberCount > maxMembers) { filteredOut++; continue; }

          const channels: Record<string, string> = { meetup: normalizedUrl };

          const descUrls = extractUrlsFromText(snippet);
          const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
          Object.assign(channels, socialFromDesc);

          if (snippet.toLowerCase().includes("instagram")) channels.instagram = channels.instagram || "detected";
          if (snippet.toLowerCase().includes("facebook")) channels.facebook = channels.facebook || "detected";
          if (snippet.toLowerCase().includes("discord")) channels.discord = channels.discord || "detected";

          const descEmails = extractEmailsFromText(snippet);

          const locationMatch = snippet.match(/(?:in\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})/);
          const location = locationMatch ? locationMatch[1] : "";

          leads.push({
            source: "meetup",
            communityName: groupName,
            communityType: detectCommunityType(fullText),
            description: snippet.substring(0, 2000),
            location,
            website: normalizedUrl,
            email: descEmails[0] || "",
            phone: "",
            leaderName: "",
            memberCount,
            subscriberCount: 0,
            ownedChannels: channels,
            monetizationSignals: detectMonetization(snippet),
            engagementSignals: {
              member_count: memberCount,
              attendance_proxy: memberCount,
              ...detectEngagement(snippet),
            },
            tripFitSignals: detectTripFit(fullText),
            raw: result,
          });
        }
      }

      await appendAndSave(`Meetup: found ${leads.length} unique groups so far${filteredOut > 0 ? ` (${filteredOut} filtered by member count)` : ""}`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Meetup Google search batch failed: ${err.message}`);
    }
  }

  const meetupUrls = leads.map(l => l.ownedChannels?.meetup || l.website).filter(u => u.includes("meetup.com"));
  if (meetupUrls.length > 0) {
    await appendAndSave(`Meetup: scraping ${meetupUrls.length} group pages for organizer info...`);
    try {
      const aboutUrls = meetupUrls.map(u => u.replace(/\/$/, "") + "/");
      const cheerioUrls = aboutUrls.map(u => ({ url: u }));
      const batchSize = 30;
      for (let b = 0; b < cheerioUrls.length; b += batchSize) {
        const urlBatch = cheerioUrls.slice(b, b + batchSize);
        try {
          const { items: pageResults, costUsd: cheerioCost } = await runActorAndGetResults("apify~cheerio-scraper", {
            startUrls: urlBatch,
            pageFunction: `async function pageFunction(context) {
              const { $, request } = context;
              const text = $('body').text() || '';
              const organizerEl = $('[class*="organizer"], [data-testid*="organizer"], .organizers-list a, .groupHome-organizers a');
              const organizerName = organizerEl.first().text().trim() || '';
              const memberEl = $('[class*="member"] span, [data-testid*="member-count"]');
              const memberText = memberEl.first().text().trim() || '';
              const description = $('meta[name="description"]').attr('content') || $('meta[property="og:description"]').attr('content') || '';
              const links = [];
              $('a[href]').each(function() {
                const href = $(this).attr('href') || '';
                if (href.includes('linkedin.com') || href.includes('instagram.com') || href.includes('facebook.com') || href.includes('twitter.com') || href.includes('youtube.com') || href.includes('linktr.ee') || href.includes('beacons.ai')) {
                  links.push(href);
                }
              });
              const emails = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g) || [];
              return { url: request.url, organizerName, memberText, description, links, emails: emails.slice(0, 5) };
            }`,
            maxRequestsPerCrawl: urlBatch.length,
            maxConcurrency: 30,
            proxyConfiguration: { useApifyProxy: true },
          }, 120000);
          await storage.incrementApifySpend(runId, cheerioCost);

          for (const page of pageResults) {
            const pageUrl = (page.url || "").replace(/\/$/, "") + "/";
            const lead = leads.find(l => {
              const meetupUrl = (l.ownedChannels?.meetup || l.website || "").replace(/\/$/, "") + "/";
              return meetupUrl === pageUrl;
            });
            if (!lead) continue;

            if (page.organizerName && !lead.leaderName) {
              lead.leaderName = page.organizerName;
            }

            if (page.emails && page.emails.length > 0 && !lead.email) {
              lead.email = page.emails[0];
            }

            if (page.memberText) {
              const parsed = parseInt(page.memberText.replace(/[^0-9]/g, ""), 10);
              if (parsed > 0 && lead.memberCount === 0) {
                lead.memberCount = parsed;
                lead.engagementSignals.member_count = parsed;
                lead.engagementSignals.attendance_proxy = parsed;
              }
            }

            if (page.links) {
              for (const link of page.links) {
                const linkLower = link.toLowerCase();
                if (linkLower.includes("linkedin.com") && !lead.ownedChannels.linkedin) lead.ownedChannels.linkedin = link;
                else if (linkLower.includes("instagram.com") && !lead.ownedChannels.instagram) lead.ownedChannels.instagram = link;
                else if (linkLower.includes("facebook.com") && !lead.ownedChannels.facebook) lead.ownedChannels.facebook = link;
                else if (linkLower.includes("twitter.com") || linkLower.includes("x.com") && !lead.ownedChannels.twitter) lead.ownedChannels.twitter = link;
                else if (linkLower.includes("youtube.com") && !lead.ownedChannels.youtube) lead.ownedChannels.youtube = link;
                else if ((linkLower.includes("linktr.ee") || linkLower.includes("beacons.ai")) && !lead.ownedChannels.linktree) lead.ownedChannels.linktree = link;
              }
            }

            if (page.description && lead.description.length < 100) {
              lead.description = page.description.substring(0, 2000);
            }
          }
        } catch (err: any) {
          if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
          await appendAndSave(`[WARN] Meetup Cheerio batch failed: ${err.message}`);
        }
      }

      const withOrganizer = leads.filter(l => l.leaderName).length;
      const withEmail = leads.filter(l => l.email).length;
      await appendAndSave(`Meetup page scrape: ${withOrganizer}/${leads.length} with organizer name, ${withEmail} with email`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Meetup page scraping failed: ${err.message}`);
    }
  }

  await appendAndSave(`Meetup discovery complete: ${leads.length} groups from ${googleQueries.length} queries`);
  return leads;
}

async function scrapeYouTubeChannels(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  for (const kw of keywords) {
    if (leads.length >= maxItems) break;
    try {
      await appendAndSave(`YouTube: searching for "${kw}"...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("streamers~youtube-scraper", {
        searchQueries: [kw],
        maxResults: Math.min(50, maxItems - leads.length),
        maxResultsShorts: 0,
        maxResultStreams: 0,
        proxyConfiguration: { useApifyProxy: true },
      }, 180000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        if (!item.channelName && !item.title) continue;

        const channelName = item.channelName || item.title || "";
        const description = item.channelDescription || item.text || "";
        const subscribers = item.numberOfSubscribers || 0;
        const channelUrl = item.channelUrl || item.url || "";
        const location = item.channelLocation || "";

        const channels: Record<string, string> = { youtube: channelUrl };
        if (description.toLowerCase().includes("patreon")) channels.patreon = "detected";
        if (description.toLowerCase().includes("podcast")) channels.podcast = "detected";
        if (description.toLowerCase().includes("newsletter")) channels.newsletter = "detected";

        let externalWebsite = "";
        if (item.channelDescriptionLinks) {
          for (const link of item.channelDescriptionLinks) {
            const linkUrl = (link.url || "").toLowerCase();
            if (linkUrl.includes("discord")) channels.discord = link.url;
            else if (linkUrl.includes("patreon")) channels.patreon = link.url;
            else if (linkUrl.includes("instagram")) channels.instagram = link.url;
            else if (linkUrl.includes("twitter") || linkUrl.includes("x.com")) channels.twitter = link.url;
            else if (linkUrl.includes("linkedin.com")) channels.linkedin = link.url;
            else if (linkUrl.includes("facebook.com")) channels.facebook = link.url;
            else if (linkUrl.startsWith("http") && !linkUrl.includes("youtube.com") && !linkUrl.includes("google.com")) {
              if (!externalWebsite) externalWebsite = link.url;
              channels.website = link.url;
            }
          }
        }

        const allText = `${description} ${item.channelAbout || ""}`;
        const businessEmail = item.channelEmail || extractEmailsFromText(allText)[0] || "";

        const monetization: Record<string, any> = {};
        if (item.isMonetized) monetization.youtube_monetized = true;
        if (channels.patreon) monetization.patreon = true;

        leads.push({
          source: "youtube",
          communityName: channelName,
          communityType: detectCommunityType(`${channelName} ${description}`),
          description: description.substring(0, 2000),
          location,
          website: externalWebsite || channelUrl,
          email: businessEmail,
          phone: "",
          leaderName: channelName,
          memberCount: 0,
          subscriberCount: subscribers,
          ownedChannels: channels,
          monetizationSignals: { ...monetization, ...detectMonetization(description) },
          engagementSignals: {
            subscriber_count: subscribers,
            total_videos: item.channelTotalVideos || 0,
            ...(subscribers > 0 ? { recurring: true } : {}),
          },
          tripFitSignals: detectTripFit(description),
          raw: item,
        });
      }

      await appendAndSave(`YouTube: found ${leads.length} channels so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] YouTube search failed for "${kw}": ${err.message}`);
    }
  }

  return leads;
}

async function scrapeRedditCommunities(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  try {
    await appendAndSave(`Reddit: searching ${keywords.length} keywords...`);
    const { items, costUsd: actorCost } = await runActorAndGetResults("trudax~reddit-scraper-lite", {
      searches: keywords,
      searchCommunities: true,
      searchPosts: false,
      searchComments: false,
      searchUsers: false,
      maxItems: maxItems,
      maxCommunitiesCount: maxItems,
      scrollTimeout: 30,
      proxy: { useApifyProxy: true },
    }, 180000);
    await storage.incrementApifySpend(runId, actorCost);

    for (const item of items) {
      if (leads.length >= maxItems) break;
      if (item.dataType !== "community" && item.dataType !== "subreddit" && !item.communityName && !item.name) continue;

      const name = item.name || item.communityName || item.title || "";
      const description = item.description || item.body || "";
      const memberCount = item.numberOfMembers || item.members || 0;
      const url = item.url || `https://www.reddit.com/r/${name.replace("r/", "")}`;

      leads.push({
        source: "reddit",
        communityName: name.replace(/^r\//, ""),
        communityType: detectCommunityType(`${name} ${description}`),
        description: description.substring(0, 2000),
        location: "",
        website: url,
        email: extractEmailsFromText(description)[0] || "",
        phone: "",
        leaderName: "",
        memberCount,
        subscriberCount: 0,
        ownedChannels: { reddit: url },
        monetizationSignals: detectMonetization(description),
        engagementSignals: {
          member_count: memberCount,
          attendance_proxy: memberCount,
          ...detectEngagement(description),
        },
        tripFitSignals: detectTripFit(description),
        raw: item,
      });
    }

    await appendAndSave(`Reddit: found ${leads.length} communities`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] Reddit search failed: ${err.message}`);
  }

  return leads;
}

async function scrapeEventbriteEvents(
  runId: number,
  keywords: string[],
  geos: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  for (const kw of keywords) {
    if (leads.length >= maxItems) break;
    try {
      const city = geos.length > 0 ? geos[0] : "";
      await appendAndSave(`Eventbrite: searching for "${kw}" ${city ? `in ${city}` : ""}...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("aitorsm~eventbrite", {
        country: "united-states",
        city: city || "all",
        category: "custom",
        keyword: kw,
        maxItems: Math.min(50, maxItems - leads.length),
      }, 300000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        const eventName = item.name || item.title || "";
        const organizer = item.primary_organizer || {};
        const organizerName = organizer.name || "";
        const organizerUrl = organizer.url || "";
        const venue = item.primary_venue || {};
        const address = venue.address || {};
        const location = [address.city, address.region, address.country].filter(Boolean).join(", ");
        const description = item.summary || item.description || "";
        const tags = (item.tags || []).map((t: any) => t.display_name || "").filter(Boolean);
        const fullText = `${eventName} ${organizerName} ${description} ${tags.join(" ")}`;

        const channels: Record<string, string> = { eventbrite: organizerUrl || "active" };
        if (organizer.facebook) channels.facebook = organizer.facebook;
        if (organizer.twitter) channels.twitter = organizer.twitter;
        if (organizer.instagram) channels.instagram = organizer.instagram;
        if (organizer.website_url) channels.website = organizer.website_url;

        const organizerContact = organizer.contact || {};
        const eventbriteEmail = organizerContact.email || extractEmailsFromText(`${description} ${organizer.description || ""}`)[0] || "";
        const eventbritePhone = organizerContact.phone || extractPhonesFromText(description)[0] || "";
        const organizerWebsite = organizer.website_url || organizerContact.website || "";

        const monetization: Record<string, any> = {};
        const ticketInfo = item.ticket_availability || {};
        if (!ticketInfo.is_free) monetization.paid_events = true;
        if (organizer.num_upcoming_events > 1) monetization.recurring_events = true;

        const followerCount = organizer.num_followers || 0;

        leads.push({
          source: "eventbrite",
          communityName: organizerName || eventName,
          communityType: detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location,
          website: organizerWebsite || organizerUrl || item.url || "",
          email: eventbriteEmail,
          phone: eventbritePhone,
          leaderName: organizerName,
          memberCount: followerCount,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: { ...monetization, ...detectMonetization(fullText) },
          engagementSignals: {
            has_calendar: true,
            recurring: true,
            member_count: followerCount,
            attendance_proxy: followerCount,
          },
          tripFitSignals: detectTripFit(fullText),
          raw: item,
        });
      }

      await appendAndSave(`Eventbrite: found ${leads.length} organizers so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Eventbrite search failed for "${kw}": ${err.message}`);
    }
  }

  return leads;
}

function parsePatreonCount(val: any): number {
  if (typeof val === "number") return val;
  if (typeof val === "string") {
    const cleaned = val.replace(/[^0-9]/g, "");
    return cleaned ? parseInt(cleaned, 10) : 0;
  }
  return 0;
}

function isPatreonCdnUrl(url: string): boolean {
  if (!url) return false;
  const lower = url.toLowerCase();
  return lower.includes("patreonusercontent.com") || lower.includes("patreon-media") || lower.includes("token-hash=");
}

function computeApolloInputHash(lead: { leaderName?: string | null; communityName?: string | null; website?: string | null; ownedChannels?: Record<string, string> | null }): string {
  const name = (lead.leaderName || lead.communityName || "").toLowerCase().trim();
  const channels = (lead.ownedChannels as Record<string, string>) || {};
  let domain = "";
  try {
    const ws = channels.website || lead.website || "";
    if (ws) domain = new URL(ws).hostname.replace(/^www\./, "");
  } catch {}
  const linkedin = (channels.linkedin || "").split("?")[0].toLowerCase();
  return `${name}|${domain}|${linkedin}`;
}

function isValidApolloCandidate(leaderName: string): boolean {
  if (!leaderName) return false;
  const trimmed = leaderName.trim();
  const nameParts = trimmed.split(/\s+/);
  if (nameParts.length < 2) return false;
  if (nameParts.length > 5) return false;
  if (nameParts[0].length <= 1 || nameParts[1].length <= 1) return false;
  if (trimmed.length > 40) return false;

  const lower = trimmed.toLowerCase();

  const junkSubstrings = [
    "contact", "donate", "volunteer", "subscribe", "login", "sign up",
    "resources", "directions", "opportunities", "prayer",
    "request", "faq", "learn more", "read more",
    "submit", "register", "download", "upload",
    "enquiry", "wholesale", "packages", "book now",
  ];
  if (junkSubstrings.some((w) => lower.includes(w))) return false;

  const junkWordBoundary = [
    "menu", "home", "about", "search", "click", "view", "join",
    "share", "follow", "apply", "reserve", "offers", "call",
    "visit", "cannot", "bible", "needs", "support", "careers",
    "give", "back", "close", "select", "send",
  ];
  if (junkWordBoundary.some((w) => new RegExp(`\\b${w}\\b`).test(lower))) return false;

  const startsWith = [
    "the ", "adventures of", "walking is", "podcast", "us ",
    "at ", "to ", "local ", "deals ",
  ];
  if (startsWith.some((s) => lower.startsWith(s))) return false;

  const endsWith = [
    " podcast", " radio", " show", " tv", " team", " group",
    " club", " church", " ministry", " community",
  ];
  if (endsWith.some((e) => lower.endsWith(e))) return false;

  const allCaps = nameParts.length >= 2 && nameParts.every((p) => p === p.toUpperCase() && p.length > 2);
  if (allCaps) return false;

  const hasUrl = /https?:\/\/|www\.|\.com|\.org|\.net/.test(lower);
  if (hasUrl) return false;

  const nonAlpha = trimmed.replace(/[a-zA-Z\s'-]/g, "");
  if (nonAlpha.length > 2) return false;

  const hasMixedCase = nameParts.some(p => /[a-z]/.test(p) && /[A-Z]/.test(p.slice(1)));
  const allPartsCapitalized = nameParts.every(p => /^[A-Z][a-z]/.test(p) || /^[A-Z]$/.test(p) || /^(de|van|von|del|di|el|la|le|des|den|der)$/i.test(p));
  if (!allPartsCapitalized && hasMixedCase && nameParts.length <= 2) return false;

  if (/[³²¹°™®©]/.test(trimmed)) return false;

  if (nameParts.some(p => p.length > 15)) return false;

  return true;
}

async function scrapePatreonCreators(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters?: { minMemberCount?: number; maxMemberCount?: number; minPostCount?: number },
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenUrls = new Set<string>();

  const existingLeads = await storage.listLeads();
  for (const l of existingLeads) {
    if (l.website && l.website.includes("patreon.com")) {
      seenUrls.add(l.website.split("?")[0].toLowerCase());
    }
    const ch = (l.ownedChannels as Record<string, string>) || {};
    if (ch.patreon && ch.patreon.startsWith("http") && ch.patreon.includes("patreon.com")) {
      seenUrls.add(ch.patreon.split("?")[0].toLowerCase());
    }
  }
  await appendAndSave(`Patreon: ${seenUrls.size} known creator URLs from previous runs`);

  const dedupedKeywords = Array.from(new Set(keywords.map(k => k.trim()).filter(Boolean)));
  if (dedupedKeywords.length === 0) {
    await appendAndSave(`Patreon: no keywords to search`);
    return leads;
  }

  let overlapMultiplier = 1;
  try {
    const allRuns = await storage.listRuns();
    const pastKeywords = new Set<string>();
    for (const run of allRuns) {
      const p = run.params as any;
      if (p?.keywords && Array.isArray(p.keywords)) {
        for (const kw of p.keywords) pastKeywords.add(kw.toLowerCase().trim());
      }
    }
    const overlapCount = dedupedKeywords.filter(k => pastKeywords.has(k.toLowerCase())).length;
    const overlapRatio = overlapCount / dedupedKeywords.length;
    if (overlapRatio >= 0.5) {
      overlapMultiplier = overlapRatio >= 0.8 ? 2.5 : 1.8;
      await appendAndSave(`Patreon: ${Math.round(overlapRatio * 100)}% keyword overlap with past runs, increasing crawl depth ${overlapMultiplier}x`);
    }
  } catch {}

  const baseCrawl = Math.min(800, Math.max(100, maxItems * 6));
  const adjustedCrawl = Math.min(1500, Math.round(baseCrawl * overlapMultiplier));

  const keywordPreview = dedupedKeywords.length <= 5 ? dedupedKeywords.join(", ") : `${dedupedKeywords.slice(0, 5).join(", ")} (+${dedupedKeywords.length - 5} more)`;
  await appendAndSave(`Patreon: searching ${dedupedKeywords.length} keywords (crawl depth: ${adjustedCrawl}): ${keywordPreview}`);

  try {
    const { items, costUsd: actorCost } = await runActorAndGetResults("louisdeconinck~patreon-scraper", {
      searchQueries: dedupedKeywords,
      maxRequestsPerCrawl: adjustedCrawl,
    }, 300000);
    await storage.incrementApifySpend(runId, actorCost);

    await appendAndSave(`Patreon: scraper returned ${items.length} raw results`);

    if (items.length > 0) {
      const sampleKeys = Object.keys(items[0]).sort().join(", ");
      await appendAndSave(`Patreon raw fields: ${sampleKeys}`);
      const socialSample = {
        youtube: items[0].youtube,
        instagram: items[0].instagram,
        twitter: items[0].twitter,
        facebook: items[0].facebook,
        tiktok: items[0].tiktok,
        twitch: items[0].twitch,
      };
      await appendAndSave(`Patreon social data sample: ${JSON.stringify(socialSample)}`);
    }

    let skippedDupe = 0;
    let skippedFilter = 0;

    for (const item of items) {
      if (leads.length >= maxItems) break;

      const creatorName = item.creator_name || item.name || item.creatorName || "";
      if (!creatorName) continue;

      const creatorUrl = (item.url || item.profile_url || "").split("?")[0];
      if (creatorUrl && seenUrls.has(creatorUrl.toLowerCase())) {
        skippedDupe++;
        continue;
      }

      const description = item.about || item.description || item.creation_name || "";
      const memberCount = parsePatreonCount(item.patron_count || item.total_members || item.paid_members || 0);
      const postCount = parsePatreonCount(item.post_count || item.total_posts || 0);

      if (filters) {
        if (filters.minMemberCount && filters.minMemberCount > 0 && memberCount < filters.minMemberCount) {
          skippedFilter++;
          continue;
        }
        if (filters.maxMemberCount && filters.maxMemberCount > 0 && memberCount > filters.maxMemberCount) {
          skippedFilter++;
          continue;
        }
        if (filters.minPostCount && filters.minPostCount > 0 && postCount < filters.minPostCount) {
          skippedFilter++;
          continue;
        }
      }

      if (creatorUrl) seenUrls.add(creatorUrl.toLowerCase());

      const fullText = `${creatorName} ${description}`;

      const patreonLink = creatorUrl || "active";
      const channels: Record<string, string> = { patreon: patreonLink };

        if (item.youtube && typeof item.youtube === "string" && item.youtube.startsWith("http")) channels.youtube = item.youtube;
        if (item.instagram && typeof item.instagram === "string" && item.instagram.startsWith("http")) channels.instagram = item.instagram;
        if (item.twitter && typeof item.twitter === "string" && item.twitter.startsWith("http")) channels.twitter = item.twitter;
        if (item.facebook && typeof item.facebook === "string" && item.facebook.startsWith("http")) channels.facebook = item.facebook;
        if (item.tiktok && typeof item.tiktok === "string" && item.tiktok.startsWith("http")) channels.tiktok = item.tiktok;
        if (item.twitch && typeof item.twitch === "string" && item.twitch.startsWith("http")) channels.twitch = item.twitch;

        const allLinks: string[] = [];
        const urlRegex = /https?:\/\/[^\s"'<>,)}\]]+/g;
        const aboutText = item.about || "";
        const combinedText = `${description} ${aboutText}`;
        const descUrls = combinedText.match(urlRegex) || [];
        allLinks.push(...descUrls);

        const bareDomains = extractBareDomainUrls(combinedText);
        for (const bd of bareDomains) {
          if (!allLinks.some(l => l.toLowerCase().includes(new URL(bd).hostname.replace(/^www\./, "")))) {
            allLinks.push(bd);
          }
        }

        for (const link of allLinks) {
          try {
            const host = new URL(link).hostname.replace(/^www\./, "");
            if (isPatreonCdnUrl(link)) continue;
            if ((host.includes("youtube.com") || host.includes("youtu.be")) && !channels.youtube) channels.youtube = link;
            else if (host.includes("instagram.com") && !channels.instagram) channels.instagram = link;
            else if ((host.includes("twitter.com") || host === "x.com" || host.endsWith(".x.com")) && !channels.twitter) channels.twitter = link;
            else if ((host.includes("discord.gg") || host.includes("discord.com")) && !channels.discord) channels.discord = link;
            else if (host.includes("facebook.com") && !channels.facebook) channels.facebook = link;
            else if (host.includes("tiktok.com") && !channels.tiktok) channels.tiktok = link;
            else if (host.includes("twitch.tv") && !channels.twitch) channels.twitch = link;
            else if (host.includes("linkedin.com") && !channels.linkedin) channels.linkedin = link;
            else if (host.includes("substack.com") && !channels.substack) channels.substack = link;
            else if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("linkin.bio") || host.includes("bio.link") || host.includes("solo.to") || host.includes("carrd.co") || host.includes("campsite.bio")) && !channels.linktree) channels.linktree = link;
          } catch {}
        }

        const allTextForEmail = `${description} ${aboutText}`;
        const creatorEmail = extractEmailsFromText(allTextForEmail)[0] || extractObfuscatedEmails(allTextForEmail)[0] || "";

        const realName = extractRealNameFromAbout(aboutText, creatorName);

        const aggregatorUrls = extractLinkAggregatorUrls(aboutText, channels);
        if (aggregatorUrls.length > 0 && !channels.linktree) {
          channels.linktree = aggregatorUrls[0];
        }

        if (!channels.website) {
          for (const link of allLinks) {
            try {
              if (isPatreonCdnUrl(link)) continue;
              const host = new URL(link).hostname.replace(/^www\./, "");
              const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com"];
              if (!socialHosts.some((s) => host.includes(s))) {
                channels.website = link;
                break;
              }
            } catch {}
          }
        }

        const monetization: Record<string, any> = { patreon: true };
        if (item.tiers && Array.isArray(item.tiers) && item.tiers.length > 0) monetization.paid_membership = true;
        if (item.earnings_per_month) monetization.monthly_earnings = item.earnings_per_month;

        leads.push({
          source: "patreon",
          communityName: creatorName,
          communityType: detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location: "",
          website: creatorUrl || "",
          email: creatorEmail,
          phone: "",
          leaderName: realName || creatorName,
          memberCount,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: { ...monetization, ...detectMonetization(description) },
          engagementSignals: {
            member_count: memberCount,
            post_count: postCount,
            paid_members: parsePatreonCount(item.paid_members || 0),
            attendance_proxy: memberCount,
            recurring: true,
          },
          tripFitSignals: detectTripFit(fullText),
          raw: item,
        });
      }

    const withSocials = leads.filter((l) => {
      const ch = l.ownedChannels || {};
      return Object.entries(ch).some(([k, v]) => k !== "patreon" && k !== "website" && v && v !== "detected" && v.startsWith("http"));
    }).length;
    await appendAndSave(`Patreon: ${leads.length} creators (${withSocials} with real social URLs, skipped ${skippedDupe} dupes, ${skippedFilter} filtered)`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] Patreon search failed: ${err.message}`);
  }

  return leads;
}

function extractUrlsFromText(text: string): string[] {
  const urlRegex = /https?:\/\/[^\s<>"')\]},]+/gi;
  return (text.match(urlRegex) || []).map(u => u.replace(/[.,;:!?)]+$/, ""));
}

function extractSocialChannelsFromUrls(urls: string[]): Record<string, string> {
  const channels: Record<string, string> = {};
  for (const url of urls) {
    try {
      const host = new URL(url).hostname.replace(/^www\./, "");
      if (host.includes("instagram.com") && !channels.instagram) channels.instagram = url.split("?")[0];
      else if ((host.includes("twitter.com") || host === "x.com") && !channels.twitter) channels.twitter = url.split("?")[0];
      else if (host.includes("youtube.com") && !channels.youtube) channels.youtube = url.split("?")[0];
      else if (host.includes("linkedin.com") && !channels.linkedin) channels.linkedin = url.split("?")[0];
      else if (host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link")) {
        if (!channels.linktree) channels.linktree = url.split("?")[0];
      }
      else if (host.includes("discord.gg") || host.includes("discord.com")) channels.discord = url.split("?")[0];
      else if (!["facebook.com", "fb.com", "fb.me", "google.com", "apple.com", "play.google.com"].some(s => host.includes(s))) {
        if (!channels.website) channels.website = url.split("?")[0];
      }
    } catch {}
  }
  return channels;
}

function parseMemberCountFromSnippet(text: string): number {
  const memberPatterns = [
    /([\d,.]+)\s*[Mm](?:illion)?\s+(?:members|people|followers)/i,
    /([\d,.]+)\s*[Kk]\s*(?:members|people|followers)/i,
    /([\d,.]+)\+?\s*(?:members|people|followers)/i,
    /(?:members|people|followers)[:\s]*([\d,.]+)\s*([KkMm])?/i,
  ];
  for (const pat of memberPatterns) {
    const m = text.match(pat);
    if (m) {
      const raw = m[1].replace(/,/g, "");
      let num = parseFloat(raw);
      if (isNaN(num) || num <= 0) continue;
      const suffix = m[2] || "";
      if (/[Kk]/.test(suffix)) num *= 1000;
      else if (/[Mm]/.test(suffix)) num *= 1000000;
      else if (pat === memberPatterns[0]) num *= 1000000;
      else if (pat === memberPatterns[1]) num *= 1000;
      return Math.round(num);
    }
  }
  return 0;
}

function isFbGroupPostUrl(rawUrl: string): boolean {
  try {
    const u = new URL(rawUrl);
    return /\/groups\/[^/]+\/posts\//i.test(u.pathname);
  } catch {
    return false;
  }
}

function extractFbGroupSlug(rawUrl: string): string {
  try {
    const u = new URL(rawUrl);
    const path = u.pathname.replace(/\/+$/, "");
    const parts = path.split("/").filter(Boolean);
    if (parts[0] === "groups" && parts[1]) return parts[1];
  } catch {}
  return "";
}

function slugToGroupName(slug: string): string {
  return slug
    .replace(/[-_]+/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

function extractGroupNameFromSnippet(snippet: string): string {
  const arrowMatch = snippet.match(/▻\s*(.+?)(?:\.\s|\s*$)/);
  if (arrowMatch && arrowMatch[1] && arrowMatch[1].length > 3 && arrowMatch[1].length < 100) {
    let name = arrowMatch[1].trim();
    name = name.replace(/\s*\((?:www|http).*$/i, "").trim();
    if (name.length > 3) return name;
  }
  const nameEnding = /(?:Group|Club|Community|Society|Network|Team|Crew|Alliance|Association|Coalition)\b/i;
  const headMatch = snippet.match(/^([A-Z][A-Za-z0-9 &'''\-()]+?(?:Group|Club|Community|Society|Network|Team|Crew|Alliance|Association|Coalition))\b/);
  if (headMatch && headMatch[1] && headMatch[1].length > 3 && headMatch[1].length < 100) {
    const name = headMatch[1].trim();
    const wordCount = name.split(/\s+/).length;
    if (wordCount <= 8 && !/^(She|He|They|We|I|It|The|This|That|How|What|Where|When|Why|Who|Is|Are|Was|Were|Do|Does|Did|Can|Could|Would|Should|If|So|But|And|Or|Just|Also|Some|Any|All|My|Your|Our|Her|His|Its|No)\s/i.test(name)) {
      return name;
    }
  }
  return "";
}

function extractFbGroupUrl(rawUrl: string): string {
  try {
    const u = new URL(rawUrl);
    const host = u.hostname.replace(/^(www\.|m\.|mobile\.)/, "");
    if (host !== "facebook.com") return rawUrl.split("?")[0];

    const groupId = u.searchParams.get("group_id");
    if (groupId) return `https://www.facebook.com/groups/${groupId}`;

    const path = u.pathname.replace(/\/+$/, "");
    if (path.startsWith("/groups/")) {
      const parts = path.split("/").filter(Boolean);
      if (parts.length >= 2 && parts[1]) {
        return `https://www.facebook.com/groups/${parts[1]}`;
      }
    }
    return rawUrl.split("?")[0];
  } catch {
    return rawUrl;
  }
}

function expandFacebookKeywords(keywords: string[], geos: string[], maxQueries: number = 100): string[] {
  const synonymMap: Record<string, string[]> = {
    "book club": ["book group", "reading club", "reading group", "book lovers", "readers group", "literature club", "book discussion"],
    "hiking": ["hiking group", "hiking club", "hikers group", "trail club", "outdoor hiking", "day hikes"],
    "hiking club": ["hiking group", "hikers club", "trail club", "outdoor hiking"],
    "run club": ["running club", "runners group", "running group", "joggers club", "run group"],
    "running club": ["run club", "runners group", "running group", "joggers club"],
    "fitness": ["fitness group", "fitness club", "workout group", "gym group", "exercise group"],
    "yoga": ["yoga group", "yoga club", "yoga community", "yoga lovers"],
    "travel": ["travel group", "travel club", "travelers group", "travel community", "travel buddies"],
    "travel club": ["travel group", "travelers club", "travel community", "travel buddies"],
    "social club": ["social group", "social gathering", "social events", "meetup group"],
    "women": ["women's group", "women's club", "ladies group", "women's community"],
    "mom": ["moms group", "mothers group", "mommy group", "parents group"],
    "church": ["church group", "faith group", "ministry group", "bible study group", "worship community"],
    "alumni": ["alumni group", "alumni club", "alumni network", "alumni association"],
    "professional": ["professional group", "professional network", "networking group", "career group"],
    "photography": ["photography group", "photographers club", "photo club", "camera club"],
    "cooking": ["cooking group", "cooking club", "foodies group", "culinary club", "recipe group"],
    "wine": ["wine club", "wine group", "wine lovers", "wine tasting group"],
    "outdoor": ["outdoor group", "outdoor club", "outdoor adventure", "nature group"],
    "cycling": ["cycling group", "cycling club", "biking group", "bike club", "cyclists group"],
  };

  const TOP_US_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Denver", "Seattle", "Portland", "Nashville", "Atlanta",
    "Miami", "Boston", "Minneapolis", "Charlotte", "San Francisco",
  ];

  const expanded: string[] = [];
  const seen = new Set<string>();
  const addQuery = (q: string) => {
    if (!seen.has(q) && expanded.length < maxQueries) {
      seen.add(q);
      expanded.push(q);
    }
  };

  for (const kw of keywords) {
    addQuery(`site:facebook.com/groups "${kw}"`);

    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms) {
      addQuery(`site:facebook.com/groups "${syn}"`);
    }

    addQuery(`site:facebook.com/groups ${kw} community`);
    addQuery(`site:facebook.com/groups ${kw} group`);
    addQuery(`site:facebook.com/groups ${kw} meetup`);
    addQuery(`site:facebook.com/groups ${kw} network`);
  }

  const allSearchTerms = [...keywords];
  for (const kw of keywords) {
    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms) allSearchTerms.push(syn);
  }

  const geoCities = geos.length > 0 ? geos.slice(0, 20) : TOP_US_CITIES;
  for (const term of allSearchTerms) {
    if (expanded.length >= maxQueries) break;
    for (const geo of geoCities) {
      addQuery(`site:facebook.com/groups "${term}" "${geo}"`);
    }
  }

  return expanded;
}

async function scrapeFacebookGroups(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number; geos?: string[] } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenGroupUrls = new Set<string>();
  const minMembers = filters.minMemberCount || 0;
  const maxMembers = filters.maxMemberCount || 0;

  const googleQueries = expandFacebookKeywords(keywords, filters.geos || []);
  await appendAndSave(`Facebook: expanded ${keywords.length} keyword(s) into ${googleQueries.length} Google queries (target: ${maxItems} groups)`);
  const batchSize = 10;

  for (let i = 0; i < googleQueries.length; i += batchSize) {
    if (leads.length >= maxItems) break;
    const batch = googleQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`Facebook (via Google): batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(googleQueries.length / batchSize)} (${leads.length}/${maxItems} found so far)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 5,
        resultsPerPage: 20,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      let filteredOut = 0;
      for (const item of items) {
        if (leads.length >= maxItems) break;

        const organicResults = item.organicResults || [];
        for (const result of organicResults) {
          if (leads.length >= maxItems) break;

          const rawUrl = result.url || result.link || "";
          if (!rawUrl.includes("facebook.com/groups/")) continue;

          const url = extractFbGroupUrl(rawUrl);

          if (seenGroupUrls.has(url)) continue;
          seenGroupUrls.add(url);

          const title = result.title || "";
          const snippet = result.description || result.snippet || "";
          const fullText = `${title} ${snippet}`;
          const isPostUrl = isFbGroupPostUrl(rawUrl);

          let groupName: string;
          if (isPostUrl) {
            const fromSnippet = extractGroupNameFromSnippet(snippet);
            const slug = extractFbGroupSlug(rawUrl);
            const isNumericSlug = /^\d+$/.test(slug);
            groupName = fromSnippet || (isNumericSlug ? "" : slugToGroupName(slug)) || `FB Group ${slug}`;
          } else {
            groupName = title
              .replace(/\s*\|\s*Facebook$/i, "")
              .replace(/\s*[-–—]\s*Facebook$/i, "")
              .replace(/\s*Facebook\s*$/i, "")
              .trim();
          }

          const memberCount = parseMemberCountFromSnippet(fullText);

          if (minMembers > 0 && memberCount > 0 && memberCount < minMembers) { filteredOut++; continue; }
          if (maxMembers > 0 && memberCount > 0 && memberCount > maxMembers) { filteredOut++; continue; }

          const channels: Record<string, string> = { facebook: url };

          const descUrls = extractUrlsFromText(snippet);
          const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
          Object.assign(channels, socialFromDesc);

          if (snippet.toLowerCase().includes("discord") && !channels.discord) channels.discord = "detected";
          if (snippet.toLowerCase().includes("newsletter") && !channels.newsletter) channels.newsletter = "detected";

          const descEmails = extractEmailsFromText(snippet);

          leads.push({
            source: "facebook",
            communityName: groupName,
            communityType: detectCommunityType(fullText),
            description: snippet.substring(0, 2000),
            location: "",
            website: channels.website || url,
            email: descEmails[0] || "",
            phone: "",
            leaderName: "",
            memberCount,
            subscriberCount: 0,
            ownedChannels: channels,
            monetizationSignals: detectMonetization(snippet),
            engagementSignals: {
              member_count: memberCount,
              attendance_proxy: memberCount,
              ...detectEngagement(snippet),
            },
            tripFitSignals: detectTripFit(fullText),
            raw: result,
          });
        }
      }

      await appendAndSave(`Facebook: found ${leads.length} unique groups so far${filteredOut > 0 ? ` (${filteredOut} filtered by member count)` : ""}`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Facebook Google search batch failed: ${err.message}`);
    }
  }

  await appendAndSave(`Facebook discovery complete: ${leads.length} groups from ${googleQueries.length} queries`);
  return leads;
}

async function scrapeApplePodcasts(
  runId: number,
  keywords: string[],
  emailTarget: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minEpisodeCount?: number; podcastCountry?: string } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenPodcastIds = new Set<string>();
  const minEpisodes = filters.minEpisodeCount || 0;
  const country = filters.podcastCountry || "US";
  let totalEmailsFound = 0;

  const existingLeads = await storage.listLeads();
  for (const l of existingLeads) {
    const ch = (l.ownedChannels as Record<string, string>) || {};
    if (ch.podcast && ch.podcast.includes("podcasts.apple.com")) {
      const idMatch = ch.podcast.match(/id(\d+)/);
      if (idMatch) seenPodcastIds.add(idMatch[1]);
    }
  }
  await appendAndSave(`Podcasts: ${seenPodcastIds.size} known podcast IDs from previous runs`);

  const dedupedKeywords = Array.from(new Set(keywords.map(k => k.trim()).filter(Boolean)));
  if (dedupedKeywords.length === 0) {
    await appendAndSave(`Podcasts: no keywords to search`);
    return leads;
  }

  const keywordPreview = dedupedKeywords.length <= 5 ? dedupedKeywords.join(", ") : `${dedupedKeywords.slice(0, 5).join(", ")} (+${dedupedKeywords.length - 5} more)`;
  await appendAndSave(`Podcasts: email target ${emailTarget} — searching ${dedupedKeywords.length} keywords in ${country} store: ${keywordPreview}`);

  const maxResultsPerQuery = Math.max(10, Math.min(500, Math.ceil(emailTarget * 3 / dedupedKeywords.length)));

  for (const kw of dedupedKeywords) {
    if (totalEmailsFound >= emailTarget) {
      await appendAndSave(`Podcasts: email target reached (${totalEmailsFound}/${emailTarget}), stopping search`);
      break;
    }

    try {
      await appendAndSave(`Podcasts: searching "${kw}" (max ${maxResultsPerQuery} results)... [${totalEmailsFound}/${emailTarget} emails so far]`);

      const { items, costUsd: actorCost } = await runActorAndGetResults("benthepythondev~podcast-intelligence-aggregator", {
        mode: "search",
        searchQuery: kw,
        country: country,
        maxResults: maxResultsPerQuery,
        includeEpisodes: false,
      }, 180000, 0.03);
      await storage.incrementApifySpend(runId, actorCost);

      let skippedDupe = 0;
      let skippedFilter = 0;
      const batchLeads: PlatformLead[] = [];

      for (const item of items) {
        const podcastId = String(item.itunes_id || item.id || "");
        const podcastTitle = item.title || item.name || "";
        if (!podcastTitle) continue;

        if (podcastId && seenPodcastIds.has(podcastId)) {
          skippedDupe++;
          continue;
        }

        const episodeCount = item.track_count || item.rss_data?.episode_count || 0;
        if (minEpisodes > 0 && episodeCount < minEpisodes) {
          skippedFilter++;
          continue;
        }

        if (podcastId) seenPodcastIds.add(podcastId);

        const artistName = item.artist || item.artistName || item.rss_data?.author || "";
        const description = item.description?.standard || item.description || item.rss_data?.description || "";
        const feedUrl = item.feed_url || item.feedUrl || "";
        const podcastUrl = item.itunes_url || item.url || "";
        const websiteUrl = item.websiteUrl || item.rss_data?.link || "";
        const genres = item.genres || item.genreNames || [];
        const primaryGenre = item.primary_genre || (Array.isArray(genres) && genres.length > 0 ? (typeof genres[0] === "string" ? genres[0] : genres[0]?.name || "") : "");

        const channels: Record<string, string> = {};
        if (podcastUrl) channels.podcast = podcastUrl;
        if (feedUrl) channels.rss = feedUrl;
        if (websiteUrl && !websiteUrl.includes("podcasts.apple.com")) channels.website = websiteUrl;

        const allTextUrls = extractUrlsFromText(description);
        const socialFromDesc = extractSocialChannelsFromUrls(allTextUrls);
        for (const [k, v] of Object.entries(socialFromDesc)) {
          if (!channels[k]) channels[k] = v;
        }

        if (description.toLowerCase().includes("linktree") || description.toLowerCase().includes("linktr.ee")) {
          const ltMatch = description.match(/https?:\/\/linktr\.ee\/[^\s"'<>,)}\]]+/i);
          if (ltMatch && !channels.linktree) channels.linktree = ltMatch[0].replace(/[.,;:!?)]+$/, "");
        }

        const fullText = `${podcastTitle} ${artistName} ${description}`;
        const descEmails = extractEmailsFromText(description);

        const monetization: Record<string, any> = { podcast: true };
        if (episodeCount > 100) monetization.established = true;
        if (description.toLowerCase().includes("sponsor") || description.toLowerCase().includes("patreon")) monetization.sponsored = true;

        const lead: PlatformLead = {
          source: "podcast",
          communityName: podcastTitle,
          communityType: detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location: "",
          website: websiteUrl || podcastUrl || "",
          email: descEmails[0] || "",
          phone: "",
          leaderName: artistName || podcastTitle,
          memberCount: 0,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: { ...monetization, ...detectMonetization(description) },
          engagementSignals: {
            episode_count: episodeCount,
            genre: primaryGenre,
            attendance_proxy: episodeCount,
            recurring: true,
            ...detectEngagement(description),
          },
          tripFitSignals: detectTripFit(fullText),
          raw: { ...item, feedUrl, podcastId },
        };

        if (lead.email) totalEmailsFound++;
        batchLeads.push(lead);
        leads.push(lead);
      }

      await appendAndSave(`Podcasts: "${kw}" found ${batchLeads.length} podcasts (skipped ${skippedDupe} dupes, ${skippedFilter} filtered). Total: ${leads.length} leads, ${totalEmailsFound} with email from description`);

      const batchLeadsWithRss = batchLeads.filter(l =>
        !l.email && l.ownedChannels?.rss && l.ownedChannels.rss.startsWith("http")
      );
      if (batchLeadsWithRss.length > 0) {
        await appendAndSave(`RSS inline: scraping ${batchLeadsWithRss.length} feeds for emails... [${totalEmailsFound}/${emailTarget} emails]`);

        const RSS_SUB_BATCH_SIZE = 40;
        let rssEmailsFound = 0;

        for (let subIdx = 0; subIdx < batchLeadsWithRss.length; subIdx += RSS_SUB_BATCH_SIZE) {
          const subBatch = batchLeadsWithRss.slice(subIdx, subIdx + RSS_SUB_BATCH_SIZE);

          try {
            const startUrls = subBatch.map(l => ({ url: l.ownedChannels!.rss! }));
            const { items: rssResults, costUsd: rssCost } = await runActorAndGetResults("apify~cheerio-scraper", {
              startUrls,
              maxCrawlPages: subBatch.length,
              maxConcurrency: 30,
              requestTimeoutSecs: 30,
              pageFunction: `async function pageFunction(context) {
  const { body, request } = context;
  const text = typeof body === 'string' ? body : body.toString('utf8');
  var ownerEmail = '';
  var ownerName = '';
  var link = '';
  var author = '';
  var emails = [];
  var ownerMatch = text.match(/<itunes:owner>[\\s\\S]*?<itunes:email>([^<]+)<\\/itunes:email>[\\s\\S]*?<\\/itunes:owner>/i);
  if (ownerMatch) ownerEmail = ownerMatch[1].trim();
  var nameMatch = text.match(/<itunes:owner>[\\s\\S]*?<itunes:name>([^<]+)<\\/itunes:name>[\\s\\S]*?<\\/itunes:owner>/i);
  if (nameMatch) ownerName = nameMatch[1].trim();
  var linkMatch = text.match(/<link>([^<]+)<\\/link>/);
  if (linkMatch) link = linkMatch[1].trim();
  var authorMatch = text.match(/<itunes:author>([^<]+)<\\/itunes:author>/);
  if (authorMatch) author = authorMatch[1].trim();
  var emailRegex = /[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}/g;
  var found = text.match(emailRegex) || [];
  for (var i = 0; i < Math.min(found.length, 10); i++) { emails.push(found[i]); }
  var urls = [];
  var urlRegex = /https?:\\/\\/[^\\s<>"'\\)\\]\\},]+/g;
  var urlFound = text.substring(0, 20000).match(urlRegex) || [];
  for (var j = 0; j < Math.min(urlFound.length, 50); j++) { urls.push(urlFound[j]); }
  return { url: request.url, ownerEmail: ownerEmail, ownerName: ownerName, link: link, author: author, emails: emails, urls: urls };
}`,
            }, 180000);
            await storage.incrementApifySpend(runId, rssCost);

            for (const result of rssResults) {
              const feedUrl = result.url || "";
              const matchLead = subBatch.find(l => {
                try {
                  return l.ownedChannels?.rss && feedUrl.toLowerCase().includes(
                    new URL(l.ownedChannels.rss).hostname.replace(/^www\./, "").toLowerCase()
                  ) && feedUrl.toLowerCase().includes(
                    new URL(l.ownedChannels.rss).pathname.split("/").filter(Boolean).slice(0, 2).join("/").toLowerCase()
                  );
                } catch { return false; }
              });

              if (!matchLead) continue;

              const ownerEmail = result.ownerEmail || "";
              if (ownerEmail && !isBlockedEmail(ownerEmail) && !matchLead.email) {
                matchLead.email = cleanEmail(ownerEmail);
                if (matchLead.email) {
                  rssEmailsFound++;
                  totalEmailsFound++;
                }
              }

              if (!matchLead.email) {
                const allEmails: string[] = result.emails || [];
                for (const email of allEmails) {
                  if (!isBlockedEmail(email)) {
                    matchLead.email = cleanEmail(email);
                    if (matchLead.email) {
                      rssEmailsFound++;
                      totalEmailsFound++;
                      break;
                    }
                  }
                }
              }

              const ownerName = result.ownerName || result.author || "";
              if (ownerName && (!matchLead.leaderName || matchLead.leaderName === matchLead.communityName)) {
                matchLead.leaderName = ownerName;
              }

              const rssLink = result.link || "";
              if (rssLink && rssLink.startsWith("http") && !rssLink.includes("podcasts.apple.com") && !matchLead.ownedChannels?.website) {
                matchLead.ownedChannels!.website = rssLink;
              }

              const rssUrls: string[] = result.urls || [];
              for (const url of rssUrls) {
                if (!url || !url.startsWith("http")) continue;
                try {
                  const host = new URL(url).hostname.replace(/^www\./, "");
                  if (host.includes("linkedin.com") && url.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
                    matchLead.ownedChannels!.linkedin = url.split("?")[0];
                  }
                  if (host.includes("instagram.com") && !matchLead.ownedChannels?.instagram) {
                    matchLead.ownedChannels!.instagram = url.split("?")[0];
                  }
                  if ((host.includes("twitter.com") || host === "x.com") && !matchLead.ownedChannels?.twitter) {
                    matchLead.ownedChannels!.twitter = url.split("?")[0];
                  }
                  if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link")) && !matchLead.ownedChannels?.linktree) {
                    matchLead.ownedChannels!.linktree = url.split("?")[0];
                  }
                } catch {}
              }
            }
          } catch (err: any) {
            if (err.costUsd) {
              await storage.incrementApifySpend(runId, err.costUsd);
            }
            await appendAndSave(`[WARN] RSS inline sub-batch ${Math.floor(subIdx / RSS_SUB_BATCH_SIZE) + 1} failed (${subBatch.length} feeds): ${err.message}`);
          }
        }

        await appendAndSave(`RSS inline: +${rssEmailsFound} emails from feeds. Progress: ${totalEmailsFound}/${emailTarget} emails, ${leads.length} total leads`);
      }
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Podcast search failed for "${kw}": ${err.message}`);
    }
  }

  const withWebsite = leads.filter(l => l.ownedChannels?.website).length;
  const withRss = leads.filter(l => l.ownedChannels?.rss).length;
  const withEmail = leads.filter(l => l.email).length;
  const reason = withEmail >= emailTarget ? "email target reached" : "all keywords exhausted";
  await appendAndSave(`Podcasts: DONE (${reason}) — ${leads.length} total leads, ${withEmail}/${emailTarget} emails found (${withWebsite} with website, ${withRss} with RSS feed)`);

  return leads;
}

async function enrichFromRssFeeds(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsWithRss = leads.filter(l =>
    l.ownedChannels?.rss && l.ownedChannels.rss.startsWith("http")
  );

  if (leadsWithRss.length === 0) return;

  await appendAndSave(`RSS feed scrape: ${leadsWithRss.length} podcast feeds to parse for host emails...`);

  const startUrls = leadsWithRss.map(l => ({ url: l.ownedChannels!.rss! }));

  try {
    const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~cheerio-scraper", {
      startUrls,
      maxCrawlPages: leadsWithRss.length,
      maxConcurrency: 30,
      pageFunction: `async function pageFunction(context) {
  const { body, request } = context;
  const text = typeof body === 'string' ? body : body.toString('utf8');
  var ownerEmail = '';
  var ownerName = '';
  var link = '';
  var author = '';
  var emails = [];
  var ownerMatch = text.match(/<itunes:owner>[\\s\\S]*?<itunes:email>([^<]+)<\\/itunes:email>[\\s\\S]*?<\\/itunes:owner>/i);
  if (ownerMatch) ownerEmail = ownerMatch[1].trim();
  var nameMatch = text.match(/<itunes:owner>[\\s\\S]*?<itunes:name>([^<]+)<\\/itunes:name>[\\s\\S]*?<\\/itunes:owner>/i);
  if (nameMatch) ownerName = nameMatch[1].trim();
  var linkMatch = text.match(/<link>([^<]+)<\\/link>/);
  if (linkMatch) link = linkMatch[1].trim();
  var authorMatch = text.match(/<itunes:author>([^<]+)<\\/itunes:author>/);
  if (authorMatch) author = authorMatch[1].trim();
  var emailRegex = /[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}/g;
  var found = text.match(emailRegex) || [];
  for (var i = 0; i < Math.min(found.length, 10); i++) { emails.push(found[i]); }
  var urls = [];
  var urlRegex = /https?:\\/\\/[^\\s<>"'\\)\\]\\},]+/g;
  var urlFound = text.substring(0, 20000).match(urlRegex) || [];
  for (var j = 0; j < Math.min(urlFound.length, 50); j++) { urls.push(urlFound[j]); }
  return { url: request.url, ownerEmail: ownerEmail, ownerName: ownerName, link: link, author: author, emails: emails, urls: urls };
}`,
    }, 120000);
    await storage.incrementApifySpend(runId, actorCost);

    let enrichedCount = 0;
    let emailsFound = 0;

    for (const result of results) {
      const feedUrl = result.url || "";
      const matchLead = leadsWithRss.find(l =>
        l.ownedChannels?.rss && feedUrl.toLowerCase().includes(
          new URL(l.ownedChannels.rss).hostname.replace(/^www\./, "").toLowerCase()
        ) && feedUrl.toLowerCase().includes(
          new URL(l.ownedChannels.rss).pathname.split("/").filter(Boolean).slice(0, 2).join("/").toLowerCase()
        )
      );

      if (!matchLead) continue;

      let foundAnything = false;

      const ownerEmail = result.ownerEmail || "";
      if (ownerEmail && !isBlockedEmail(ownerEmail) && !matchLead.email) {
        matchLead.email = cleanEmail(ownerEmail);
        if (matchLead.email) {
          emailsFound++;
          foundAnything = true;
        }
      }

      if (!matchLead.email) {
        const allEmails: string[] = result.emails || [];
        for (const email of allEmails) {
          if (!isBlockedEmail(email)) {
            matchLead.email = cleanEmail(email);
            if (matchLead.email) {
              emailsFound++;
              foundAnything = true;
              break;
            }
          }
        }
      }

      const ownerName = result.ownerName || result.author || "";
      if (ownerName && (!matchLead.leaderName || matchLead.leaderName === matchLead.communityName)) {
        matchLead.leaderName = ownerName;
        foundAnything = true;
      }

      const rssLink = result.link || "";
      if (rssLink && rssLink.startsWith("http") && !rssLink.includes("podcasts.apple.com") && !matchLead.ownedChannels?.website) {
        matchLead.ownedChannels!.website = rssLink;
        foundAnything = true;
      }

      const rssUrls: string[] = result.urls || [];
      for (const url of rssUrls) {
        if (!url || !url.startsWith("http")) continue;
        try {
          const host = new URL(url).hostname.replace(/^www\./, "");
          if (host.includes("linkedin.com") && url.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
            matchLead.ownedChannels!.linkedin = url.split("?")[0];
            foundAnything = true;
          }
          if (host.includes("instagram.com") && !matchLead.ownedChannels?.instagram) {
            matchLead.ownedChannels!.instagram = url.split("?")[0];
            foundAnything = true;
          }
          if ((host.includes("twitter.com") || host === "x.com") && !matchLead.ownedChannels?.twitter) {
            matchLead.ownedChannels!.twitter = url.split("?")[0];
            foundAnything = true;
          }
          if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link")) && !matchLead.ownedChannels?.linktree) {
            matchLead.ownedChannels!.linktree = url.split("?")[0];
            foundAnything = true;
          }
        } catch {}
      }

      if (foundAnything) enrichedCount++;
    }

    await appendAndSave(`RSS feed scrape: enriched ${enrichedCount}/${leadsWithRss.length} leads (${emailsFound} direct emails from RSS feeds)`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] RSS feed scraping failed: ${err.message}`);
  }
}

async function scrapeSubstackWriters(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenSlugs = new Set<string>();
  const SUBSTACK_SYSTEM_SLUGS = ["support", "www", "open", "blog", "help", "on", "app", "reader", "api", "cdn", "email", "newsletter"];
  const SUBSTACK_CDN_PATTERN = /substackcdn\.com|substack-post-media\.s3|bucketeer-.*\.s3|amazonaws\.com/i;

  const googleQueries = keywords.flatMap(kw => [
    `site:substack.com/about "${kw}"`,
    `site:substack.com "${kw}" newsletter writer`,
    `site:substack.com "${kw}" inurl:about`,
  ]);
  const batchSize = 10;

  for (let i = 0; i < googleQueries.length; i += batchSize) {
    if (leads.length >= maxItems) break;
    const batch = googleQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`Substack (via Google): batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(googleQueries.length / batchSize)} (${leads.length}/${maxItems} found)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 3,
        resultsPerPage: 20,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        const organicResults = item.organicResults || [];
        for (const result of organicResults) {
          if (leads.length >= maxItems) break;

          const rawUrl = result.url || result.link || "";
          if (!rawUrl.includes("substack.com")) continue;

          let substackSlug = "";
          const subdomainMatch = rawUrl.match(/https?:\/\/([a-zA-Z0-9-]+)\.substack\.com/);
          if (subdomainMatch) {
            substackSlug = subdomainMatch[1];
          }
          const atMatch = rawUrl.match(/substack\.com\/@([a-zA-Z0-9_-]+)/);
          if (!substackSlug && atMatch) {
            substackSlug = atMatch[1];
          }
          const openPubMatch = rawUrl.match(/open\.substack\.com\/pub\/([a-zA-Z0-9_-]+)/);
          if (!substackSlug && openPubMatch) {
            substackSlug = openPubMatch[1];
          }
          if (!substackSlug) continue;
          if (SUBSTACK_SYSTEM_SLUGS.includes(substackSlug)) continue;

          if (seenSlugs.has(substackSlug)) continue;
          seenSlugs.add(substackSlug);

          const substackUrl = `https://${substackSlug}.substack.com`;

          const title = result.title || "";
          const snippet = result.description || result.snippet || "";
          const fullText = `${title} ${snippet}`;

          const writerName = title
            .replace(/\s*\|\s*Substack$/i, "")
            .replace(/\s*[-–—]\s*Substack$/i, "")
            .replace(/\s*Substack\s*$/i, "")
            .replace(/\s*[-–—]\s*by\s+.*$/i, "")
            .replace(/^About\s*[-–—]\s*/i, "")
            .trim();

          const channels: Record<string, string> = { substack: substackUrl };
          const descUrls = extractUrlsFromText(snippet);
          const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
          Object.assign(channels, socialFromDesc);

          const descEmails = extractEmailsFromText(snippet);

          leads.push({
            source: "substack",
            communityName: writerName || substackSlug,
            communityType: detectCommunityType(fullText),
            description: snippet.substring(0, 2000),
            location: "",
            website: substackUrl,
            email: descEmails[0] || "",
            phone: "",
            leaderName: "",
            memberCount: 0,
            subscriberCount: 0,
            ownedChannels: channels,
            monetizationSignals: { ...detectMonetization(snippet), newsletter: true, subscriber_base: true },
            engagementSignals: detectEngagement(snippet),
            tripFitSignals: detectTripFit(fullText),
            raw: result,
          });
        }
      }

      await appendAndSave(`Substack: found ${leads.length} unique publications so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Substack Google search batch failed: ${err.message}`);
    }
  }

  if (leads.length > 0) {
    await appendAndSave(`Substack: scraping ${leads.length} publication about pages + API for contact info...`);
    const SUB_BATCH = 20;
    let enrichedCount = 0;
    let apiEmailCount = 0;

    for (let i = 0; i < leads.length; i += SUB_BATCH) {
      const subBatch = leads.slice(i, i + SUB_BATCH);
      const slugMap = new Map<string, PlatformLead>();
      const scrapeUrls: { url: string }[] = [];
      for (const l of subBatch) {
        const slug = l.ownedChannels?.substack?.match(/https?:\/\/([a-zA-Z0-9-]+)\.substack\.com/)?.[1] || "";
        if (!slug) continue;
        slugMap.set(slug, l);
        scrapeUrls.push(
          { url: `https://${slug}.substack.com/about` },
          { url: `https://${slug}.substack.com/api/v1/publication` },
        );
      }
      if (scrapeUrls.length === 0) continue;

      try {
        const { items: pageResults, costUsd: scrapeCost } = await runActorAndGetResults("apify~cheerio-scraper", {
          startUrls: scrapeUrls,
          maxCrawlPages: scrapeUrls.length,
          maxConcurrency: 30,
          requestTimeoutSecs: 30,
          pageFunction: `async function pageFunction(context) {
  const { $, request, body } = context;
  var url = request.url;
  var isApi = url.indexOf('/api/v1/publication') !== -1;

  if (isApi) {
    try {
      var bodyStr = typeof body === 'string' ? body : (body && body.toString ? body.toString() : '');
      var data = JSON.parse(bodyStr);
      var authorName = '';
      var authorEmail = '';
      var authorPhoto = '';
      if (data.author_name) authorName = data.author_name;
      if (data.author_email) authorEmail = data.author_email;
      if (data.author && data.author.name) authorName = authorName || data.author.name;
      if (data.owner && data.owner.name) authorName = authorName || data.owner.name;
      if (data.owner && data.owner.email) authorEmail = authorEmail || data.owner.email;
      if (data.editors && data.editors.length > 0) {
        var ed = data.editors[0];
        if (ed.name && !authorName) authorName = ed.name;
        if (ed.profile_photo_url) authorPhoto = ed.profile_photo_url;
      }
      if (data.name) {
        var pubName = data.name;
      }
      return {
        url: url,
        isApi: true,
        authorName: authorName,
        authorEmail: authorEmail,
        pubName: data.name || '',
        subscriberCount: data.subscriber_count || data.subscribers || 0,
        description: data.description || data.about || '',
      };
    } catch(e) {
      return { url: url, isApi: true, error: 'parse failed' };
    }
  }

  var emails = [];
  var socialLinks = [];
  var authorName = '';
  var subscriberCount = '';
  var description = '';
  var websiteUrl = '';

  var text = $('body').text() || '';
  var emailPattern = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
  var found = text.match(emailPattern) || [];
  found.forEach(function(e) {
    if (e && !e.match(/\\.png|\\.jpg|\\.gif|\\.jpeg|\\.webp|\\.svg|substack\\.com|sentry\\.io|example\\.com|cloudfront\\.net|amazonaws\\.com|substackcdn/i)) {
      emails.push(e.toLowerCase().trim());
    }
  });

  var obfuscatedEmail = text.match(/[a-zA-Z0-9._%+-]+\\s*(?:\\[at\\]|\\(at\\)|\\{at\\}|\\bat\\b)\\s*[a-zA-Z0-9.-]+\\s*(?:\\[dot\\]|\\(dot\\)|\\{dot\\}|\\.)[a-zA-Z]{2,}/gi);
  if (obfuscatedEmail) {
    obfuscatedEmail.forEach(function(raw) {
      var cleaned = raw.replace(/\\s*(?:\\[at\\]|\\(at\\)|\\{at\\}|\\bat\\b)\\s*/gi, '@').replace(/\\s*(?:\\[dot\\]|\\(dot\\)|\\{dot\\})\\s*/gi, '.').trim().toLowerCase();
      if (cleaned.match(/@/) && !cleaned.match(/substack|sentry|example/i)) {
        emails.push(cleaned);
      }
    });
  }

  $('a[href]').each(function() {
    var href = $(this).attr('href') || '';
    if (href.match(/twitter\\.com|x\\.com|instagram\\.com|youtube\\.com|linkedin\\.com|facebook\\.com|tiktok\\.com|linktr\\.ee|beacons\\.ai|bio\\.link/i)) {
      socialLinks.push(href);
    }
    if (href.match(/^https?:\\/\\//) && !href.match(/substack\\.com|substackcdn|twitter\\.com|x\\.com|instagram\\.com|youtube\\.com|linkedin\\.com|facebook\\.com|tiktok\\.com|apple\\.com|spotify\\.com|google\\.com|amazonaws\\.com|cloudfront|sentry\\.io/i)) {
      if (!websiteUrl && !href.match(/\\.(png|jpg|jpeg|gif|svg|webp|ico|css|js)($|\\?)/i)) {
        websiteUrl = href;
      }
    }
  });

  $('a[href*="mailto:"]').each(function() {
    var mailto = $(this).attr('href') || '';
    var em = mailto.replace('mailto:', '').split('?')[0].trim().toLowerCase();
    if (em && em.indexOf('@') > 0 && !em.match(/substack|sentry|example/i)) {
      emails.push(em);
    }
  });

  var nameEl = $('h1, .publication-name, [class*="pub-name"], [class*="publication-name"]');
  if (nameEl.length) {
    var candidate = nameEl.first().text().trim();
    if (candidate && candidate.length < 100) authorName = candidate;
  }

  var authorEl = $('[class*="author"], [class*="byline"], .pencraft a[href*="/@"]');
  if (authorEl.length) {
    var aCandidate = authorEl.first().text().trim();
    if (aCandidate && aCandidate.length < 80 && aCandidate.length > 1) {
      if (!authorName || authorName === aCandidate) authorName = aCandidate;
      else authorName = aCandidate + ' (' + authorName + ')';
    }
  }

  var bioText = $('[class*="about"], [class*="bio"], [class*="description"], .pencraft p').text() || '';
  var namePatterns = [
    /(?:I'm|I am|my name is|hi,? I'm|hello,? I'm|hey,? I'm)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+){0,2})/i,
    /(?:written by|by|author:?)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+){0,2})/i,
  ];
  for (var p = 0; p < namePatterns.length; p++) {
    var nm = bioText.match(namePatterns[p]);
    if (nm && nm[1]) {
      authorName = nm[1].trim();
      break;
    }
  }

  var subEl = $('[class*="subscriber"], [class*="readers"]');
  if (subEl.length) subscriberCount = subEl.first().text().trim();

  var descEl = $('meta[name="description"]');
  if (descEl.length) description = descEl.attr('content') || '';
  if (!description) {
    var ogDesc = $('meta[property="og:description"]');
    if (ogDesc.length) description = ogDesc.attr('content') || '';
  }

  return {
    url: request.url,
    isApi: false,
    emails: [...new Set(emails)],
    socialLinks: [...new Set(socialLinks)],
    authorName: authorName,
    subscriberCount: subscriberCount,
    description: description,
    websiteUrl: websiteUrl,
  };
}`,
        }, 180000);
        await storage.incrementApifySpend(runId, scrapeCost);

        for (const pageData of pageResults) {
          const pageUrl = pageData.url || "";
          const slugMatch = pageUrl.match(/https?:\/\/([a-zA-Z0-9-]+)\.substack\.com/);
          if (!slugMatch) continue;
          const slug = slugMatch[1];

          const lead = slugMap.get(slug);
          if (!lead) continue;

          if (pageData.isApi) {
            if (pageData.authorEmail && !lead.email) {
              const apiEmail = cleanEmail(pageData.authorEmail);
              if (apiEmail && !isBlockedEmail(apiEmail)) {
                lead.email = apiEmail;
                apiEmailCount++;
              }
            }
            if (pageData.authorName && !lead.leaderName) {
              lead.leaderName = pageData.authorName;
            }
            if (pageData.pubName && (!lead.communityName || lead.communityName === slug)) {
              lead.communityName = pageData.pubName;
            }
            const apiSubCount = typeof pageData.subscriberCount === 'number'
              ? pageData.subscriberCount
              : parseInt(String(pageData.subscriberCount || "0").replace(/,/g, "")) || 0;
            if (apiSubCount > 0) {
              lead.subscriberCount = apiSubCount;
              lead.memberCount = apiSubCount;
              lead.engagementSignals = {
                ...lead.engagementSignals,
                subscriber_count: apiSubCount,
              };
            }
            if (pageData.description && (!lead.description || lead.description.length < 50)) {
              lead.description = pageData.description.substring(0, 2000);
            }
            continue;
          }

          if (pageData.emails?.length > 0) {
            const validEmail = pageData.emails.find((e: string) => !isBlockedEmail(e));
            if (validEmail && !lead.email) {
              lead.email = cleanEmail(validEmail);
              enrichedCount++;
            }
          }

          if (pageData.authorName) {
            const cleanedName = pageData.authorName
              .replace(/\s*\(.*\)$/, "")
              .trim();
            if (cleanedName && cleanedName.length > 1 && cleanedName.length < 80) {
              lead.leaderName = cleanedName;
            }
          }

          if (!lead.leaderName && lead.communityName) {
            lead.leaderName = extractRealNameFromAbout(lead.description || "", lead.communityName) || lead.communityName;
          }

          if (pageData.subscriberCount) {
            const subMatch = pageData.subscriberCount.match(/[\d,]+/);
            if (subMatch) {
              const parsed = parseInt(subMatch[0].replace(/,/g, "")) || 0;
              if (parsed > 0 && (!lead.subscriberCount || parsed > lead.subscriberCount)) {
                lead.subscriberCount = parsed;
                lead.memberCount = parsed;
                lead.engagementSignals = {
                  ...lead.engagementSignals,
                  subscriber_count: parsed,
                };
              }
            }
          }

          if (pageData.description && (!lead.description || lead.description.length < 50)) {
            lead.description = pageData.description.substring(0, 2000);
          }

          if (pageData.websiteUrl && !SUBSTACK_CDN_PATTERN.test(pageData.websiteUrl)) {
            lead.ownedChannels = lead.ownedChannels || {};
            if (!lead.ownedChannels.website) {
              lead.ownedChannels.website = pageData.websiteUrl;
              lead.website = pageData.websiteUrl;
            }
          }

          const socialLinks = pageData.socialLinks || [];
          const socialChannels = extractSocialChannelsFromUrls(socialLinks);
          lead.ownedChannels = { ...lead.ownedChannels, ...socialChannels };

          const linkAggUrls = extractLinkAggregatorUrls(socialLinks.join(" "), lead.ownedChannels);
          if (linkAggUrls.length > 0 && !lead.ownedChannels.linktree) {
            lead.ownedChannels.linktree = linkAggUrls[0];
          }
        }
      } catch (err: any) {
        if (err.costUsd) {
          await storage.incrementApifySpend(runId, err.costUsd);
        }
        await appendAndSave(`[WARN] Substack page scrape batch failed: ${err.message}`);
      }
    }

    for (const lead of leads) {
      if (lead.website && lead.website.includes("substack.com")) {
        if (lead.ownedChannels?.website && !SUBSTACK_CDN_PATTERN.test(lead.ownedChannels.website)) {
          lead.website = lead.ownedChannels.website;
        }
      }
    }

    await appendAndSave(`Substack scrape: ${enrichedCount} emails from about pages, ${apiEmailCount} from API, scraped ${leads.length} publications`);
  }

  await appendAndSave(`Substack discovery complete: ${leads.length} publications found`);
  return leads;
}

function cleanMightyName(raw: string): string {
  let name = raw
    .replace(/\s*\|\s*Mighty Networks$/i, "")
    .replace(/\s*[-–—]\s*Mighty Networks$/i, "")
    .replace(/\s*Mighty Networks\s*$/i, "")
    .replace(/\s*[-–—]\s*Join\s+.*$/i, "")
    .replace(/^(About|Home|Welcome|Events|Feed|Members|Courses?|Posts?)\s*\|\s*/i, "")
    .replace(/^(About|Home|Welcome to|Join)\s+/i, "")
    .replace(/\s*\|\s*(About|Home|Feed|Events|Members|Courses?|Posts?)\s*$/i, "")
    .replace(/\bJoin\b[\s\S]*?Sign\s*In/gi, "")
    .replace(/\bSign\s*In\b/gi, "")
    .replace(/\bJoin\s*(Now|Us|Free|Today)?\b/gi, "")
    .replace(/\bLog\s*In\b/gi, "")
    .replace(/\bDon'?t?\b\s*$/i, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (name.length > 120) name = name.substring(0, 120).trim();
  return name;
}

const MIGHTY_JUNK_NAMES = new Set([
  "join", "sign in", "log in", "about", "home", "community", "tribe", "collective",
  "network", "group", "club", "hub", "forum", "members", "events", "courses",
  "support", "help", "contact", "info", "admin", "team", "staff", "host",
  "survivors", "judgment", "professional", "volunteers", "friends", "people",
  "welcome", "subscribe", "newsletter", "donate", "blog", "podcast",
]);

function isLikelyPersonName(name: string): boolean {
  if (!name || name.length < 3) return false;
  const words = name.trim().split(/\s+/);
  if (words.length < 2 || words.length > 5) return false;
  if (MIGHTY_JUNK_NAMES.has(name.toLowerCase())) return false;
  for (const w of words) {
    if (MIGHTY_JUNK_NAMES.has(w.toLowerCase())) return false;
  }
  if (/\b(and|or|the|for|with|from|that|this|their|our|your|ltd|llc|inc|org|co)\s*$/i.test(name)) return false;
  if (/^\d+$/.test(name)) return false;
  if (!/^[A-Z]/.test(words[0])) return false;
  if (words.every(w => w === w.toUpperCase()) && name.length > 5) return false;
  if (!words.every(w => /^[A-Z][a-z]+$/.test(w) || w.length <= 3)) return false;
  return true;
}

function extractNameFromEmail(email: string): string {
  if (!email) return "";
  const prefix = email.split("@")[0];
  if (!prefix || prefix.length < 3) return "";
  const genericPrefixes = ["info", "hello", "contact", "support", "admin", "team",
    "help", "office", "sales", "mail", "noreply", "no-reply", "webmaster",
    "billing", "service", "enquiries", "general", "press", "media",
    "first", "last", "email", "emailsupport", "donate", "news",
    "corporategiving", "betrayalpod"];
  if (genericPrefixes.includes(prefix.toLowerCase().replace(/[._-]/g, ""))) return "";
  const parts = prefix.split(/[._-]+/).filter(p => p.length >= 2);
  if (parts.length < 2 || parts.length > 3) return "";
  if (parts.some(p => /\d{2,}/.test(p))) return "";
  if (parts.some(p => p.length > 12)) return "";
  const capitalized = parts.map(p => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase());
  const name = capitalized.join(" ");
  if (name.length < 5 || name.length > 40) return "";
  return name;
}

function extractNameFromLinkedIn(linkedinUrl: string): string {
  if (!linkedinUrl) return "";
  const match = linkedinUrl.match(/linkedin\.com\/in\/([a-zA-Z0-9-]+)/);
  if (!match) return "";
  let slug = match[1];
  slug = slug.replace(/-?\d{3,}$/, "").replace(/-+$/, "");
  if (!slug || slug.length < 3) return "";
  const junkSuffixes = ["contact", "profile", "official", "page", "real", "main", "cmt", "phd", "mba", "md", "esq", "tribe", "community", "club", "group"];
  const parts = slug.split("-").filter(p => p.length >= 2 && !/^\d+$/.test(p));
  const cleanParts = parts.filter(p => !junkSuffixes.includes(p.toLowerCase()));
  if (cleanParts.length < 2 || cleanParts.length > 4) return "";
  if (cleanParts.some(p => p.length > 15)) return "";
  const capitalized = cleanParts.map(p => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase());
  return capitalized.join(" ");
}

function cleanMightyLeaderName(raw: string): string {
  if (!raw) return "";
  let name = raw
    .replace(/\bJoin\b[\s\S]*?Sign\s*In/gi, "")
    .replace(/\bSign\s*In\b/gi, "")
    .replace(/\bJoin\s*(Now|Us|Free|Today)?\b/gi, "")
    .replace(/\bLog\s*In\b/gi, "")
    .replace(/\bDon'?t?\b/gi, "")
    .replace(/\bHosted?\s+by\b/gi, "")
    .replace(/\bCreated?\s+by\b/gi, "")
    .replace(/\b(About|Home|Feed|Events|Members|Courses?|Posts?)\b/gi, "")
    .replace(/[|•·\-–—]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (name.length < 2 || name.length > 80) return "";
  if (!/[a-zA-Z]{2,}/.test(name)) return "";
  return name;
}

async function scrapeMightyNetworks(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  geos: string[] = [],
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenSlugs = new Set<string>();
  const MIGHTY_SYSTEM_SLUGS = ["community", "www", "api", "app", "help", "support", "faq", "docs", "blog", "status"];

  const MIGHTY_KEYWORD_EXPANSIONS: Record<string, string[]> = {
    "hiking": ["hiking", "trail", "backpacking", "outdoors", "outdoor adventure"],
    "outdoor": ["outdoor", "outdoors", "nature", "adventure", "wilderness"],
    "yoga": ["yoga", "mindfulness", "meditation", "wellness yoga"],
    "fitness": ["fitness", "workout", "training", "gym", "health fitness"],
    "travel": ["travel", "adventure travel", "group travel", "wanderlust"],
    "coaching": ["coaching", "life coach", "business coaching", "mentor"],
    "wellness": ["wellness", "holistic", "health wellness", "self-care"],
    "community": ["community", "tribe", "collective", "network"],
    "running": ["running", "run club", "marathon", "runners"],
    "photography": ["photography", "photo", "photographers"],
  };

  const expandedKeywords = new Set<string>();
  for (const kw of keywords) {
    expandedKeywords.add(kw);
    const kwLower = kw.toLowerCase();
    for (const [trigger, expansions] of Object.entries(MIGHTY_KEYWORD_EXPANSIONS)) {
      if (kwLower.includes(trigger)) {
        for (const exp of expansions) {
          if (exp.toLowerCase() !== kwLower) expandedKeywords.add(exp);
        }
      }
    }
  }

  const allKeywords = Array.from(expandedKeywords);
  const googleQueries: string[] = [];

  for (const kw of allKeywords) {
    googleQueries.push(`site:mn.co "${kw}"`);
    googleQueries.push(`site:mn.co ${kw} community`);
  }

  for (const kw of keywords) {
    googleQueries.push(`site:mightynetworks.com "${kw}" community`);
    googleQueries.push(`"mn.co" ${kw} join community`);
    googleQueries.push(`"mighty networks" "${kw}" community`);
    googleQueries.push(`mn.co ${kw} group membership`);
  }

  if (geos.length > 0) {
    for (const kw of keywords) {
      for (const geo of geos.slice(0, 3)) {
        googleQueries.push(`site:mn.co "${kw}" "${geo}"`);
        googleQueries.push(`"mighty networks" "${kw}" "${geo}" community`);
      }
    }
  }

  const MAX_QUERIES = 40;
  const uniqueQueries = Array.from(new Set(googleQueries)).slice(0, MAX_QUERIES);
  await appendAndSave(`Mighty Networks: ${uniqueQueries.length} search queries (${allKeywords.length} keywords${geos.length > 0 ? `, ${geos.length} geos` : ""})`);
  const batchSize = 10;

  for (let i = 0; i < uniqueQueries.length; i += batchSize) {
    if (leads.length >= maxItems) break;
    const batch = uniqueQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`Mighty Networks (via Google): batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(uniqueQueries.length / batchSize)} (${leads.length}/${maxItems} found)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 5,
        resultsPerPage: 20,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        const organicResults = item.organicResults || [];
        for (const result of organicResults) {
          if (leads.length >= maxItems) break;

          const rawUrl = result.url || result.link || "";
          let mightySlug = "";
          const mnCoMatch = rawUrl.match(/https?:\/\/([a-zA-Z0-9-]+)\.mn\.co/);
          if (mnCoMatch) {
            mightySlug = mnCoMatch[1];
          }
          const mightyMatch = rawUrl.match(/https?:\/\/([a-zA-Z0-9-]+)\.mightynetworks\.com/);
          if (!mightySlug && mightyMatch) {
            mightySlug = mightyMatch[1];
          }
          if (!mightySlug) continue;
          if (MIGHTY_SYSTEM_SLUGS.includes(mightySlug)) continue;

          if (seenSlugs.has(mightySlug)) continue;
          seenSlugs.add(mightySlug);

          const mightyUrl = `https://${mightySlug}.mn.co`;

          const title = result.title || "";
          const snippet = result.description || result.snippet || "";
          const fullText = `${title} ${snippet}`;

          let communityName = cleanMightyName(title);
          if (!communityName) communityName = mightySlug;

          const channels: Record<string, string> = { mighty: mightyUrl };
          const descUrls = extractUrlsFromText(snippet);
          const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
          Object.assign(channels, socialFromDesc);

          const descEmails = extractEmailsFromText(snippet);

          leads.push({
            source: "mighty",
            communityName: communityName,
            communityType: detectCommunityType(fullText),
            description: snippet.substring(0, 2000),
            location: "",
            website: mightyUrl,
            email: descEmails[0] || "",
            phone: "",
            leaderName: "",
            memberCount: 0,
            subscriberCount: 0,
            ownedChannels: channels,
            monetizationSignals: { ...detectMonetization(snippet), membership: true, paid_community: true },
            engagementSignals: detectEngagement(snippet),
            tripFitSignals: detectTripFit(fullText),
            raw: result,
          });
        }
      }

      await appendAndSave(`Mighty Networks: found ${leads.length} unique communities so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Mighty Networks Google search batch failed: ${err.message}`);
    }
  }

  if (leads.length > 0) {
    await appendAndSave(`Mighty Networks: scraping ${leads.length} community landing pages for host info...`);
    const SUB_BATCH = 20;
    let enrichedCount = 0;

    for (let i = 0; i < leads.length; i += SUB_BATCH) {
      const subBatch = leads.slice(i, i + SUB_BATCH);
      const slugMap = new Map<string, PlatformLead>();
      const scrapeUrls: { url: string }[] = [];
      for (const l of subBatch) {
        const slug = l.ownedChannels?.mighty?.match(/https?:\/\/([a-zA-Z0-9-]+)\.mn\.co/)?.[1] || "";
        if (!slug) continue;
        slugMap.set(slug, l);
        scrapeUrls.push({ url: `https://${slug}.mn.co/landing` });
      }
      if (scrapeUrls.length === 0) continue;

      try {
        const { items: pageResults, costUsd: scrapeCost } = await runActorAndGetResults("apify~cheerio-scraper", {
          startUrls: scrapeUrls,
          maxCrawlPages: scrapeUrls.length,
          maxConcurrency: 30,
          requestTimeoutSecs: 30,
          pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  var url = request.url;
  var text = $('body').text() || '';
  var hostName = '';
  var communityTitle = '';
  var description = '';
  var memberText = '';
  var emails = [];
  var links = [];

  var metaDesc = $('meta[name="description"]').attr('content') || '';
  var ogDesc = $('meta[property="og:description"]').attr('content') || '';
  description = metaDesc || ogDesc || '';

  var ogTitle = $('meta[property="og:title"]').attr('content') || '';
  var pageTitle = $('title').text() || '';
  communityTitle = ogTitle || pageTitle || '';

  // Try specific selectors for host/creator name - avoid nav/button elements
  var hostSelectors = [
    '.host-name', '.creator-name', '.author-name', '.organizer-name',
    '[data-testid*="host"]', '[data-testid*="creator"]',
    '.community-host', '.network-host'
  ];
  for (var si = 0; si < hostSelectors.length; si++) {
    var el = $(hostSelectors[si]);
    if (el.length) {
      var elText = el.first().text().trim();
      if (elText.length > 1 && elText.length < 80 && !/sign\\s*in|join|log\\s*in/i.test(elText)) {
        hostName = elText.substring(0, 100);
        break;
      }
    }
  }

  // Fallback: look for "hosted by" / "created by" patterns in body text
  if (!hostName) {
    var byMatch = text.match(/(?:hosted by|created by|run by|founded by|led by|built by|managed by|curated by)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+){0,3})/i);
    if (byMatch) {
      var candidate = byMatch[1].trim();
      if (!/sign\\s*in|join|log\\s*in|mighty|network/i.test(candidate)) {
        hostName = candidate;
      }
    }
  }

  // Fallback: look for og:site_name which sometimes has the host name
  if (!hostName) {
    var siteName = $('meta[property="og:site_name"]').attr('content') || '';
    if (siteName && siteName.length > 1 && siteName.length < 80 && !/mighty\\s*network|sign\\s*in|join/i.test(siteName)) {
      hostName = siteName;
    }
  }

  var memberMatch = text.match(/(\\d[\\d,]+)\\s*(?:members?|people|community members)/i);
  if (memberMatch) memberText = memberMatch[0];

  var allLinks = [];
  $('a[href]').each(function() {
    var href = $(this).attr('href') || '';
    if (href.startsWith('http') && !href.includes('mn.co') && !href.includes('mightynetworks.com') && !href.includes('apple.com/app') && !href.includes('play.google.com')) {
      allLinks.push(href);
    }
  });
  links = allLinks.slice(0, 20);

  var emailMatches = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g) || [];
  emails = emailMatches.filter(function(e) { return !e.includes('mightynetworks') && !e.includes('mn.co'); }).slice(0, 5);

  return { url: url, hostName: hostName, communityTitle: communityTitle, description: description, memberText: memberText, emails: emails, links: links };
}`,
        }, 120000);
        await storage.incrementApifySpend(runId, scrapeCost);

        for (const page of pageResults) {
          const pageUrl = page.url || "";
          const slugMatch = pageUrl.match(/https?:\/\/([a-zA-Z0-9-]+)\.mn\.co/);
          if (!slugMatch) continue;
          const slug = slugMatch[1];
          const lead = slugMap.get(slug);
          if (!lead) continue;

          if (page.communityTitle) {
            const cleanedTitle = cleanMightyName(page.communityTitle);
            if (cleanedTitle && cleanedTitle.length > 1) {
              lead.communityName = cleanedTitle;
            }
          }

          if (page.hostName) {
            const cleanedHost = cleanMightyLeaderName(page.hostName);
            if (cleanedHost && !lead.leaderName && isLikelyPersonName(cleanedHost)) {
              lead.leaderName = cleanedHost;
            }
          }

          if (page.description && (!lead.description || lead.description.length < 50)) {
            lead.description = page.description.substring(0, 2000);
          }

          if (page.memberText) {
            const memberMatch = page.memberText.match(/[\d,]+/);
            if (memberMatch) {
              const parsed = parseInt(memberMatch[0].replace(/,/g, "")) || 0;
              if (parsed > 0) {
                lead.memberCount = parsed;
                lead.engagementSignals = { ...lead.engagementSignals, member_count: parsed };
              }
            }
          }

          if (page.emails && page.emails.length > 0) {
            if (!lead.email) {
              lead.email = page.emails[0];
              enrichedCount++;
            }
          }

          if (page.links && page.links.length > 0) {
            const socialLinks = extractSocialChannelsFromUrls(page.links);
            lead.ownedChannels = { ...lead.ownedChannels, ...socialLinks };
            const personalWebsite = page.links.find((l: string) => {
              try {
                const domain = new URL(l).hostname.replace("www.", "");
                return !["youtube.com", "instagram.com", "twitter.com", "x.com", "facebook.com",
                  "linkedin.com", "tiktok.com", "discord.gg", "discord.com", "linktr.ee",
                  "beacons.ai", "mn.co", "mightynetworks.com", "apple.com", "google.com",
                  "play.google.com", "apps.apple.com"].some(s => domain.includes(s));
              } catch { return false; }
            });
            if (personalWebsite) {
              lead.ownedChannels = { ...lead.ownedChannels, website: personalWebsite };
              lead.website = personalWebsite;
            }
          }

          if (!lead.leaderName && lead.description) {
            const extractedName = extractRealNameFromAbout(lead.description, "");
            if (extractedName && isLikelyPersonName(extractedName)) {
              lead.leaderName = cleanMightyLeaderName(extractedName) || "";
            }
          }
        }
      } catch (err: any) {
        if (err.costUsd) {
          await storage.incrementApifySpend(runId, err.costUsd);
        }
        await appendAndSave(`[WARN] Mighty Networks page scrape batch failed: ${err.message}`);
      }
    }

    for (const lead of leads) {
      if (lead.website && (lead.website.includes("mn.co") || lead.website.includes("mightynetworks.com"))) {
        if (lead.ownedChannels?.website) {
          lead.website = lead.ownedChannels.website;
        }
      }
    }

    let nameExtractCount = 0;
    for (const lead of leads) {
      if (lead.leaderName) continue;

      const linkedinUrl = lead.ownedChannels?.linkedin || "";
      const linkedinName = extractNameFromLinkedIn(linkedinUrl);
      if (linkedinName && isLikelyPersonName(linkedinName)) {
        lead.leaderName = linkedinName;
        nameExtractCount++;
        continue;
      }

      const emailName = extractNameFromEmail(lead.email || "");
      if (emailName && isLikelyPersonName(emailName)) {
        lead.leaderName = emailName;
        nameExtractCount++;
        continue;
      }
    }

    await appendAndSave(`Mighty Networks scrape: ${enrichedCount} emails from landing pages, ${nameExtractCount} names from contacts, scraped ${leads.length} communities`);
  }

  await appendAndSave(`Mighty Networks discovery complete: ${leads.length} communities found`);
  return leads;
}

function deduplicateLinkedInKeywords(keywords: string[]): string[] {
  const rootMap: Record<string, string[]> = {
    "alumni": ["alumni group", "alumni network", "alumni association", "alumni groups", "alumni organizations", "alumni chapter", "alumni society"],
    "hiking": ["hiking group", "hiking club", "hiking groups", "hiking clubs", "hikers group", "hikers club"],
    "running": ["run club", "running group", "runners club", "running clubs", "run clubs", "runners group"],
    "church": ["church group", "faith group", "ministry group", "church groups", "faith groups", "ministry groups"],
    "fitness": ["fitness group", "fitness club", "fitness groups", "fitness clubs", "gym group", "workout group"],
    "travel": ["travel group", "travel club", "travel groups", "travel clubs", "adventure group"],
    "outdoor": ["outdoor group", "outdoor club", "outdoor groups", "outdoor clubs", "outdoor adventure"],
    "cycling": ["cycling group", "cycling club", "bike group", "bike club"],
    "yoga": ["yoga group", "yoga club", "yoga community"],
    "photography": ["photography group", "photography club", "photo group", "photo club"],
    "book": ["book club", "book group", "reading group", "reading club"],
    "women": ["women group", "women network", "women's group", "women's network", "women professionals"],
    "professional": ["professional group", "professional network", "professionals group"],
    "nonprofit": ["nonprofit group", "nonprofit network", "ngo group"],
    "tech": ["tech group", "tech network", "technology group", "developer group"],
    "wellness": ["wellness group", "wellness club", "wellness community"],
  };

  const normalized = keywords.map(k => k.toLowerCase().trim());
  const rootConcepts = new Set<string>();
  const dedupedRoots: string[] = [];

  for (const kw of normalized) {
    let foundRoot = false;
    for (const [root, variants] of Object.entries(rootMap)) {
      if (kw === root || kw.includes(root) || variants.some(v => kw === v || kw.includes(v))) {
        if (!rootConcepts.has(root)) {
          rootConcepts.add(root);
          dedupedRoots.push(root);
        }
        foundRoot = true;
        break;
      }
    }
    if (!foundRoot && !rootConcepts.has(kw)) {
      rootConcepts.add(kw);
      dedupedRoots.push(kw);
    }
  }

  return dedupedRoots;
}

function getContextualDiversifiers(rootConcept: string): string[] {
  const diversifiers: Record<string, string[]> = {
    "alumni": [
      "Harvard alumni", "Stanford alumni", "MIT alumni", "UCLA alumni", "NYU alumni",
      "Columbia alumni", "Michigan alumni", "USC alumni", "Georgetown alumni", "Duke alumni",
      "Penn alumni", "Yale alumni", "Princeton alumni", "Cornell alumni", "Northwestern alumni",
      "Berkeley alumni", "UVA alumni", "Emory alumni", "Vanderbilt alumni", "Texas alumni",
      "Ohio State alumni", "Florida alumni", "Georgia Tech alumni", "Boston University alumni",
    ],
    "church": [
      "Baptist church", "Methodist church", "Presbyterian church", "Catholic parish",
      "nondenominational church", "Pentecostal church", "Lutheran church",
      "church community", "faith community", "Bible study group",
    ],
    "hiking": [
      "Sierra Club", "trail runners", "mountain hiking", "national park hikers",
      "backpacking group", "day hikers", "Appalachian trail",
    ],
    "running": [
      "marathon training", "5K running", "trail running", "road runners",
      "half marathon group", "track club",
    ],
    "fitness": [
      "CrossFit community", "personal trainers", "bodybuilding", "strength training",
      "wellness coaching", "group fitness",
    ],
    "travel": [
      "group travel", "adventure travel", "backpackers", "travel bloggers",
      "solo travelers", "luxury travel", "travel photographers",
    ],
    "outdoor": [
      "outdoor adventure", "camping group", "kayaking club", "rock climbing",
      "wilderness exploration", "outdoor fitness",
    ],
    "women": [
      "women in tech", "women entrepreneurs", "executive women", "women in finance",
      "women's leadership", "professional women",
    ],
    "professional": [
      "business networking", "executive networking", "industry leaders",
      "young professionals", "entrepreneurs network",
    ],
    "tech": [
      "software engineers", "data science", "product managers", "tech founders",
      "AI professionals", "cybersecurity",
    ],
  };
  return diversifiers[rootConcept] || [];
}

function expandLinkedInKeywords(keywords: string[], geos: string[], maxQueries: number = 100): string[] {
  const synonymMap: Record<string, string[]> = {
    "hiking": ["hiking club", "trail group", "hikers", "outdoor hiking"],
    "running": ["run club", "runners club", "marathon group", "runners", "running club"],
    "cycling": ["cycling club", "bike group", "bike club", "cyclists"],
    "fitness": ["fitness club", "workout group", "gym community", "fitness professionals"],
    "yoga": ["yoga community", "yoga professionals", "yoga teachers", "yoga studio"],
    "travel": ["travel club", "adventure travel", "group travel", "travelers network"],
    "outdoor": ["outdoor recreation", "outdoor adventure", "nature group", "outdoors enthusiasts"],
    "photography": ["photographers network", "photo club", "photographers", "camera club"],
    "book": ["book club", "reading group", "book lovers", "readers", "literary group"],
    "alumni": ["alumni association", "graduates network", "university alumni", "college alumni", "alumni chapter"],
    "women": ["women leaders", "women's network", "women in business", "women entrepreneurs", "women professionals"],
    "professional": ["professional network", "industry professionals", "business professionals", "professionals community"],
    "coaching": ["executive coaching", "leadership coaching", "life coaching", "business coaching"],
    "wellness": ["wellness professionals", "holistic health", "wellness community", "health wellness"],
    "food": ["food lovers", "culinary group", "wine group", "foodies", "food enthusiasts"],
    "tech": ["technology professionals", "startup community", "tech network", "developers", "software engineers"],
    "church": ["faith community", "ministry group", "spiritual community", "church leaders", "Bible study"],
    "social club": ["social group", "networking group", "social networking", "social events"],
    "nonprofit": ["social impact", "community leadership", "NGO network", "nonprofit professionals"],
    "climbing": ["mountaineering", "outdoor climbing", "climbers", "rock climbing"],
    "surf": ["water sports group", "surfing community", "surfers"],
    "leadership": ["leaders network", "executive leadership", "leadership development"],
    "entrepreneur": ["startup founders", "business owners", "entrepreneurs network", "small business"],
    "marketing": ["marketers network", "digital marketing", "marketing professionals", "growth marketing"],
    "real estate": ["realtors network", "real estate professionals", "property investors"],
    "healthcare": ["medical professionals", "health workers", "healthcare network", "nurses group"],
    "education": ["teachers network", "educators", "education professionals", "school administrators"],
    "finance": ["financial professionals", "investors network", "finance community", "fintech"],
    "hr": ["HR professionals", "human resources", "HR network", "people operations", "talent acquisition"],
    "sustainability": ["green business", "climate action", "environmental", "ESG professionals"],
  };

  const TOP_US_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Diego", "Dallas", "Austin",
    "Denver", "Seattle", "Portland", "Nashville", "Atlanta",
    "Miami", "Boston", "San Francisco",
  ];

  const expanded: string[] = [];
  const seen = new Set<string>();
  const addQuery = (q: string) => {
    const normalized = q.toLowerCase().trim().replace(/\s+/g, " ");
    if (!seen.has(normalized) && expanded.length < maxQueries) {
      seen.add(normalized);
      expanded.push(q);
    }
  };

  const dedupedKeywords = deduplicateLinkedInKeywords(keywords);

  for (const kw of dedupedKeywords) {
    addQuery(`site:linkedin.com/groups/ "${kw}" "members"`);
    addQuery(`"linkedin.com/groups/" "${kw}" members`);
    addQuery(`site:linkedin.com/groups/ ${kw}`);

    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms.slice(0, 4)) {
      addQuery(`site:linkedin.com/groups/ "${syn}" "members"`);
    }

    addQuery(`site:linkedin.com/groups/ ${kw} group`);
    addQuery(`site:linkedin.com/groups/ ${kw} community`);
    addQuery(`site:linkedin.com/groups/ ${kw} network`);
    addQuery(`site:linkedin.com/groups/ ${kw} club`);
    addQuery(`site:linkedin.com/groups/ ${kw} association`);

    const diversifiers = getContextualDiversifiers(kwLower);
    for (const div of diversifiers.slice(0, 12)) {
      addQuery(`site:linkedin.com/groups/ "${div}"`);
    }
  }

  for (const kw of dedupedKeywords) {
    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms) {
      addQuery(`site:linkedin.com/groups/ "${syn}"`);
    }
  }

  const allSearchTerms = [...dedupedKeywords];
  for (const kw of dedupedKeywords) {
    const kwLower = kw.toLowerCase().trim();
    const synonyms = synonymMap[kwLower] || [];
    for (const syn of synonyms.slice(0, 3)) allSearchTerms.push(syn);
  }

  const geoCities = geos.length > 0 ? geos.slice(0, 10) : TOP_US_CITIES.slice(0, 10);
  for (const term of allSearchTerms.slice(0, 4)) {
    if (expanded.length >= maxQueries) break;
    for (const geo of geoCities) {
      addQuery(`site:linkedin.com/groups/ "${term}" "${geo}"`);
    }
  }

  return expanded;
}

function parseLinkedInMembersFromSnippet(text: string): number {
  const patterns = [
    /(\d[\d,]*)\s*members?/i,
    /(\d[\d,]*)\s*professionals?/i,
    /(\d[\d,]*)\s*people/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return parseInt(match[1].replace(/,/g, ""), 10) || 0;
    }
  }
  return 0;
}

function extractLinkedInGroupId(url: string): string {
  const match = url.match(/linkedin\.com\/groups\/([^/?&#]+)/);
  if (!match) return "";
  const slug = match[1].replace(/\/$/, "").toLowerCase();
  const blockedSlugs = new Set([
    "discover", "you-may-like", "mygroups", "home", "groups-directory",
    "your-profile", "search", "settings", "create", "requests",
    "suggested", "browse", "feed", "manage", "invitations",
  ]);
  if (!slug || blockedSlugs.has(slug)) return "";
  return slug;
}

function cleanLinkedInGroupName(title: string): string {
  return title
    .replace(/\s*\|\s*LinkedIn$/i, "")
    .replace(/\s*[-–—]\s*LinkedIn$/i, "")
    .replace(/\s*LinkedIn\s*$/i, "")
    .replace(/\s*\|\s*Groups$/i, "")
    .replace(/\s*[-–—]\s*Groups$/i, "")
    .trim();
}

async function scrapeLinkedInGroupsViaActor(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number; geos?: string[] } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenGroupIds = new Set<string>();
  const minMembers = filters.minMemberCount || 0;
  const maxMembers = filters.maxMemberCount || 0;
  const dedupedKeywords = deduplicateLinkedInKeywords(keywords);

  const allSearchTerms: string[] = [];
  for (const kw of dedupedKeywords) {
    allSearchTerms.push(kw);
    const diversifiers = getContextualDiversifiers(kw.toLowerCase().trim());
    for (const div of diversifiers.slice(0, 6)) {
      allSearchTerms.push(div);
    }
  }

  const seenTerms = new Set<string>();
  const uniqueTerms: string[] = [];
  for (const t of allSearchTerms) {
    const norm = t.toLowerCase().trim();
    if (!seenTerms.has(norm)) {
      seenTerms.add(norm);
      uniqueTerms.push(t);
    }
  }
  await appendAndSave(`LinkedIn Groups: searching ${uniqueTerms.length} keywords via LinkedIn actor (deduped roots: ${dedupedKeywords.join(", ")}, target: ${maxItems} groups)`);

  const itemsPerKeyword = Math.max(10, Math.ceil((maxItems * 1.5) / uniqueTerms.length));

  for (const keyword of uniqueTerms) {
    if (leads.length >= maxItems) break;

    try {
      await appendAndSave(`LinkedIn Groups: searching "${keyword}" (${leads.length}/${maxItems} found)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults(
        "contactminerlabs/linkedin-groups-search-scraper---cheap",
        {
          KEYWORD: keyword,
          TIME_FILTER: "any",
          MAX_ITEMS: itemsPerKeyword,
        },
        120000,
      );
      await storage.incrementApifySpend(runId, actorCost);

      let newInBatch = 0;
      let filteredOut = 0;
      for (const item of items) {
        if (leads.length >= maxItems) break;

        const rawGroupId = String(item.groupId || "");
        const rawGroupUrl = item.groupUrl || "";
        const groupId = rawGroupId || extractLinkedInGroupId(rawGroupUrl) || "";
        if (!groupId) continue;

        const effectiveId = groupId;
        if (seenGroupIds.has(effectiveId)) continue;
        seenGroupIds.add(effectiveId);

        const groupName = item.groupName || `LinkedIn Group ${effectiveId}`;
        const memberCount = typeof item.membersCount === "number" ? item.membersCount : (parseInt(String(item.membersCount || "0").replace(/[^0-9]/g, ""), 10) || 0);

        if (minMembers > 0 && memberCount > 0 && memberCount < minMembers) { filteredOut++; continue; }
        if (maxMembers > 0 && memberCount > 0 && memberCount > maxMembers) { filteredOut++; continue; }

        const description = item.description || item.snippet || "";
        const fullText = `${groupName} ${description}`;
        const canonicalUrl = `https://www.linkedin.com/groups/${effectiveId}/`;
        const channels: Record<string, string> = { linkedin_group: canonicalUrl };

        const descUrls = extractUrlsFromText(description);
        const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
        Object.assign(channels, socialFromDesc);

        const descEmails = extractEmailsFromText(description);

        let leaderName = "";
        const leaderPatterns = [
          /(?:admin|administrator|owner|manager|founder|created by|managed by|run by|led by|organized by)\s*[:\-–]?\s*([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)/i,
          /([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))/i,
        ];
        for (const pattern of leaderPatterns) {
          const leaderMatch = fullText.match(pattern);
          if (leaderMatch && leaderMatch[1] && leaderMatch[1].length > 4) {
            leaderName = leaderMatch[1].trim();
            break;
          }
        }

        leads.push({
          source: "linkedin",
          communityName: groupName,
          communityType: item.category ? (item.category.toLowerCase().includes("travel") ? "travel" : detectCommunityType(fullText)) : detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location: "",
          website: channels.website || canonicalUrl,
          email: descEmails[0] || "",
          phone: "",
          leaderName,
          memberCount,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: detectMonetization(description),
          engagementSignals: {
            member_count: memberCount,
            attendance_proxy: memberCount,
            activity_level: item.activity || "",
            ...detectEngagement(description),
          },
          tripFitSignals: detectTripFit(fullText),
          raw: item,
        });
        newInBatch++;
      }

      await appendAndSave(`LinkedIn Groups: "${keyword}" → ${items.length} results, ${newInBatch} new unique groups${filteredOut > 0 ? ` (${filteredOut} filtered)` : ""}`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      const errorMsg = err.message || String(err);
      if (errorMsg.includes("actor-is-not-rented") || errorMsg.includes("not found") || errorMsg.includes("404")) {
        await appendAndSave(`[WARN] LinkedIn Groups actor not available: ${errorMsg.substring(0, 200)}. Falling back to Google search...`);
        return scrapeLinkedInGroupsViaGoogle(runId, keywords, maxItems - leads.length, appendAndSave, filters, leads, seenGroupIds);
      }
      await appendAndSave(`[WARN] LinkedIn Groups search failed for "${keyword}": ${errorMsg.substring(0, 200)}`);
    }
  }

  if (leads.length === 0 && uniqueTerms.length > 0) {
    await appendAndSave(`LinkedIn Groups: actor returned 0 groups across ${uniqueTerms.length} keywords. Falling back to Google search...`);
    return scrapeLinkedInGroupsViaGoogle(runId, keywords, maxItems, appendAndSave, filters, leads, seenGroupIds);
  }

  if (leads.length < Math.min(maxItems, 3) && uniqueTerms.length >= 3) {
    await appendAndSave(`LinkedIn Groups: actor found only ${leads.length} groups (low yield). Supplementing with Google search...`);
    return scrapeLinkedInGroupsViaGoogle(runId, keywords, maxItems - leads.length, appendAndSave, filters, leads, seenGroupIds);
  }

  await appendAndSave(`LinkedIn Groups discovery complete: ${leads.length} groups from ${uniqueTerms.length} keyword searches`);
  return leads;
}

async function scrapeLinkedInGroupsViaGoogle(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number; geos?: string[] } = {},
  existingLeads: PlatformLead[] = [],
  existingIds: Set<string> = new Set(),
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [...existingLeads];
  const seenGroupIds = new Set<string>(existingIds);
  const minMembers = filters.minMemberCount || 0;
  const maxMembers = filters.maxMemberCount || 0;

  const googleQueries = expandLinkedInKeywords(keywords, filters.geos || []);
  await appendAndSave(`LinkedIn Groups (Google fallback): ${googleQueries.length} queries, target: ${maxItems + existingLeads.length} groups`);
  const batchSize = 10;
  let consecutiveZeroBatches = 0;

  for (let i = 0; i < googleQueries.length; i += batchSize) {
    if (leads.length >= maxItems + existingLeads.length) break;

    if (consecutiveZeroBatches >= 3) {
      await appendAndSave(`LinkedIn Groups (Google): stopping early — ${consecutiveZeroBatches} zero-yield batches. Found ${leads.length} groups.`);
      break;
    }

    const batch = googleQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`LinkedIn Groups (Google): batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(googleQueries.length / batchSize)} (${leads.length} found)...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 3,
        resultsPerPage: 50,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      let filteredOut = 0;
      let totalOrganic = 0;
      let linkedinUrlCount = 0;
      for (const item of items) {
        if (leads.length >= maxItems + existingLeads.length) break;

        const organicResults = item.organicResults || [];
        totalOrganic += organicResults.length;
        for (const result of organicResults) {
          if (leads.length >= maxItems + existingLeads.length) break;

          const rawUrl = result.url || result.link || "";
          if (!rawUrl.includes("linkedin.com/groups/")) continue;
          if (/linkedin\.com\/groups\/(discover|you-may-like|mygroups|home|groups-directory|search|settings|create|requests|suggested|browse|feed|manage|invitations)\b/i.test(rawUrl)) continue;
          if (rawUrl.includes("/posts/") || rawUrl.includes("/about/rules") || rawUrl.includes("/members/")) continue;
          linkedinUrlCount++;

          const groupId = extractLinkedInGroupId(rawUrl);
          if (!groupId) continue;
          if (seenGroupIds.has(groupId)) continue;
          seenGroupIds.add(groupId);

          const title = result.title || "";
          const snippet = result.description || result.snippet || "";
          const fullText = `${title} ${snippet}`;
          const groupName = cleanLinkedInGroupName(title) || `LinkedIn Group ${groupId}`;
          const memberCount = parseLinkedInMembersFromSnippet(fullText);

          if (minMembers > 0 && memberCount > 0 && memberCount < minMembers) { filteredOut++; continue; }
          if (maxMembers > 0 && memberCount > 0 && memberCount > maxMembers) { filteredOut++; continue; }

          const groupUrl = `https://www.linkedin.com/groups/${groupId}/`;
          const channels: Record<string, string> = { linkedin_group: groupUrl };

          const descUrls = extractUrlsFromText(snippet);
          const socialFromDesc = extractSocialChannelsFromUrls(descUrls);
          Object.assign(channels, socialFromDesc);

          const descEmails = extractEmailsFromText(snippet);

          const sitelinks = result.sitelinks || result.siteLinks || [];
          for (const sl of (Array.isArray(sitelinks) ? sitelinks : [])) {
            const slUrl = sl?.url || sl?.link || "";
            const slTitle = sl?.title || "";
            const slSnippet = sl?.snippet || sl?.description || "";
            if (slUrl && slUrl.includes("linkedin.com/in/")) {
              if (!channels.linkedin) channels.linkedin = slUrl.split("?")[0];
              const slugMatch = slUrl.match(/linkedin\.com\/in\/([^/?&#]+)/);
              if (slugMatch) {
                const nameFromSlug = slugMatch[1].replace(/-/g, " ").replace(/\d+$/g, "").trim();
                if (nameFromSlug.split(" ").length >= 2) {
                  const parts = nameFromSlug.split(" ");
                  const formatted = parts.map((p: string) => p.charAt(0).toUpperCase() + p.slice(1)).join(" ");
                  if (!channels._sitelinkLeaderName) channels._sitelinkLeaderName = formatted;
                }
              }
            }
            const slEmails = extractEmailsFromText(`${slTitle} ${slSnippet}`);
            if (slEmails.length > 0 && !descEmails.includes(slEmails[0])) {
              descEmails.push(slEmails[0]);
            }
          }

          let leaderName = "";
          const leaderPatterns = [
            /(?:admin|administrator|owner|manager|founder|created by|managed by|run by|led by|organized by)\s*[:\-–]?\s*([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)/i,
            /([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))/i,
          ];
          for (const pattern of leaderPatterns) {
            const leaderMatch = fullText.match(pattern);
            if (leaderMatch && leaderMatch[1] && leaderMatch[1].length > 4) {
              leaderName = leaderMatch[1].trim();
              break;
            }
          }
          if (!leaderName && channels._sitelinkLeaderName) {
            leaderName = channels._sitelinkLeaderName;
          }
          delete channels._sitelinkLeaderName;

          leads.push({
            source: "linkedin",
            communityName: groupName,
            communityType: detectCommunityType(fullText),
            description: snippet.substring(0, 2000),
            location: "",
            website: channels.website || groupUrl,
            email: descEmails[0] || "",
            phone: "",
            leaderName,
            memberCount,
            subscriberCount: 0,
            ownedChannels: channels,
            monetizationSignals: detectMonetization(snippet),
            engagementSignals: {
              member_count: memberCount,
              attendance_proxy: memberCount,
              ...detectEngagement(snippet),
            },
            tripFitSignals: detectTripFit(fullText),
            raw: result,
          });
        }
      }

      if (linkedinUrlCount === 0) {
        consecutiveZeroBatches++;
      } else {
        consecutiveZeroBatches = 0;
      }

      await appendAndSave(`LinkedIn Groups (Google): ${totalOrganic} results, ${linkedinUrlCount} LinkedIn URLs, ${leads.length} groups${filteredOut > 0 ? ` (${filteredOut} filtered)` : ""}`);
    } catch (err: any) {
      if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
      await appendAndSave(`[WARN] LinkedIn Groups Google batch failed: ${err.message}`);
    }
  }

  await appendAndSave(`LinkedIn Groups (Google fallback) complete: ${leads.length} groups`);
  return leads;
}

async function scrapeLinkedInGroups(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number; geos?: string[] } = {},
): Promise<PlatformLead[]> {
  return scrapeLinkedInGroupsViaActor(runId, keywords, maxItems, appendAndSave, filters);
}

async function enrichLinkedInGroupPages(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const linkedInLeads = leads.filter(l => l.source === "linkedin" && !l.leaderName && l.ownedChannels?.linkedin_group);
  if (linkedInLeads.length === 0) {
    await appendAndSave("LinkedIn group page scrape: all leads already have leader names or no group URLs");
    return;
  }

  await appendAndSave(`LinkedIn group page scrape: scraping ${linkedInLeads.length} group pages for admin/about info...`);

  const urls = linkedInLeads.map(l => l.ownedChannels!.linkedin_group!);

  try {
    const { items, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~cheerio-scraper", {
      startUrls: urls.map(u => ({ url: u })),
      maxRequestsPerCrawl: linkedInLeads.length,
      maxConcurrency: 5,
      pageFunction: `async function pageFunction(context) {
        const { $, request } = context;
        const title = $('title').text() || '';
        const description = $('meta[name="description"]').attr('content') || '';
        const ogDescription = $('meta[property="og:description"]').attr('content') || '';
        const ogTitle = $('meta[property="og:title"]').attr('content') || '';
        const twitterTitle = $('meta[name="twitter:title"]').attr('content') || '';
        const twitterDesc = $('meta[name="twitter:description"]').attr('content') || '';
        const canonical = $('link[rel="canonical"]').attr('href') || '';
        const jsonLd = $('script[type="application/ld+json"]').text() || '';
        const profileLinks = [];
        $('a[href*="linkedin.com/in/"]').each(function() {
          profileLinks.push($(this).attr('href') || '');
        });
        const bodySnippet = $('body').text().substring(0, 8000) || '';
        const isAuthWall = bodySnippet.includes('Sign in') && bodySnippet.includes('Join now') && bodySnippet.length < 2000;
        return {
          url: request.url,
          title,
          description,
          ogDescription,
          ogTitle,
          twitterTitle,
          twitterDesc,
          canonical,
          jsonLd: jsonLd.substring(0, 3000),
          profileLinks: profileLinks.slice(0, 10),
          bodySnippet,
          isAuthWall,
        };
      }`,
    }, 120000);
    await storage.incrementApifySpend(runId, actorCost);

    let namesFound = 0;
    let emailsFound = 0;
    let authWallCount = 0;
    for (const item of items) {
      const pageUrl = item.url || "";
      const lead = linkedInLeads.find(l => l.ownedChannels?.linkedin_group && pageUrl.includes(extractLinkedInGroupId(l.ownedChannels.linkedin_group)));
      if (!lead) continue;

      if (item.isAuthWall) {
        authWallCount++;
      }

      const allText = [item.title, item.description, item.ogDescription, item.ogTitle, item.twitterTitle, item.twitterDesc, item.bodySnippet].filter(Boolean).join(" ");

      if (item.ogTitle && !lead.communityName.includes(item.ogTitle.replace(/\s*\|\s*LinkedIn$/, "").trim())) {
        const betterName = (item.ogTitle || "").replace(/\s*\|\s*LinkedIn$/, "").trim();
        if (betterName.length > lead.communityName.length && betterName.length < 200) {
          lead.communityName = betterName;
        }
      }

      const profileLinks = (item.profileLinks || []) as string[];
      if (!lead.leaderName && profileLinks.length > 0) {
        for (const pLink of profileLinks) {
          const slugMatch = pLink.match(/linkedin\.com\/in\/([^/?&#]+)/);
          if (slugMatch) {
            const slug = slugMatch[1].replace(/-/g, " ").replace(/\d+$/g, "").trim();
            const parts = slug.split(/\s+/).filter((p: string) => p.length > 1);
            if (parts.length >= 2 && parts.length <= 4) {
              lead.leaderName = parts.map((p: string) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(" ");
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.linkedin = pLink.split("?")[0];
              namesFound++;
              break;
            }
          }
        }
      }

      if (!lead.leaderName) {
        const namePatterns = [
          /(?:admin|administrator|owner|manager|founder|created by|managed by|run by|led by|organized by)\s*[:\-–]?\s*([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)/i,
          /([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))/i,
          /(?:about|group|community)\s+(?:by|from)\s+([A-Z][a-z]+ [A-Z][a-z]+)/i,
        ];
        for (const pattern of namePatterns) {
          const nameMatch = allText.match(pattern);
          if (nameMatch && nameMatch[1] && nameMatch[1].length > 4) {
            lead.leaderName = nameMatch[1].trim();
            namesFound++;
            break;
          }
        }
      }

      if (item.jsonLd) {
        try {
          const ld = JSON.parse(item.jsonLd);
          if (ld && !lead.leaderName) {
            const author = ld.author?.name || ld.creator?.name || ld.founder?.name || "";
            if (author && author.split(" ").length >= 2 && author.length > 4) {
              lead.leaderName = author;
              namesFound++;
            }
          }
        } catch {}
      }

      if (!lead.email) {
        const emails = extractEmailsFromText(allText);
        if (emails.length > 0) {
          lead.email = emails[0];
          emailsFound++;
        }
      }

      if (!lead.ownedChannels?.website) {
        const websiteUrls = extractUrlsFromText(allText);
        for (const wu of websiteUrls) {
          try {
            const host = new URL(wu).hostname.replace(/^www\./, "");
            if (!["linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com", "google.com"].some(s => host.includes(s))) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.website = wu.split("?")[0];
              break;
            }
          } catch {}
        }
      }

      const desc = (item.description || item.ogDescription || item.twitterDesc || "").substring(0, 2000);
      if (desc.length > lead.description.length) {
        lead.description = desc;
      }
    }

    await appendAndSave(`LinkedIn group page scrape: found ${namesFound} leader names, ${emailsFound} emails from ${items.length} pages${authWallCount > 0 ? ` (${authWallCount} pages blocked by auth wall)` : ""}`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] LinkedIn group page scrape failed: ${err.message}`);
  }
}

const GOOGLE_ENRICHMENT_MAX = Infinity;
const GOOGLE_ENRICHMENT_SOCIAL_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com", "yelp.com", "tripadvisor.com", "bbb.org", "mn.co", "mightynetworks.com"];

async function googleSearchEnrichCreators(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsToSearch = leads
    .filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin)
    .slice(0, GOOGLE_ENRICHMENT_MAX);

  if (leadsToSearch.length === 0) {
    await appendAndSave("Google enrichment: all leads already have website/LinkedIn or email");
    return;
  }

  await appendAndSave(`Google enrichment: searching for ${leadsToSearch.length} creators without website/LinkedIn...`);

  const queries: { term: string; leadIdx: number }[] = [];
  for (const l of leadsToSearch) {
    const brandName = l.communityName || "";
    const realName = l.leaderName || "";
    const idx = leads.indexOf(l);
    const hasRealName = realName && realName !== brandName && isValidApolloCandidate(realName);

    const communityType = l.communityType || "";
    const nicheQualifier = communityType && communityType !== "unknown"
      ? communityType.replace(/_/g, " ")
      : "";

    if (hasRealName) {
      queries.push({ term: `"${realName}" email website`, leadIdx: idx });
      queries.push({ term: `site:linkedin.com "${realName}"`, leadIdx: idx });
      queries.push({ term: `"${realName}" "reach me" OR "contact me" OR "get in touch"`, leadIdx: idx });
      if (nicheQualifier) {
        queries.push({ term: `"${realName}" ${nicheQualifier} email`, leadIdx: idx });
      }
    } else if (isValidApolloCandidate(brandName)) {
      queries.push({ term: `"${brandName}" contact website email`, leadIdx: idx });
      queries.push({ term: `site:linkedin.com "${brandName}"`, leadIdx: idx });
      queries.push({ term: `"${brandName}" "contact us" OR "about us" OR "reach out"`, leadIdx: idx });
    } else {
      const patreonUrl = l.website || l.ownedChannels?.patreon || "";
      if (patreonUrl.includes("patreon.com/")) {
        const slug = patreonUrl.split("patreon.com/")[1]?.split(/[?#/]/)[0];
        if (slug && slug.length >= 3) {
          queries.push({ term: `"${slug}" email contact website`, leadIdx: idx });
          queries.push({ term: `"${slug}" "at" "dot" com`, leadIdx: idx });
        }
      } else if (brandName.length >= 3) {
        queries.push({ term: `"${brandName}" contact email`, leadIdx: idx });
      }
    }
  }

  const batchSize = 20;
  const concurrentSearchBatches = 5;
  const enrichedLeadIndices = new Set<number>();
  const CIRCUIT_BREAKER_THRESHOLD = 5;
  let consecutiveSearchFailures = 0;
  let searchCircuitBroken = false;

  const completedSearchBatchNums = await loadIntraStepProgress(runId, "google_contact_search");
  const allSearchBatches: { queries: { term: string; leadIdx: number }[]; batchNum: number }[] = [];
  const totalBatches = Math.ceil(queries.length / batchSize);
  for (let i = 0; i < queries.length; i += batchSize) {
    allSearchBatches.push({
      queries: queries.slice(i, i + batchSize),
      batchNum: Math.floor(i / batchSize) + 1,
    });
  }

  if (completedSearchBatchNums.size > 0) {
    await appendAndSave(`Google enrichment: resuming — ${completedSearchBatchNums.size}/${totalBatches} batches already completed, skipping`);
  }

  async function processSearchBatch(batchInfo: { queries: { term: string; leadIdx: number }[]; batchNum: number }): Promise<void> {
    if (searchCircuitBroken) return;
    if (completedSearchBatchNums.has(batchInfo.batchNum)) return;
    const batch = batchInfo.queries;
    await appendAndSave(`Google enrichment: batch ${batchInfo.batchNum}/${totalBatches} (${batch.length} searches)...`);

    try {
      const searchQueries = batch.map(q => ({ term: q.term, countryCode: "us", languageCode: "en", maxPagesPerQuery: 1, resultsPerPage: 5 }));

      const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~google-search-scraper", {
        queries: searchQueries.map(q => q.term).join("\n"),
        maxPagesPerQuery: 1,
        resultsPerPage: 5,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 180000);
      await storage.incrementApifySpend(runId, actorCost);

      const resultsByQuery = new Map<number, any[]>();
      for (const r of results) {
        const searchQuery = r.searchQuery?.term || r.searchQuery || "";
        const matchIdx = batch.findIndex(q => q.term === searchQuery);
        const organic = r.organicResults || [];
        if (matchIdx >= 0) {
          const existing = resultsByQuery.get(matchIdx) || [];
          existing.push(...organic);
          resultsByQuery.set(matchIdx, existing);
        } else if (organic.length > 0) {
          const fallbackIdx = batch.findIndex(q => searchQuery.includes(q.term.substring(0, 20)));
          if (fallbackIdx >= 0) {
            const existing = resultsByQuery.get(fallbackIdx) || [];
            existing.push(...organic);
            resultsByQuery.set(fallbackIdx, existing);
          }
        }
      }

      consecutiveSearchFailures = 0;
      const totalOrganic = Array.from(resultsByQuery.values()).reduce((sum, arr) => sum + arr.length, 0);

      for (let j = 0; j < batch.length; j++) {
        const lead = leads[batch[j].leadIdx];
        const searchResults = resultsByQuery.get(j) || [];

        let foundAnything = false;

        for (const result of searchResults) {
          const url = result.url || result.link || "";
          const title = result.title || "";
          const description = result.description || result.snippet || "";
          const fullResult = `${url} ${title} ${description}`;

          if (!url) continue;

          try {
            const host = new URL(url).hostname.replace(/^www\./, "");

            if (host.includes("linkedin.com") && url.includes("/in/") && !lead.ownedChannels?.linkedin) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.linkedin = url.split("?")[0];
              foundAnything = true;
            }

            if (!GOOGLE_ENRICHMENT_SOCIAL_HOSTS.some(s => host.includes(s)) && !lead.ownedChannels?.website) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.website = url.split("?")[0];
              foundAnything = true;
            }

            if (host.includes("instagram.com") && !lead.ownedChannels?.instagram) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.instagram = url.split("?")[0];
              foundAnything = true;
            }

            if ((host.includes("twitter.com") || host === "x.com") && !lead.ownedChannels?.twitter) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.twitter = url.split("?")[0];
              foundAnything = true;
            }

            if (host.includes("facebook.com") && !lead.ownedChannels?.facebook) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.facebook = url.split("?")[0];
              foundAnything = true;
            }
          } catch {}

          if (!lead.email) {
            const emails = extractEmailsFromText(fullResult);
            if (emails.length > 0) {
              lead.email = emails[0];
              foundAnything = true;
            } else {
              const obfuscated = extractObfuscatedEmails(fullResult);
              if (obfuscated.length > 0) {
                lead.email = obfuscated[0];
                foundAnything = true;
              }
            }
          }
        }

        if (foundAnything) enrichedLeadIndices.add(batch[j].leadIdx);
      }

      completedSearchBatchNums.add(batchInfo.batchNum);
      await saveIntraStepProgress(runId, "google_contact_search", Array.from(completedSearchBatchNums));
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      consecutiveSearchFailures++;
      await appendAndSave(`[WARN] Google enrichment batch ${batchInfo.batchNum} failed: ${err.message}`);
      if (consecutiveSearchFailures >= CIRCUIT_BREAKER_THRESHOLD) {
        searchCircuitBroken = true;
        await appendAndSave(`[WARN] Google enrichment circuit breaker tripped after ${consecutiveSearchFailures} consecutive failures — skipping remaining batches`);
      }
    }
  }

  for (let i = 0; i < allSearchBatches.length; i += concurrentSearchBatches) {
    if (searchCircuitBroken) break;
    const concurrentSlice = allSearchBatches.slice(i, i + concurrentSearchBatches);
    await Promise.allSettled(concurrentSlice.map((b, idx) =>
      new Promise<void>(resolve => setTimeout(() => processSearchBatch(b).then(resolve).catch(resolve), idx * 1000))
    ));
  }

  await appendAndSave(`Google enrichment: found new data for ${enrichedLeadIndices.size}/${leadsToSearch.length} creators`);
}

async function googleBridgeEnrichFacebookGroups(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const groupLeads = leads.filter(l => (l.source === "facebook" || l.source === "meetup" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
  if (groupLeads.length === 0) {
    await appendAndSave("Google Bridge: all Facebook/Meetup/LinkedIn leads already have website/LinkedIn or email");
    return;
  }

  await appendAndSave(`Google Bridge: searching for leaders/orgs behind ${groupLeads.length} Facebook/Meetup/LinkedIn groups...`);

  const queries: { term: string; leadIdx: number }[] = [];
  for (const l of groupLeads) {
    const groupName = l.communityName || "";
    if (!groupName || groupName.length < 3) continue;
    const idx = leads.indexOf(l);

    queries.push({ term: `"${groupName}" website contact email`, leadIdx: idx });

    if (l.leaderName && l.leaderName.length > 2) {
      queries.push({ term: `"${l.leaderName}" "${groupName}" linkedin OR email`, leadIdx: idx });
    } else {
      queries.push({ term: `"${groupName}" organizer OR founder OR leader linkedin`, leadIdx: idx });
    }

    if (l.source === "linkedin") {
      queries.push({ term: `"${groupName}" admin OR owner OR manager linkedin`, leadIdx: idx });
      queries.push({ term: `"${groupName}" president OR director OR chairman contact`, leadIdx: idx });
      queries.push({ term: `site:linkedin.com/in/ "${groupName}"`, leadIdx: idx });
      if (!l.leaderName) {
        const shortName = groupName.replace(/\s*(linkedin|group|network|association|community|club)\s*/gi, " ").trim();
        if (shortName.length > 3 && shortName !== groupName) {
          queries.push({ term: `"${shortName}" founder OR president OR executive director email`, leadIdx: idx });
        }
      }
    }
  }

  const batchSize = 20;
  const concurrentBridgeBatches = 5;
  const enrichedLeadIndices = new Set<number>();
  const CIRCUIT_BREAKER_THRESHOLD = 5;
  let consecutiveFailures = 0;
  let circuitBroken = false;

  const completedBatchNums = await loadIntraStepProgress(runId, "google_bridge");
  const allBridgeBatches: { queries: typeof queries; batchNum: number }[] = [];
  const totalBatches = Math.ceil(queries.length / batchSize);
  for (let i = 0; i < queries.length; i += batchSize) {
    allBridgeBatches.push({
      queries: queries.slice(i, i + batchSize),
      batchNum: Math.floor(i / batchSize) + 1,
    });
  }

  if (completedBatchNums.size > 0) {
    await appendAndSave(`Google Bridge: resuming — ${completedBatchNums.size}/${totalBatches} batches already completed, skipping`);
  }

  async function processBridgeBatch(batchInfo: { queries: typeof queries; batchNum: number }): Promise<void> {
    if (circuitBroken) return;
    if (completedBatchNums.has(batchInfo.batchNum)) return;
    const batch = batchInfo.queries;
    await appendAndSave(`Google Bridge: batch ${batchInfo.batchNum}/${totalBatches} (${batch.length} searches)...`);

    try {
      const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~google-search-scraper", {
        queries: batch.map(q => q.term).join("\n"),
        maxPagesPerQuery: 1,
        resultsPerPage: 5,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 180000);
      await storage.incrementApifySpend(runId, actorCost);

      const resultsByQuery = new Map<number, any[]>();
      for (const r of results) {
        const searchQuery = r.searchQuery?.term || r.searchQuery || "";
        const matchIdx = batch.findIndex(q => q.term === searchQuery);
        const organic = r.organicResults || [];
        if (matchIdx >= 0) {
          const existing = resultsByQuery.get(matchIdx) || [];
          existing.push(...organic);
          resultsByQuery.set(matchIdx, existing);
        } else if (organic.length > 0) {
          const fallbackIdx = batch.findIndex(q => searchQuery.includes(q.term.substring(0, 20)));
          if (fallbackIdx >= 0) {
            const existing = resultsByQuery.get(fallbackIdx) || [];
            existing.push(...organic);
            resultsByQuery.set(fallbackIdx, existing);
          }
        }
      }

      consecutiveFailures = 0;
      const totalOrganic = Array.from(resultsByQuery.values()).reduce((sum, arr) => sum + arr.length, 0);
      await appendAndSave(`Google Bridge: batch ${batchInfo.batchNum} got ${results.length} query results, ${totalOrganic} organic results`);

      for (let j = 0; j < batch.length; j++) {
        const lead = leads[batch[j].leadIdx];
        const searchResults = resultsByQuery.get(j) || [];

        let foundAnything = false;

        for (const result of searchResults) {
          const url = result.url || result.link || "";
          const title = result.title || "";
          const description = result.description || result.snippet || "";
          const fullResult = `${url} ${title} ${description}`;

          if (!url) continue;

          try {
            const host = new URL(url).hostname.replace(/^www\./, "");

            if (host.includes("linkedin.com") && url.includes("/in/") && !lead.ownedChannels?.linkedin) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.linkedin = url.split("?")[0];
              foundAnything = true;
            }

            if (host.includes("instagram.com") && !lead.ownedChannels?.instagram) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.instagram = url.split("?")[0];
              foundAnything = true;
            }

            if ((host.includes("twitter.com") || host === "x.com") && !lead.ownedChannels?.twitter) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.twitter = url.split("?")[0];
              foundAnything = true;
            }

            if (host.includes("youtube.com") && !lead.ownedChannels?.youtube) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.youtube = url.split("?")[0];
              foundAnything = true;
            }

            if (!GOOGLE_ENRICHMENT_SOCIAL_HOSTS.some(s => host.includes(s)) && !lead.ownedChannels?.website) {
              if (!lead.ownedChannels) lead.ownedChannels = {};
              lead.ownedChannels.website = url.split("?")[0];
              foundAnything = true;
            }

            if (!lead.leaderName) {
              const nameMatch = title.match(/^([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*[-|–·,]/);
              if (nameMatch && nameMatch[1].length > 4) {
                lead.leaderName = nameMatch[1].trim();
                foundAnything = true;
              }

              if (!lead.leaderName && host.includes("linkedin.com") && url.includes("/in/")) {
                const slugMatch = url.match(/\/in\/([^/?&#]+)/);
                if (slugMatch) {
                  const slug = slugMatch[1].replace(/-/g, " ").replace(/\d+/g, "").trim();
                  const parts = slug.split(/\s+/).filter((p: string) => p.length > 1);
                  if (parts.length >= 2 && parts.length <= 4) {
                    const name = parts.map((p: string) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(" ");
                    if (name.length > 4) {
                      lead.leaderName = name;
                      foundAnything = true;
                    }
                  }
                }
              }

              if (!lead.leaderName) {
                const contextPatterns = [
                  /(?:admin|administrator|owner|manager|founder|created by|managed by|run by|led by|organized by)\s*[:\-–]?\s*([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)/i,
                  /([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(?:is the|,\s*(?:admin|founder|owner|manager|organizer|leader))/i,
                ];
                const searchText = `${title} ${description}`;
                for (const pattern of contextPatterns) {
                  const ctxMatch = searchText.match(pattern);
                  if (ctxMatch && ctxMatch[1] && ctxMatch[1].length > 4) {
                    lead.leaderName = ctxMatch[1].trim();
                    foundAnything = true;
                    break;
                  }
                }
              }
            }
          } catch {}

          if (!lead.email) {
            const emails = extractEmailsFromText(fullResult);
            if (emails.length > 0) {
              lead.email = emails[0];
              foundAnything = true;
            } else {
              const obfuscated = extractObfuscatedEmails(fullResult);
              if (obfuscated.length > 0) {
                lead.email = obfuscated[0];
                foundAnything = true;
              }
            }
          }
        }

        if (foundAnything) enrichedLeadIndices.add(batch[j].leadIdx);
      }

      completedBatchNums.add(batchInfo.batchNum);
      await saveIntraStepProgress(runId, "google_bridge", Array.from(completedBatchNums));
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      consecutiveFailures++;
      await appendAndSave(`[WARN] Google Bridge batch ${batchInfo.batchNum} failed: ${err.message}`);
      if (consecutiveFailures >= CIRCUIT_BREAKER_THRESHOLD) {
        circuitBroken = true;
        await appendAndSave(`[WARN] Google Bridge circuit breaker tripped after ${consecutiveFailures} consecutive failures — skipping remaining batches`);
      }
    }
  }

  for (let i = 0; i < allBridgeBatches.length; i += concurrentBridgeBatches) {
    if (circuitBroken) break;
    const concurrentSlice = allBridgeBatches.slice(i, i + concurrentBridgeBatches);
    await Promise.allSettled(concurrentSlice.map((b, idx) =>
      new Promise<void>(resolve => setTimeout(() => processBridgeBatch(b).then(resolve).catch(resolve), idx * 1000))
    ));
  }

  await appendAndSave(`Google Bridge: found new data for ${enrichedLeadIndices.size}/${groupLeads.length} Facebook/Meetup groups`);
}

const LINK_AGGREGATOR_MAX = Infinity;

async function enrichFromLinkAggregators(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsWithAggregator = leads.filter(l =>
    !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http")
  ).slice(0, LINK_AGGREGATOR_MAX);

  if (leadsWithAggregator.length === 0) return;

  await appendAndSave(`Link aggregator: scraping ${leadsWithAggregator.length} pages for contact info...`);

  const startUrls = leadsWithAggregator.map(l => ({ url: l.ownedChannels!.linktree! }));

  try {
    const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~cheerio-scraper", {
      startUrls,
      maxCrawlPages: leadsWithAggregator.length,
      maxConcurrency: 30,
      pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  const text = $('body').text();
  const links = [];
  $('a[href]').each(function() { links.push($(this).attr('href')); });
  return { url: request.url, text: text.substring(0, 8000), links: links.slice(0, 200) };
}`,
    }, 90000);
    await storage.incrementApifySpend(runId, actorCost);

    let enrichedCount = 0;
    for (const result of results) {
      const pageUrl = result.url || "";
      const pageText = result.text || "";
      const links: string[] = result.links || [];

      const matchLead = leadsWithAggregator.find(l =>
        l.ownedChannels?.linktree && pageUrl.toLowerCase().includes(
          new URL(l.ownedChannels.linktree).pathname.toLowerCase()
        )
      );

      if (!matchLead) continue;

      let foundAnything = false;

      const emails = extractEmailsFromText(pageText);
      const validEmail = emails.find(e => !isBlockedEmail(e));
      if (validEmail && !matchLead.email) {
        matchLead.email = validEmail;
        foundAnything = true;
      }

      const allUrls = [...links];
      const urlRegex = /https?:\/\/[^\s"'<>,)}\]]+/g;
      const textUrls = pageText.match(urlRegex) || [];
      allUrls.push(...textUrls);

      const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com"];

      for (const link of allUrls) {
        if (!link || typeof link !== "string" || !link.startsWith("http")) continue;
        try {
          const host = new URL(link).hostname.replace(/^www\./, "");

          if (host.includes("linkedin.com") && link.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linkedin = link.split("?")[0];
            foundAnything = true;
          }

          if (!socialHosts.some(s => host.includes(s)) && !matchLead.ownedChannels?.website) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.website = link.split("?")[0];
            foundAnything = true;
          }
        } catch {}
      }

      if (foundAnything) enrichedCount++;
    }

    await appendAndSave(`Link aggregator: enriched ${enrichedCount}/${leadsWithAggregator.length} leads with new data`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] Link aggregator scraping failed: ${err.message}`);
  }
}

const YOUTUBE_ABOUT_MAX = Infinity;

async function enrichFromYouTubeAboutPages(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsWithYouTube = leads.filter(l =>
    l.ownedChannels?.youtube && l.ownedChannels.youtube.startsWith("http")
  ).slice(0, YOUTUBE_ABOUT_MAX);

  if (leadsWithYouTube.length === 0) {
    return;
  }

  await appendAndSave(`YouTube about pages: checking ${leadsWithYouTube.length} channels for email/website...`);

  const startUrls: { url: string }[] = [];
  for (const lead of leadsWithYouTube) {
    const ytUrl = lead.ownedChannels!.youtube!;
    let aboutUrl = ytUrl.replace(/\/+$/, "");
    if (!aboutUrl.includes("/about")) {
      aboutUrl += "/about";
    }
    startUrls.push({ url: aboutUrl });
  }

  try {
    const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~cheerio-scraper", {
      startUrls,
      maxCrawlPages: leadsWithYouTube.length,
      maxConcurrency: 30,
      pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  const text = $('body').text();
  const links = [];
  $('a[href]').each(function() { links.push($(this).attr('href')); });
  return { url: request.url, text: text.substring(0, 8000), links: links.slice(0, 100) };
}`,
    }, 90000);
    await storage.incrementApifySpend(runId, actorCost);

    let enrichedCount = 0;
    for (const result of results) {
      const pageUrl = result.url || "";
      const pageText = result.text || "";
      const links: string[] = result.links || [];

      const channelBase = pageUrl.replace(/\/about\/?$/, "").replace(/\/+$/, "").toLowerCase();

      const matchLead = leadsWithYouTube.find(l => {
        const ytUrl = (l.ownedChannels?.youtube || "").replace(/\/+$/, "").toLowerCase();
        return ytUrl === channelBase || channelBase.startsWith(ytUrl);
      });

      if (!matchLead) continue;

      let foundAnything = false;

      const emails = extractEmailsFromText(pageText);
      const obfuscatedEmails = extractObfuscatedEmails(pageText);
      const allEmails = [...emails, ...obfuscatedEmails];
      const validEmail = allEmails.find(e => !isBlockedEmail(e));
      if (validEmail && !matchLead.email) {
        matchLead.email = validEmail;
        foundAnything = true;
      }

      const allUrls = [...links];
      const urlRegex = /https?:\/\/[^\s"'<>,)}\]]+/g;
      const textUrls = pageText.match(urlRegex) || [];
      allUrls.push(...textUrls);

      const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com", "yelp.com", "tripadvisor.com"];

      for (const link of allUrls) {
        if (!link || typeof link !== "string" || !link.startsWith("http")) continue;
        try {
          const host = new URL(link).hostname.replace(/^www\./, "");

          if (host.includes("linkedin.com") && link.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linkedin = link.split("?")[0];
            foundAnything = true;
          }

          if (!socialHosts.some(s => host.includes(s)) && !matchLead.ownedChannels?.website) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.website = link.split("?")[0];
            foundAnything = true;
          }

          if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link") || host.includes("solo.to") || host.includes("carrd.co") || host.includes("campsite.bio")) && !matchLead.ownedChannels?.linktree) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linktree = link.split("?")[0];
            foundAnything = true;
          }
        } catch {}
      }

      if (!matchLead.leaderName) {
        const extractedName = extractRealNameFromAbout(pageText, matchLead.communityName || "");
        if (extractedName) {
          matchLead.leaderName = extractedName;
          foundAnything = true;
        }
      }

      if (foundAnything) enrichedCount++;
    }

    await appendAndSave(`YouTube about pages: enriched ${enrichedCount}/${leadsWithYouTube.length} leads with new data`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] YouTube about page scraping failed: ${err.message}`);
  }
}

const INSTAGRAM_BIO_MAX = Infinity;

async function enrichFromInstagramBios(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsWithInstagram = leads.filter(l =>
    l.ownedChannels?.instagram && l.ownedChannels.instagram.startsWith("http")
  ).slice(0, INSTAGRAM_BIO_MAX);

  if (leadsWithInstagram.length === 0) return;

  await appendAndSave(`Instagram bios: checking ${leadsWithInstagram.length} profiles for email/website...`);

  const usernames: string[] = [];
  for (const lead of leadsWithInstagram) {
    const igUrl = lead.ownedChannels!.instagram!;
    const match = igUrl.match(/instagram\.com\/([^/?#]+)/i);
    if (match && match[1]) {
      const handle = match[1].replace(/^@/, "").toLowerCase();
      if (handle && handle !== "p" && handle !== "explore" && handle !== "reel" && handle !== "stories") {
        usernames.push(handle);
      }
    }
  }

  if (usernames.length === 0) return;

  try {
    const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~instagram-profile-scraper", {
      usernames,
    }, 300000, 0.0016);
    await storage.incrementApifySpend(runId, actorCost);

    let enrichedCount = 0;
    for (const result of results) {
      const igUsername = (result.username || "").toLowerCase();
      if (!igUsername) continue;

      const matchLead = leadsWithInstagram.find(l => {
        const igUrl = (l.ownedChannels?.instagram || "").toLowerCase();
        return igUrl.includes(`/${igUsername}`) || igUrl.includes(`/${igUsername}/`);
      });

      if (!matchLead) continue;

      let foundAnything = false;

      const bio = result.biography || "";
      const bioEmails = extractEmailsFromText(bio);
      const bioObfuscatedEmails = extractObfuscatedEmails(bio);
      const allBioEmails = [...bioEmails, ...bioObfuscatedEmails];
      const validEmail = allBioEmails.find(e => !isBlockedEmail(e));
      if (validEmail && !matchLead.email) {
        matchLead.email = validEmail;
        foundAnything = true;
      }

      if (!matchLead.ownedChannels?.website) {
        const bareDomains = extractBareDomainUrls(bio);
        if (bareDomains.length > 0) {
          if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
          matchLead.ownedChannels.website = bareDomains[0];
          if (!matchLead.website) matchLead.website = bareDomains[0];
          foundAnything = true;
        }
      }

      const externalUrl = result.externalUrl || "";
      const externalUrls: Array<{ url?: string }> = result.externalUrls || [];
      const allExternalUrls = [externalUrl, ...externalUrls.map(u => u.url || "")].filter(Boolean);

      for (const link of allExternalUrls) {
        if (!link.startsWith("http")) continue;
        try {
          const host = new URL(link).hostname.replace(/^www\./, "");

          if (host.includes("linkedin.com") && link.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linkedin = link.split("?")[0];
            foundAnything = true;
          }

          if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link") || host.includes("solo.to") || host.includes("carrd.co") || host.includes("campsite.bio")) && !matchLead.ownedChannels?.linktree) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linktree = link.split("?")[0];
            foundAnything = true;
          }

          const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly"];
          if (!socialHosts.some(s => host.includes(s)) && !matchLead.ownedChannels?.website) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.website = link.split("?")[0];
            foundAnything = true;
          }
        } catch {}
      }

      const fullName = result.fullName || "";
      if (fullName && !matchLead.leaderName) {
        const words = fullName.trim().split(/\s+/);
        if (words.length >= 2 && words[0].length > 1 && /^[A-Z]/.test(words[0])) {
          matchLead.leaderName = fullName.trim();
          foundAnything = true;
        }
      }

      if (result.followersCount && result.followersCount > 0) {
        if (!matchLead.engagementSignals) matchLead.engagementSignals = {};
        (matchLead.engagementSignals as any).instagram_followers = result.followersCount;
        foundAnything = true;
      }

      if (foundAnything) enrichedCount++;
    }

    await appendAndSave(`Instagram bios: enriched ${enrichedCount}/${leadsWithInstagram.length} leads with new data`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] Instagram bio scraping failed: ${err.message}`);
  }
}

const TWITTER_BIO_MAX = Infinity;

async function enrichFromTwitterBios(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const leadsWithTwitter = leads.filter(l => {
    const tw = l.ownedChannels?.twitter;
    return tw && (tw.startsWith("http") || tw.startsWith("@"));
  }).slice(0, TWITTER_BIO_MAX);

  if (leadsWithTwitter.length === 0) return;

  await appendAndSave(`Twitter bios: checking ${leadsWithTwitter.length} profiles for email/website...`);

  const handleMap = new Map<string, PlatformLead>();
  const handles: string[] = [];

  for (const lead of leadsWithTwitter) {
    const tw = lead.ownedChannels!.twitter!;
    let handle = "";
    if (tw.startsWith("http")) {
      const match = tw.match(/(?:twitter\.com|x\.com)\/([^/?#]+)/i);
      if (match && match[1]) handle = match[1].replace(/^@/, "").toLowerCase();
    } else if (tw.startsWith("@")) {
      handle = tw.replace(/^@/, "").toLowerCase();
    }
    if (handle && handle !== "home" && handle !== "explore" && handle !== "search" && handle !== "i" && handle !== "intent") {
      handles.push(handle);
      handleMap.set(handle, lead);
    }
  }

  if (handles.length === 0) return;

  const paddedHandles = [...handles];
  while (paddedHandles.length < 5) {
    paddedHandles.push(handles[0]);
  }

  try {
    const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apidojo~twitter-user-scraper", {
      twitterHandles: paddedHandles,
      getFollowers: false,
      getFollowing: false,
      getRetweeters: false,
      includeUnavailableUsers: false,
      maxItems: Math.max(handles.length + 5, 10),
    }, 300000, 0.0004);
    await storage.incrementApifySpend(runId, actorCost);

    let enrichedCount = 0;
    const seenHandles = new Set<string>();

    for (const result of results) {
      const userName = (result.userName || "").toLowerCase();
      if (!userName || seenHandles.has(userName)) continue;
      seenHandles.add(userName);

      const matchLead = handleMap.get(userName);
      if (!matchLead) continue;

      let foundAnything = false;

      const bio = result.description || "";
      const bioEmails = extractEmailsFromText(bio);
      const bioObfuscatedEmails = extractObfuscatedEmails(bio);
      const allBioEmails = [...bioEmails, ...bioObfuscatedEmails];
      const validEmail = allBioEmails.find(e => !isBlockedEmail(e));
      if (validEmail && !matchLead.email) {
        matchLead.email = validEmail;
        foundAnything = true;
      }

      if (!matchLead.ownedChannels?.website) {
        const bareDomains = extractBareDomainUrls(bio);
        if (bareDomains.length > 0) {
          if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
          matchLead.ownedChannels.website = bareDomains[0];
          if (!matchLead.website) matchLead.website = bareDomains[0];
          foundAnything = true;
        }
      }

      const websiteUrls: string[] = [];
      if (result.entities?.url?.urls) {
        for (const u of result.entities.url.urls) {
          const expanded = u.expanded_url || u.url || "";
          if (expanded && expanded.startsWith("http")) websiteUrls.push(expanded);
        }
      }
      if (result.entities?.description?.urls) {
        for (const u of result.entities.description.urls) {
          const expanded = u.expanded_url || u.url || "";
          if (expanded && expanded.startsWith("http")) websiteUrls.push(expanded);
        }
      }

      for (const link of websiteUrls) {
        try {
          const host = new URL(link).hostname.replace(/^www\./, "");

          if (host.includes("linkedin.com") && link.includes("/in/") && !matchLead.ownedChannels?.linkedin) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linkedin = link.split("?")[0];
            foundAnything = true;
          }

          if ((host.includes("linktr.ee") || host.includes("beacons.ai") || host.includes("bio.link") || host.includes("solo.to") || host.includes("carrd.co") || host.includes("campsite.bio")) && !matchLead.ownedChannels?.linktree) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.linktree = link.split("?")[0];
            foundAnything = true;
          }

          const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly"];
          if (!socialHosts.some(s => host.includes(s)) && !matchLead.ownedChannels?.website) {
            if (!matchLead.ownedChannels) matchLead.ownedChannels = {};
            matchLead.ownedChannels.website = link.split("?")[0];
            foundAnything = true;
          }
        } catch {}
      }

      const displayName = result.name || "";
      if (displayName && !matchLead.leaderName) {
        const words = displayName.trim().split(/\s+/);
        if (words.length >= 2 && words[0].length > 1 && /^[A-Z]/.test(words[0])) {
          matchLead.leaderName = displayName.trim();
          foundAnything = true;
        }
      }

      if (result.followers && result.followers > 0) {
        if (!matchLead.engagementSignals) matchLead.engagementSignals = {};
        (matchLead.engagementSignals as any).twitter_followers = result.followers;
        foundAnything = true;
      }

      const location = result.location || "";
      if (location && !matchLead.location) {
        matchLead.location = location;
        foundAnything = true;
      }

      if (foundAnything) enrichedCount++;
    }

    await appendAndSave(`Twitter bios: enriched ${enrichedCount}/${leadsWithTwitter.length} leads with new data`);
  } catch (err: any) {
    if (err.costUsd) {
      await storage.incrementApifySpend(runId, err.costUsd);
    }
    await appendAndSave(`[WARN] Twitter bio scraping failed: ${err.message}`);
  }
}

async function crawlCreatorWebsitesForEmails(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<Map<string, string>> {
  const emailMap = new Map<string, string>();
  const socialHosts = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com"];

  const uniqueWebsites = new Map<string, string>();
  for (const lead of leads) {
    if (lead.email) continue;
    const website = lead.ownedChannels?.website;
    if (!website || typeof website !== "string" || !website.startsWith("http")) continue;
    if (isPatreonCdnUrl(website)) continue;
    const domain = extractDomain(website);
    if (!domain) continue;
    if (socialHosts.some((s) => domain.includes(s))) continue;
    if (!uniqueWebsites.has(domain)) {
      uniqueWebsites.set(domain, website);
    }
  }

  const websiteEntries = Array.from(uniqueWebsites.entries());
  if (websiteEntries.length === 0) {
    await appendAndSave("Website crawl: no eligible personal websites found");
    return emailMap;
  }

  await appendAndSave(`Website crawl: found ${websiteEntries.length} unique personal websites to crawl`);

  const batchSize = 10;
  const concurrentBatches = 5;
  const allBatches: { entries: [string, string][]; batchNum: number }[] = [];
  const totalBatches = Math.ceil(websiteEntries.length / batchSize);

  for (let i = 0; i < websiteEntries.length; i += batchSize) {
    allBatches.push({
      entries: websiteEntries.slice(i, i + batchSize),
      batchNum: Math.floor(i / batchSize) + 1,
    });
  }

  async function processCrawlBatch(batch: { entries: [string, string][]; batchNum: number }): Promise<void> {
    await appendAndSave(`Website crawl: processing batch ${batch.batchNum}/${totalBatches} (${batch.entries.length} sites)...`);

    const startUrls: { url: string }[] = [];
    const globs: { glob: string }[] = [];

    for (const [domain, url] of batch.entries) {
      const baseUrl = url.replace(/\/+$/, "");
      startUrls.push({ url: baseUrl });
      const subpages = ["/contact", "/about", "/about-us", "/contact-us", "/team", "/staff", "/bio", "/press", "/people", "/our-team", "/meet-the-team", "/leadership"];
      for (const page of subpages) {
        startUrls.push({ url: `${baseUrl}${page}` });
      }
      globs.push({ glob: `https://${domain}/contact*` });
      globs.push({ glob: `https://${domain}/about*` });
      globs.push({ glob: `https://${domain}/team*` });
      globs.push({ glob: `https://${domain}/staff*` });
      globs.push({ glob: `https://${domain}/people*` });
      globs.push({ glob: `https://www.${domain}/contact*` });
      globs.push({ glob: `https://www.${domain}/about*` });
      globs.push({ glob: `https://www.${domain}/team*` });
    }

    try {
      const { items: results, costUsd: actorCost } = await runActorWithWallClockTimeout("apify~cheerio-scraper", {
        startUrls,
        globs,
        maxCrawlPages: 8 * batch.entries.length,
        maxConcurrency: 6,
        pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  const text = $('body').text();
  return { url: request.url, text: text.substring(0, 5000) };
}`,
      }, 300000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const result of results) {
        const pageText = result.text || "";
        const pageUrl = result.url || "";
        if (!pageText || !pageUrl) continue;

        const pageDomain = extractDomain(pageUrl);
        if (!pageDomain || emailMap.has(pageDomain)) continue;

        const emails = extractEmailsFromText(pageText);
        const validEmail = emails.find((e) => {
          if (isBlockedEmail(e)) return false;
          const emailDomain = e.split("@")[1]?.toLowerCase();
          if (!emailDomain) return false;
          const siteBase = pageDomain.toLowerCase().replace(/^www\./, "");
          const emailBase = emailDomain.replace(/^www\./, "");
          if (emailBase === siteBase || emailBase.endsWith("." + siteBase) || siteBase.endsWith("." + emailBase)) return true;
          const siteWords = siteBase.split(".")[0];
          const emailWords = emailBase.split(".")[0];
          if (siteWords.length >= 4 && emailWords.length >= 4 && (siteWords.includes(emailWords) || emailWords.includes(siteWords))) return true;
          return false;
        });
        if (validEmail) {
          emailMap.set(pageDomain, validEmail);
        }
      }

      await appendAndSave(`Website crawl batch ${batch.batchNum}: found ${emailMap.size} total emails so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Website crawl batch ${batch.batchNum} failed: ${err.message}`);
    }
  }

  for (let i = 0; i < allBatches.length; i += concurrentBatches) {
    const concurrentSlice = allBatches.slice(i, i + concurrentBatches);
    await Promise.allSettled(concurrentSlice.map((b, idx) =>
      new Promise<void>(resolve => setTimeout(() => processCrawlBatch(b).then(resolve).catch(resolve), idx * 2000))
    ));
  }

  await appendAndSave(`Website crawl complete: found ${emailMap.size} emails from ${websiteEntries.length} websites`);
  return emailMap;
}

export async function runPipeline(runId: number): Promise<void> {
  let currentLogs = "";
  let stopHeartbeat: (() => void) | null = null;

  const appendAndSave = async (msg: string, progress?: number, step?: string) => {
    if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
    currentLogs = appendLog(currentLogs, msg);
    log(msg, "pipeline");
    const update: any = { logs: currentLogs };
    if (progress !== undefined) update.progress = progress;
    if (step !== undefined) update.step = step;
    await storage.updateRun(runId, update);
  };

  try {
    const run = await storage.getRun(runId);
    if (!run || !run.params) throw new Error("Run not found or missing params");

    const params = run.params as RunParams;
    activeRunIds.add(runId);
    stopHeartbeat = startHeartbeat(runId);
    await storage.updateRun(runId, { status: "running", startedAt: new Date() });

    const isAutonomousRun = run.isAutonomous || false;
    const budgetUsd = run.budgetUsd || 0;
    const globalEmailTarget = run.emailTarget || 0;

    if (isAutonomousRun && budgetUsd > 0) {
      const targetNote = globalEmailTarget > 0 ? `, target: ${globalEmailTarget} emails` : "";
      await appendAndSave(`Autonomous mode: $${budgetUsd.toFixed(2)} budget${targetNote}`, 2, "Step 1: Platform-specific discovery");
    } else {
      await appendAndSave("Pipeline started", 2, "Step 1: Platform-specific discovery");
    }

    const keywords = params.seedKeywords;
    const geos = params.seedGeos;
    const enabledSources = params.enabledSources || ["meetup", "youtube", "reddit", "eventbrite", "google"];
    const platformSources = enabledSources.filter((s) => s !== "google");
    const defaultMaxPerPlatform = Math.floor(params.maxDiscoveredUrls / Math.max(1, platformSources.length));

    const budgetAllocation = (run.budgetAllocation as BudgetAllocation | null);
    const platformMaxLeads = new Map<string, number>();
    if (isAutonomousRun && budgetAllocation?.platforms) {
      for (const pa of budgetAllocation.platforms) {
        platformMaxLeads.set(pa.platform, pa.maxLeads);
      }
    }
    const getMaxForPlatform = (platform: string) => platformMaxLeads.get(platform) || defaultMaxPerPlatform;

    const platformTasks: { name: string; promise: Promise<PlatformLead[]> }[] = [];
    if (enabledSources.includes("meetup")) {
      platformTasks.push({ name: "Meetup", promise: scrapeMeetupGroups(runId, keywords, geos, getMaxForPlatform("meetup"), (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("youtube")) {
      platformTasks.push({ name: "YouTube", promise: scrapeYouTubeChannels(runId, keywords, getMaxForPlatform("youtube"), (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("reddit")) {
      platformTasks.push({ name: "Reddit", promise: scrapeRedditCommunities(runId, keywords, getMaxForPlatform("reddit"), (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("eventbrite")) {
      platformTasks.push({ name: "Eventbrite", promise: scrapeEventbriteEvents(runId, keywords, geos, getMaxForPlatform("eventbrite"), (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("facebook")) {
      platformTasks.push({ name: "Facebook", promise: scrapeFacebookGroups(runId, keywords, getMaxForPlatform("facebook"), (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
        geos,
      }) });
    }
    if (enabledSources.includes("patreon")) {
      platformTasks.push({ name: "Patreon", promise: scrapePatreonCreators(runId, keywords, getMaxForPlatform("patreon"), (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
        minPostCount: params.minPostCount || 0,
      }) });
    }
    if (enabledSources.includes("podcast")) {
      platformTasks.push({ name: "Podcasts", promise: scrapeApplePodcasts(runId, keywords, getMaxForPlatform("podcast"), (msg) => appendAndSave(msg), {
        minEpisodeCount: params.minEpisodeCount || 0,
        podcastCountry: params.podcastCountry || "US",
      }) });
    }
    if (enabledSources.includes("substack")) {
      platformTasks.push({ name: "Substack", promise: scrapeSubstackWriters(runId, keywords, getMaxForPlatform("substack"), (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("mighty")) {
      platformTasks.push({ name: "Mighty Networks", promise: scrapeMightyNetworks(runId, keywords, getMaxForPlatform("mighty"), (msg) => appendAndSave(msg), params.seedGeos || []) });
    }
    if (enabledSources.includes("linkedin")) {
      platformTasks.push({ name: "LinkedIn Groups", promise: scrapeLinkedInGroups(runId, keywords, getMaxForPlatform("linkedin"), (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
        geos,
      }) });
    }

    if (platformTasks.length === 0) {
      await appendAndSave("No platform sources selected, skipping platform discovery");
    }

    const platformResults = await Promise.allSettled(platformTasks.map((t) => t.promise));

    const allPlatformLeads: PlatformLead[] = [];
    for (let i = 0; i < platformResults.length; i++) {
      const result = platformResults[i];
      if (result.status === "fulfilled") {
        allPlatformLeads.push(...result.value);
        await appendAndSave(`${platformTasks[i].name}: ${result.value.length} results`);
      } else {
        await appendAndSave(`[WARN] ${platformTasks[i].name} failed: ${result.reason?.message || "Unknown error"}`);
      }
    }

    await markStepComplete(runId, PIPELINE_STEPS.DISCOVERY);

    const mergedCount = mergeCrossPlatformLeads(allPlatformLeads);
    if (mergedCount > 0) {
      await appendAndSave(`Cross-platform merge: merged ${mergedCount} duplicate leads, ${allPlatformLeads.length} remaining`);
    }

    await saveCheckpoint(runId, allPlatformLeads, "discovery");

    const realNameCount = allPlatformLeads.filter(l => {
      const aboutText = l.raw?.about || "";
      return extractRealNameFromAbout(aboutText, l.communityName || "") !== null;
    }).length;
    if (realNameCount > 0) {
      await appendAndSave(`Real name extraction: found real names for ${realNameCount}/${allPlatformLeads.length} leads from about text`);
    }

    const enrichGroup1: { name: string; promise: Promise<void> }[] = [];

    const hasLinkedInLeads = allPlatformLeads.some(l => l.source === "linkedin" && !l.leaderName && l.ownedChannels?.linkedin_group);
    if (hasLinkedInLeads) {
      enrichGroup1.push({ name: "LinkedIn Group Pages", promise: enrichLinkedInGroupPages(runId, allPlatformLeads, appendAndSave) });
    }

    const hasGroupLeads = allPlatformLeads.some(l => l.source === "facebook" || l.source === "meetup" || l.source === "linkedin");
    if (hasGroupLeads) {
      const groupLeadsNeedingEnrich = allPlatformLeads.filter(l => (l.source === "facebook" || l.source === "meetup" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
      if (groupLeadsNeedingEnrich.length > 0) {
        await appendAndSave(`Google Bridge: ${groupLeadsNeedingEnrich.length} Facebook/Meetup/LinkedIn groups need leader/org lookup`, 29, "Step 1b: Parallel enrichment group 1");
        enrichGroup1.push({ name: "Google Bridge", promise: googleBridgeEnrichFacebookGroups(runId, allPlatformLeads, appendAndSave) });
      }
    }

    const hasPodcastLeads = allPlatformLeads.some(l => l.source === "podcast");
    if (hasPodcastLeads) {
      const podcastEmailCount = allPlatformLeads.filter(l => l.source === "podcast" && l.email).length;
      const podcastTotal = allPlatformLeads.filter(l => l.source === "podcast").length;
      await appendAndSave(`RSS feed scrape: skipped (already done inline during podcast discovery — ${podcastEmailCount}/${podcastTotal} have emails)`);
    }

    const nonPodcastLeadsWithRss = allPlatformLeads.filter(l => l.source !== "podcast" && l.ownedChannels?.rss && l.ownedChannels.rss.startsWith("http") && !l.email);
    if (nonPodcastLeadsWithRss.length > 0) {
      await appendAndSave(`RSS feed scrape: ${nonPodcastLeadsWithRss.length} non-podcast leads have RSS feeds`);
      enrichGroup1.push({ name: "RSS feeds", promise: enrichFromRssFeeds(runId, allPlatformLeads.filter(l => l.source !== "podcast"), appendAndSave) });
    }

    const leadsWithLinktreeInitial = allPlatformLeads.filter(l =>
      !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http")
    );
    if (leadsWithLinktreeInitial.length > 0) {
      await appendAndSave(`Link aggregator scrape: ${leadsWithLinktreeInitial.length} leads have Linktree/Beacons pages`);
      enrichGroup1.push({ name: "Link aggregators (pass 1)", promise: enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave) });
    }

    if (enrichGroup1.length > 0) {
      await appendAndSave(`Running ${enrichGroup1.length} enrichment tasks in parallel: ${enrichGroup1.map(t => t.name).join(", ")}`);
      const g1Results = await Promise.allSettled(enrichGroup1.map(t => t.promise));
      for (let i = 0; i < g1Results.length; i++) {
        if (g1Results[i].status === "rejected") {
          await appendAndSave(`[WARN] ${enrichGroup1[i].name} failed: ${(g1Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
        }
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.FB_GOOGLE_BRIDGE);
    await saveCheckpoint(runId, allPlatformLeads, "fb_google_bridge");

    const enrichGroup2: { name: string; promise: Promise<void> }[] = [];

    const budgetExhaustedBeforeG2 = await isBudgetExhausted(runId, 0.05);
    const emailTargetReachedBeforeG2 = await isEmailTargetReached(runId);
    if (budgetExhaustedBeforeG2 || emailTargetReachedBeforeG2) {
      const reason = emailTargetReachedBeforeG2 ? "Email target reached" : `Budget limit reached ($${(await getRunBudgetInfo(runId)).spentUsd.toFixed(2)} spent)`;
      await appendAndSave(`${reason} — skipping social scraping`);
    } else {
      const leadsWithYouTube = allPlatformLeads.filter(l =>
        l.ownedChannels?.youtube && l.ownedChannels.youtube.startsWith("http")
      );
      if (leadsWithYouTube.length > 0) {
        await appendAndSave(`YouTube about pages: ${leadsWithYouTube.length} leads have YouTube channels`, 30, "Step 2: Parallel enrichment group 2");
        enrichGroup2.push({ name: "YouTube about pages", promise: enrichFromYouTubeAboutPages(runId, allPlatformLeads, appendAndSave) });
      }

      const leadsWithInstagram = allPlatformLeads.filter(l =>
        l.ownedChannels?.instagram && l.ownedChannels.instagram.startsWith("http")
      );
      if (leadsWithInstagram.length > 0) {
        await appendAndSave(`Instagram bios: ${leadsWithInstagram.length} leads have Instagram profiles`);
        enrichGroup2.push({ name: "Instagram bios", promise: enrichFromInstagramBios(runId, allPlatformLeads, appendAndSave) });
      }

      const leadsWithTwitter = allPlatformLeads.filter(l => {
        const tw = l.ownedChannels?.twitter;
        return tw && (tw.startsWith("http") || tw.startsWith("@"));
      });
      if (leadsWithTwitter.length > 0) {
        await appendAndSave(`Twitter bios: ${leadsWithTwitter.length} leads have Twitter/X profiles`);
        enrichGroup2.push({ name: "Twitter/X bios", promise: enrichFromTwitterBios(runId, allPlatformLeads, appendAndSave) });
      }
    }

    if (enrichGroup2.length > 0) {
      await appendAndSave(`Running ${enrichGroup2.length} social scrape tasks in parallel: ${enrichGroup2.map(t => t.name).join(", ")}`);
      const g2Results = await Promise.allSettled(enrichGroup2.map(t => t.promise));
      for (let i = 0; i < g2Results.length; i++) {
        if (g2Results[i].status === "rejected") {
          await appendAndSave(`[WARN] ${enrichGroup2[i].name} failed: ${(g2Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
        }
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.INSTAGRAM_BIOS);
    await saveCheckpoint(runId, allPlatformLeads, "social_scraping");

    const enrichGroup3: { name: string; promise: Promise<void> }[] = [];

    const newLinktreeLeads = allPlatformLeads.filter(l =>
      !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http") &&
      !leadsWithLinktreeInitial.includes(l)
    );
    if (newLinktreeLeads.length > 0) {
      await appendAndSave(`Link aggregator scrape (pass 2): ${newLinktreeLeads.length} new aggregator URLs from YouTube/IG/Twitter`, 33, "Step 2d: Post-social enrichment");
      enrichGroup3.push({ name: "Link aggregators (pass 2)", promise: enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave) });
    }

    const leadsWithoutContactInfo = allPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
    if (leadsWithoutContactInfo.length > 0) {
      await appendAndSave(`Google enrichment: ${leadsWithoutContactInfo.length} leads need website/LinkedIn lookup`);
      enrichGroup3.push({ name: "Google contact search", promise: googleSearchEnrichCreators(runId, allPlatformLeads, appendAndSave) });
    } else {
      await appendAndSave("Google enrichment: skipped (all leads already have contact info)");
    }

    if (enrichGroup3.length > 0) {
      await appendAndSave(`Running ${enrichGroup3.length} post-social tasks in parallel: ${enrichGroup3.map(t => t.name).join(", ")}`);
      const g3Results = await Promise.allSettled(enrichGroup3.map(t => t.promise));
      for (let i = 0; i < g3Results.length; i++) {
        if (g3Results[i].status === "rejected") {
          await appendAndSave(`[WARN] ${enrichGroup3[i].name} failed: ${(g3Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
        }
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.GOOGLE_CONTACT_SEARCH);
    await saveCheckpoint(runId, allPlatformLeads, "google_contact_search");

    let slugDomainsProbed = 0;
    for (const pl of allPlatformLeads) {
      if (pl.ownedChannels?.website || pl.email) continue;
      const patreonUrl = pl.website || pl.ownedChannels?.patreon || "";
      if (!patreonUrl.includes("patreon.com/")) continue;
      const slug = patreonUrl.split("patreon.com/")[1]?.split(/[?#/]/)[0]?.toLowerCase();
      if (!slug || slug.length < 3 || slug.startsWith("u") && /^\d+$/.test(slug.slice(1))) continue;
      if (/[^a-z0-9_-]/.test(slug)) continue;
      const cleanSlug = slug.replace(/[_-]/g, "");
      if (cleanSlug.length < 4) continue;
      const candidateDomain = `${cleanSlug}.com`;
      if (!pl.ownedChannels) pl.ownedChannels = {};
      pl.ownedChannels.website = `https://${candidateDomain}`;
      slugDomainsProbed++;
    }
    if (slugDomainsProbed > 0) {
      await appendAndSave(`Slug domain probe: trying ${slugDomainsProbed} Patreon slugs as .com domains`);
    }

    const leadsNeedingEmail = allPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
    if (leadsNeedingEmail.length > 0) {
      await appendAndSave(`Crawling ${leadsNeedingEmail.length} creator websites for contact emails...`, 38, "Step 3: Website contact crawl");
      const websiteEmailMap = await crawlCreatorWebsitesForEmails(runId, leadsNeedingEmail, appendAndSave);
      let websiteEmailsMerged = 0;
      for (const pl of allPlatformLeads) {
        if (!pl.email && pl.ownedChannels?.website) {
          const domain = extractDomain(pl.ownedChannels.website);
          if (domain && websiteEmailMap.has(domain)) {
            pl.email = websiteEmailMap.get(domain)!;
            websiteEmailsMerged++;
          }
        }
      }
      await appendAndSave(`Website crawl: found ${websiteEmailsMerged} emails from contact pages`);
    } else {
      await appendAndSave("Website crawl: no leads with personal websites needing email");
    }
    await markStepComplete(runId, PIPELINE_STEPS.WEBSITE_CRAWL);
    await saveCheckpoint(runId, allPlatformLeads, "website_crawl");

    await appendAndSave(`Platform discovery complete: ${allPlatformLeads.length} results`, 40, "Step 4: Discovery summary");

    const emailsAfterDiscovery = allPlatformLeads.filter((l) => l.email).length;
    await appendAndSave(`After discovery: ${emailsAfterDiscovery}/${allPlatformLeads.length} leads have emails`, 45, "Step 5: Keyword discovery");

    let allDiscoveredUrls: { url: string; domain: string; source: string }[] = [];

    if (enabledSources.includes("google")) {
    const queries = buildGoogleQueries(params);
    await appendAndSave(`Generated ${queries.length} Google search queries`);

    const skipDomains = ["instagram.com"];
    if (enabledSources.includes("meetup")) skipDomains.push("meetup.com");
    if (enabledSources.includes("youtube")) skipDomains.push("youtube.com", "youtu.be");
    if (enabledSources.includes("reddit")) skipDomains.push("reddit.com");
    if (enabledSources.includes("eventbrite")) skipDomains.push("eventbrite.com");
    if (enabledSources.includes("facebook")) skipDomains.push("facebook.com");
    if (enabledSources.includes("patreon")) skipDomains.push("patreon.com");
    if (enabledSources.includes("mighty")) skipDomains.push("mn.co", "mightynetworks.com");
    if (enabledSources.includes("linkedin")) skipDomains.push("linkedin.com");

    const batchSize = 20;
    const queryBatches = [];
    for (let i = 0; i < queries.length; i += batchSize) {
      queryBatches.push(queries.slice(i, i + batchSize));
    }

    for (let batchIdx = 0; batchIdx < queryBatches.length; batchIdx++) {
      const batch = queryBatches[batchIdx];
      if (allDiscoveredUrls.length >= params.maxDiscoveredUrls) break;

      try {
        await appendAndSave(`Google Search batch ${batchIdx + 1}/${queryBatches.length} (${batch.length} queries)`);

        const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
          queries: batch.join("\n"),
          maxPagesPerQuery: 1,
          resultsPerPage: params.maxGoogleResultsPerQuery,
          languageCode: "en",
          mobileResults: false,
        }, 120000);
        await storage.incrementApifySpend(runId, actorCost);

        for (const item of items) {
          if (allDiscoveredUrls.length >= params.maxDiscoveredUrls) break;

          const results = item.organicResults || [];
          for (const result of results) {
            if (allDiscoveredUrls.length >= params.maxDiscoveredUrls) break;
            const url = result.url || result.link;
            if (!url) continue;

            const domain = extractDomain(url);
            if (!domain) continue;
            if (skipDomains.some((d) => domain.includes(d))) continue;

            if (!allDiscoveredUrls.some((u) => u.url === url)) {
              allDiscoveredUrls.push({ url, domain, source: classifyUrl(url) });
            }
          }
        }

        await appendAndSave(`Google batch ${batchIdx + 1} complete. Total website URLs: ${allDiscoveredUrls.length}`);
      } catch (err: any) {
        if (err.costUsd) {
          await storage.incrementApifySpend(runId, err.costUsd);
        }
        await appendAndSave(`[ERROR] Google batch ${batchIdx + 1} failed: ${err.message}`);
      }

      const progress = 35 + Math.round((batchIdx / queryBatches.length) * 15);
      await appendAndSave(`Progress update`, progress);
    }

    await appendAndSave(`Google discovery complete: ${allDiscoveredUrls.length} website URLs`);
    } else {
      await appendAndSave("Google URL discovery skipped (using platform-specific discovery instead)");
    }

    const sourceUrlsData: InsertSourceUrl[] = allDiscoveredUrls.map((u) => ({
      url: u.url,
      domain: u.domain,
      source: u.source,
      fetchStatus: "new",
      runId,
    }));
    if (sourceUrlsData.length > 0) {
      await storage.createSourceUrls(sourceUrlsData);
    }
    await storage.updateRun(runId, { urlsDiscovered: allDiscoveredUrls.length + allPlatformLeads.length });

    await appendAndSave(
      `Discovery complete: ${allPlatformLeads.length} platform + ${allDiscoveredUrls.length} website leads`,
      50,
      "Step 6: Extract website data"
    );

    const websiteUrls = allDiscoveredUrls
      .filter((u) => u.source === "website" || u.source === "substack")
      .map((u) => u.url)
      .slice(0, Math.min(100, params.maxDiscoveredUrls));

    let extractedPages: any[] = [];

    if (websiteUrls.length > 0) {
      const extractBatchSize = 10;
      const totalBatches = Math.ceil(websiteUrls.length / extractBatchSize);

      for (let i = 0; i < websiteUrls.length; i += extractBatchSize) {
        const batch = websiteUrls.slice(i, i + extractBatchSize);
        const batchNum = Math.floor(i / extractBatchSize) + 1;
        try {
          await appendAndSave(`Extracting ${batch.length} websites (batch ${batchNum}/${totalBatches})`);

          const { items, costUsd: actorCost } = await runActorAndGetResults("apify~cheerio-scraper", {
            startUrls: batch.map((u) => ({ url: u })),
            maxRequestsPerCrawl: batch.length * 4,
            maxConcurrency: 30,
            maxRequestRetries: 1,
            linkSelector: "a[href]",
            pseudoUrls: batch.map((baseUrl) => {
              const base = new URL(baseUrl);
              return { purl: `${base.origin}/[.*]` };
            }),
            pageFunction: `async function pageFunction(context) {
              const { request, $, log, enqueueLinks } = context;
              const title = $('title').text().trim();
              const description = $('meta[name="description"]').attr('content') || '';
              const bodyText = $('body').text().replace(/\\s+/g, ' ').substring(0, 8000);

              var mailtoEmails = [];
              $('a[href^="mailto:"]').each(function() {
                var href = $(this).attr('href') || '';
                var email = href.replace('mailto:', '').split('?')[0].trim();
                if (email && email.includes('@')) mailtoEmails.push(email);
              });

              var footerText = '';
              $('footer, .footer, #footer, [role="contentinfo"]').each(function() {
                footerText += ' ' + $(this).text();
              });

              var schemaEmails = [];
              $('script[type="application/ld+json"]').each(function() {
                try {
                  var data = JSON.parse($(this).html() || '{}');
                  if (data.email) schemaEmails.push(data.email);
                  if (data.contactPoint) {
                    var cp = Array.isArray(data.contactPoint) ? data.contactPoint : [data.contactPoint];
                    cp.forEach(function(c) { if (c.email) schemaEmails.push(c.email); });
                  }
                } catch(e) {}
              });

              var contactLinks = [];
              var contactPatterns = /\\b(contact|about|team|staff|leadership|our-team|meet-the-team|organizer|founder|who-we-are|board|people|connect|get-in-touch|join|membership|connect-with-us)\\b/i;
              $('a[href]').each(function() {
                var href = $(this).attr('href') || '';
                var text = $(this).text().toLowerCase().trim();
                if (contactPatterns.test(href) || contactPatterns.test(text)) {
                  contactLinks.push(href);
                }
              });

              var allLinks = [];
              $('a[href]').each(function() {
                var href = $(this).attr('href') || '';
                if (href.includes('linkedin.com/in/')) allLinks.push(href);
                if (href.includes('linkedin.com/company/')) allLinks.push(href);
                if (href.includes('twitter.com/') || href.includes('x.com/')) allLinks.push(href);
                if (href.includes('instagram.com/')) allLinks.push(href);
                if (href.includes('facebook.com/')) allLinks.push(href);
              });

              if (request.userData && request.userData.isSubpage) {
                return {
                  url: request.url,
                  parentUrl: request.userData.parentUrl,
                  isSubpage: true,
                  title: title,
                  description: '',
                  bodyText: bodyText + ' ' + footerText,
                  contactLinks: [],
                  socialLinks: allLinks,
                  mailtoEmails: mailtoEmails,
                  schemaEmails: schemaEmails,
                };
              }

              try {
                var absoluteContactLinks = contactLinks.map(function(link) {
                  try { return new URL(link, request.url).href; } catch(e) { return ''; }
                }).filter(function(u) { return u && u.startsWith('http'); });

                var uniqueContactLinks = absoluteContactLinks.filter(function(v, i, a) { return a.indexOf(v) === i; }).slice(0, 3);
                if (uniqueContactLinks.length > 0) {
                  await enqueueLinks({
                    urls: uniqueContactLinks,
                    userData: { isSubpage: true, parentUrl: request.url },
                  });
                }
              } catch(e) {
                log.warning('Failed to enqueue subpage links: ' + e.message);
              }

              return {
                url: request.url,
                isSubpage: false,
                title: title,
                description: description,
                bodyText: bodyText + ' ' + footerText,
                contactLinks: contactLinks.slice(0, 10),
                socialLinks: allLinks,
                mailtoEmails: mailtoEmails,
                schemaEmails: schemaEmails,
              };
            }`,
          }, 180000);
          await storage.incrementApifySpend(runId, actorCost);

          const mainPages = new Map<string, any>();
          const subPages: any[] = [];

          for (const item of items) {
            if (item.isSubpage) {
              subPages.push(item);
            } else {
              mainPages.set(item.url, item);
            }
          }

          for (const sub of subPages) {
            const parent = mainPages.get(sub.parentUrl);
            if (parent) {
              parent.bodyText = (parent.bodyText || "") + " " + (sub.bodyText || "");
              if (sub.socialLinks) {
                parent.socialLinks = [...(parent.socialLinks || []), ...sub.socialLinks];
              }
            }
          }

          for (const item of Array.from(mainPages.values())) {
            extractedPages.push(item);
          }

          await appendAndSave(`Extracted ${mainPages.size} websites + ${subPages.length} subpages in batch ${batchNum}`);
        } catch (err: any) {
          if (err.costUsd) {
            await storage.incrementApifySpend(runId, err.costUsd);
          }
          await appendAndSave(`[ERROR] Web extraction batch ${batchNum} failed: ${err.message}`);
        }

        const progress = 50 + Math.round((batchNum / totalBatches) * 15);
        await appendAndSave(`Progress update`, progress);
      }
    }

    await appendAndSave(
      `Extraction complete: ${extractedPages.length} pages + ${allPlatformLeads.length} platform results`,
      65,
      "Step 7: Create & score leads"
    );

    let createdCount = 0;

    for (const pl of allPlatformLeads) {
      try {
        let existingLead = null;
        if (pl.email) existingLead = await storage.findLeadByEmail(pl.email);
        if (!existingLead && pl.website) existingLead = await storage.findLeadByWebsite(pl.website);
        if (!existingLead && pl.communityName) {
          existingLead = await storage.findLeadByNameAndLocation(pl.communityName, pl.location);
        }

        if (existingLead) {
          await storage.updateLead(existingLead.id, { lastSeenAt: new Date() });
          continue;
        }

        const community = await storage.createCommunity({
          name: pl.communityName,
          type: pl.communityType,
          description: pl.description,
          website: pl.website,
          ownedChannels: pl.ownedChannels,
          eventCadence: pl.engagementSignals,
          audienceSignals: pl.tripFitSignals,
          sourceUrls: [pl.website],
        });

        let leaderId: number | undefined;
        if (pl.leaderName) {
          const leader = await storage.createLeader({
            name: pl.leaderName,
            role: "",
            email: pl.email,
            phone: pl.phone,
            sourceUrl: pl.website,
            communityId: community.id,
          });
          leaderId = leader.id;
        }

        const scoringInput = {
          name: pl.communityName,
          description: pl.description,
          type: pl.communityType,
          location: pl.location,
          website: pl.website,
          email: pl.email,
          phone: pl.phone,
          linkedin: pl.ownedChannels?.linkedin || "",
          ownedChannels: pl.ownedChannels,
          monetizationSignals: pl.monetizationSignals,
          engagementSignals: pl.engagementSignals,
          tripFitSignals: pl.tripFitSignals,
          leaderName: pl.leaderName,
          memberCount: pl.memberCount,
          subscriberCount: pl.subscriberCount,
          raw: pl.raw,
        };

        const breakdown = scoreLead(scoringInput);

        const leadData: InsertLead = {
          leadType: pl.leaderName ? "leader" : "community",
          communityName: pl.communityName,
          communityType: pl.communityType,
          leaderName: pl.leaderName,
          firstName: extractHighConfidenceFirstName(pl.leaderName),
          researchSummary: generateResearchSummary({ communityName: pl.communityName, communityType: pl.communityType, leaderName: pl.leaderName, description: pl.description, source: pl.source, website: pl.website, location: pl.location, ownedChannels: pl.ownedChannels, engagementSignals: pl.engagementSignals, monetizationSignals: pl.monetizationSignals, tripFitSignals: pl.tripFitSignals, raw: pl.raw }),
          location: pl.location,
          website: pl.website,
          email: pl.email,
          phone: pl.phone,
          linkedin: pl.ownedChannels?.linkedin || "",
          ownedChannels: pl.ownedChannels,
          monetizationSignals: pl.monetizationSignals,
          engagementSignals: pl.engagementSignals,
          tripFitSignals: pl.tripFitSignals,
          score: breakdown.total,
          scoreBreakdown: breakdown,
          status: "new",
          source: pl.source || "",
          raw: pl.raw,
          runId,
          communityId: community.id,
          leaderId,
        };

        await storage.createLead(leadData);
        createdCount++;
      } catch (err: any) {
        await appendAndSave(`[ERROR] Platform lead processing failed: ${err.message}`);
      }
    }

    await appendAndSave(`Created ${createdCount} leads from platforms`, 75);

    for (const item of extractedPages) {
      try {
        const pageText = [item.title || "", item.description || "", item.bodyText || ""].join(" ");
        const url = item.url || "";
        const domain = extractDomain(url);

        const mailtoEmails: string[] = (item.mailtoEmails || []) as string[];
        const schemaEmails: string[] = (item.schemaEmails || []) as string[];
        const textEmails = extractEmailsFromText(pageText);
        const emails = Array.from(new Set([...mailtoEmails, ...schemaEmails, ...textEmails]));
        const phones = extractPhonesFromText(pageText);
        const channels = detectOwnedChannels(pageText, url);
        const monetization = detectMonetization(pageText);
        const engagement = detectEngagement(pageText);
        const tripFit = detectTripFit(pageText);
        const communityType = detectCommunityType(pageText);

        const socialLinks = (item.socialLinks || []) as string[];
        const linkedinUrl = socialLinks.find((l: string) => l.includes("linkedin.com/in/")) || "";
        for (const sl of socialLinks) {
          if (sl.includes("twitter.com/") || sl.includes("x.com/")) channels.twitter = sl;
          if (sl.includes("instagram.com/")) channels.instagram = sl;
          if (sl.includes("facebook.com/")) channels.facebook = sl;
          if (sl.includes("linkedin.com/in/")) channels.linkedin = sl;
        }

        const name = item.title || domain || "Unknown Community";
        const email = emails[0] || "";

        let existingLead = null;
        if (email) existingLead = await storage.findLeadByEmail(email);
        if (!existingLead && url) existingLead = await storage.findLeadByWebsite(url);

        if (existingLead) {
          await storage.updateLead(existingLead.id, { lastSeenAt: new Date() });
          continue;
        }

        let existingCommunity = url ? await storage.findCommunityByWebsite(url) : undefined;
        let communityId: number | undefined;
        if (!existingCommunity) {
          const community = await storage.createCommunity({
            name,
            type: communityType,
            description: item.description || "",
            website: url,
            ownedChannels: channels,
            eventCadence: engagement,
            audienceSignals: tripFit,
            sourceUrls: [url],
          });
          communityId = community.id;
        } else {
          communityId = existingCommunity.id;
        }

        let leaderName = "";
        let leaderId: number | undefined;
        const leaderPatterns = [
          /(?:pastor|reverend|rev\.?|father|fr\.?|rabbi|imam)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/gi,
          /(?:led by|organized by|hosted by|contact|director|president|founder|coordinator)\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/gi,
        ];

        for (const pattern of leaderPatterns) {
          const match = pattern.exec(pageText);
          if (match && match[1]) {
            leaderName = match[1].trim();
            break;
          }
        }

        if (leaderName && communityId) {
          const leader = await storage.createLeader({
            name: leaderName,
            role: "",
            email,
            phone: phones[0] || "",
            sourceUrl: url,
            communityId,
          });
          leaderId = leader.id;
        }

        const scoringInput = {
          name,
          description: item.description || "",
          type: communityType,
          location: "",
          website: url,
          email,
          phone: phones[0] || "",
          linkedin: linkedinUrl,
          ownedChannels: channels,
          monetizationSignals: monetization,
          engagementSignals: engagement,
          tripFitSignals: tripFit,
          leaderName,
          memberCount: 0,
          subscriberCount: 0,
          raw: item,
        };

        const breakdown = scoreLead(scoringInput);

        const leadData: InsertLead = {
          leadType: leaderName ? "leader" : "community",
          communityName: name,
          communityType,
          leaderName,
          firstName: extractHighConfidenceFirstName(leaderName),
          researchSummary: generateResearchSummary({ communityName: name, communityType, leaderName, description: item.description || "", source: "google", website: url, ownedChannels: channels, engagementSignals: engagement, monetizationSignals: monetization, tripFitSignals: tripFit, raw: item }),
          location: "",
          website: url,
          email,
          phone: phones[0] || "",
          linkedin: linkedinUrl,
          ownedChannels: channels,
          monetizationSignals: monetization,
          engagementSignals: engagement,
          tripFitSignals: tripFit,
          score: breakdown.total,
          scoreBreakdown: breakdown,
          status: "new",
          source: "google",
          raw: item,
          runId,
          communityId,
          leaderId,
        };

        await storage.createLead(leadData);
        createdCount++;
      } catch (err: any) {
        await appendAndSave(`[ERROR] Website lead processing failed: ${err.message}`);
      }
    }

    await storage.updateRun(runId, { leadsExtracted: createdCount });

    await appendAndSave(`Created ${createdCount} total leads`, 80, "Step 8: Contact enrichment");
    await markStepComplete(runId, PIPELINE_STEPS.LEAD_CREATION);
    await clearCheckpoint(runId);

    const runLeads = await storage.listLeadsByRun(runId);
    const leadsToEnrich = runLeads
      .filter((l) => !l.email)
      .sort((a, b) => (b.score || 0) - (a.score || 0));
    let enrichedCount = 0;

    if (params.enableApollo !== false) {
      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        await appendAndSave(`Apollo.io: enriching ${leadsToEnrich.length} leads without email...`);

        let apolloSkipped = 0;
        let apolloCalls = 0;
        let apolloDeduped = 0;
        for (const lead of leadsToEnrich) {
          try {
            const currentHash = computeApolloInputHash(lead);
            if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) {
              apolloDeduped++;
              continue;
            }

            const leaderName = lead.leaderName || lead.communityName || "";
            if (!leaderName) continue;

            if (!isValidApolloCandidate(leaderName)) {
              apolloSkipped++;
              continue;
            }

            const nameParts = leaderName.trim().split(/\s+/);
            const firstName = nameParts[0] || "";
            const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
            const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;

            const channels = (lead.ownedChannels as Record<string, string>) || {};
            let domain = apolloExtractDomain(lead.website || "");
            if (!domain || !apolloIsEnrichable(domain)) {
              if (channels.website) {
                domain = apolloExtractDomain(channels.website);
              }
            }
            const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;

            const linkedinUrl = channels.linkedin && channels.linkedin.startsWith("http") ? channels.linkedin : undefined;

            log(`[APOLLO] Lead ${lead.id} "${leaderName}": domain=${enrichableDomain || "none"}, linkedin=${linkedinUrl ? "yes" : "no"}, hasRealName=${hasRealName}`, "apollo");

            apolloCalls++;
            let result = await apolloPersonMatch({
              name: leaderName,
              firstName: hasRealName ? firstName : undefined,
              lastName: hasRealName ? lastName : undefined,
              domain: enrichableDomain,
              organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
              linkedinUrl,
            });

            if (!result && hasRealName && !linkedinUrl) {
              apolloCalls++;
              result = await apolloPersonMatch({
                firstName,
                lastName,
                domain: enrichableDomain,
              });
              await new Promise((r) => setTimeout(r, 300));
            }

            if (!result) {
              log(`[APOLLO] Lead ${lead.id} "${leaderName}": no match found`, "apollo");
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
              continue;
            }

            log(`[APOLLO] Lead ${lead.id} "${leaderName}": MATCH email=${result.email ? "yes" : "no"} linkedin=${result.linkedin ? "yes" : "no"}`, "apollo");

            const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };

            if (result.email) updateData.email = result.email;
            if (result.phone && !lead.phone) updateData.phone = result.phone;
            if (result.linkedin && !lead.linkedin) updateData.linkedin = result.linkedin;
            if (result.location && !lead.location) updateData.location = result.location;
            if (!lead.leaderName && result.fullName) updateData.leaderName = result.fullName;

            const existingChannels = (lead.ownedChannels as Record<string, string>) || {};
            const updatedChannels = { ...existingChannels };
            if (result.twitter && !existingChannels.twitter) updatedChannels.twitter = result.twitter;
            if (result.facebook && !existingChannels.facebook) updatedChannels.facebook = result.facebook;
            if (result.linkedin && !existingChannels.linkedin) updatedChannels.linkedin = result.linkedin;
            if (Object.keys(updatedChannels).length > Object.keys(existingChannels).length) {
              updateData.ownedChannels = updatedChannels;
            }

            if (Object.keys(updateData).length <= 2) {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
              continue;
            }

            const breakdown = scoreLead({
              name: lead.communityName || "",
              description: "",
              type: lead.communityType || "",
              location: updateData.location || lead.location || "",
              website: lead.website || "",
              email: updateData.email || lead.email || "",
              phone: updateData.phone || lead.phone || "",
              linkedin: updateData.linkedin || lead.linkedin || "",
              ownedChannels: updateData.ownedChannels || existingChannels,
              monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
              engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
              tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
              leaderName: updateData.leaderName || lead.leaderName || "",
              memberCount: (lead.engagementSignals as any)?.member_count || 0,
              subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
              raw: (lead.raw as Record<string, any>) || {},
              emailValidation: lead.emailValidation || "",
            });

            updateData.score = breakdown.total;
            updateData.scoreBreakdown = breakdown;

            await storage.updateLead(lead.id, updateData);
            enrichedCount++;

            await new Promise((r) => setTimeout(r, 300));
          } catch (err: any) {
            await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
          }
        }

        await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped invalid names, ${apolloDeduped} already enriched/unchanged, ${apolloCalls} API calls used)`);
      } else {
        await appendAndSave("Apollo enrichment: skipped (no API key configured)");
      }
    } else {
      await appendAndSave("Apollo enrichment skipped (disabled by user)");
    }
    await markStepComplete(runId, PIPELINE_STEPS.APOLLO);

    const LEADS_FINDER_MAX_PER_RUN = Infinity;
    const refreshedLeads = await storage.listLeadsByRun(runId);
    const leadsForFinder = refreshedLeads
      .filter((l) => !l.email)
      .filter((l) => {
        const channels = (l.ownedChannels as Record<string, string>) || {};
        const websiteUrl = channels.website || l.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        return domain && isEnrichableDomain(domain);
      })
      .sort((a, b) => (b.score || 0) - (a.score || 0))
      .slice(0, LEADS_FINDER_MAX_PER_RUN);

    if (leadsForFinder.length > 0) {
      await appendAndSave(`Leads Finder: enriching ${leadsForFinder.length} leads by domain...`, 87, "Step 9: Leads Finder enrichment");
      let finderEnriched = 0;

      const domains = Array.from(new Set(
        leadsForFinder.map((l) => {
          const ch = (l.ownedChannels as Record<string, string>) || {};
          const websiteUrl = ch.website || l.website || "";
          return extractDomainFromUrl(websiteUrl);
        }).filter((d) => d && isEnrichableDomain(d))
      ));

      if (domains.length > 0) {
        try {
          const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", {
            company_domain: domains,
            email_status: ["validated"],
            fetch_count: Math.min(domains.length * 5, 200),
          }, 120000, 0.0015);
          await storage.incrementApifySpend(runId, actorCost);

          const emailByDomain = new Map<string, any>();
          for (const r of finderResults) {
            const email = r.email || r.work_email || r.personal_email || "";
            if (!email || isBlockedEmail(email)) continue;
            const domain = r.company_domain || r.domain || "";
            if (domain && !emailByDomain.has(domain.toLowerCase())) {
              emailByDomain.set(domain.toLowerCase(), r);
            }
          }

          for (const lead of leadsForFinder) {
            const ch = (lead.ownedChannels as Record<string, string>) || {};
            const websiteUrl = ch.website || lead.website || "";
            const domain = extractDomainFromUrl(websiteUrl);
            if (!domain) continue;

            const match = emailByDomain.get(domain.toLowerCase());
            if (!match) continue;

            const email = match.email || match.work_email || match.personal_email || "";
            if (!email || isBlockedEmail(email)) continue;

            const updateData: Record<string, any> = { email };
            if (match.phone && !lead.phone) updateData.phone = match.phone;
            if (match.linkedin_url && !lead.linkedin) updateData.linkedin = match.linkedin_url;
            if (!lead.leaderName && match.first_name && match.last_name) {
              updateData.leaderName = `${match.first_name} ${match.last_name}`;
            }

            const breakdown = scoreLead({
              name: lead.communityName || "",
              description: "",
              type: lead.communityType || "",
              location: lead.location || "",
              website: lead.website || "",
              email: updateData.email || lead.email || "",
              phone: updateData.phone || lead.phone || "",
              linkedin: updateData.linkedin || lead.linkedin || "",
              ownedChannels: ch,
              monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
              engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
              tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
              leaderName: updateData.leaderName || lead.leaderName || "",
              memberCount: (lead.engagementSignals as any)?.member_count || 0,
              subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
              raw: (lead.raw as Record<string, any>) || {},
              emailValidation: lead.emailValidation || "",
            });

            updateData.score = breakdown.total;
            updateData.scoreBreakdown = breakdown;

            await storage.updateLead(lead.id, updateData);
            finderEnriched++;
          }

          await appendAndSave(`Leads Finder: enriched ${finderEnriched} leads from ${domains.length} domains`);
          enrichedCount += finderEnriched;
        } catch (err: any) {
          if (err.costUsd) {
            await storage.incrementApifySpend(runId, err.costUsd);
          }
          await appendAndSave(`[WARN] Leads Finder failed: ${err.message}`);
        }
      }
    }

    await markStepComplete(runId, PIPELINE_STEPS.LEADS_FINDER);

    if (process.env.MILLIONVERIFIER_API_KEY) {
      await appendAndSave("Validating emails...", 90, "Step 10: Email validation");
      if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);

      const allLeadsForValidation = await storage.listLeadsByRun(runId);
      const leadsWithEmail = allLeadsForValidation.filter(l => l.email && !l.emailValidation);

      if (leadsWithEmail.length > 0) {
        await appendAndSave(`Email validation: verifying ${leadsWithEmail.length} emails via MillionVerifier...`);

        const emailsToVerify = leadsWithEmail.map(l => ({ email: l.email!, leadId: l.id }));

        try {
          const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
            if (verified % 20 === 0 || verified === total) {
              await appendAndSave(`Email validation: ${verified}/${total} verified...`);
            }
          });

          let validCount = 0, invalidCount = 0, catchAllCount = 0, unknownCount = 0;

          for (const [leadId, result] of Array.from(results.entries())) {
            const validation = mapResultToValidation(result.result);
            await storage.updateLead(leadId, { emailValidation: validation });
            if (validation === "valid") validCount++;
            else if (validation === "invalid") invalidCount++;
            else if (validation === "catch-all") catchAllCount++;
            else unknownCount++;
          }

          await appendAndSave(
            `Email validation complete: ${validCount} valid, ${invalidCount} invalid, ${catchAllCount} catch-all, ${unknownCount} unknown`
          );
        } catch (err: any) {
          await appendAndSave(`[WARN] Email validation failed: ${err.message}`);
        }
      } else {
        await appendAndSave("Email validation: no new emails to verify");
      }
    }

    await markStepComplete(runId, PIPELINE_STEPS.EMAIL_VALIDATION);

    if (isHubspotConfigured()) {
      await appendAndSave("Checking HubSpot for existing contacts...", 91, "HubSpot CRM check");
      try {
        const allRunLeads = await storage.listLeadsByRun(runId);
        const leadsWithValidEmail = allRunLeads.filter(l => l.email && l.emailValidation === "valid");
        if (leadsWithValidEmail.length > 0) {
          const emailMap = new Map(leadsWithValidEmail.map(l => [l.email!.toLowerCase(), l.id]));
          const emails = Array.from(emailMap.keys());
          const hubspotResults = await checkEmailsInHubspot(emails);
          let existingCount = 0;
          let netNewCount = 0;
          for (const entry of Array.from(hubspotResults.entries())) {
            const leadId = emailMap.get(entry[0].toLowerCase());
            if (leadId) {
              const status = entry[1] ? "existing" : "net_new";
              if (entry[1]) existingCount++; else netNewCount++;
              await storage.updateLead(leadId, { hubspotStatus: status });
            }
          }
          await appendAndSave(`HubSpot check: ${existingCount} existing, ${netNewCount} net new out of ${leadsWithValidEmail.length} valid emails`);
        } else {
          await appendAndSave("HubSpot check: no valid emails to check");
        }
      } catch (err: any) {
        await appendAndSave(`[WARN] HubSpot check failed: ${err.message}`);
      }
    }

    await appendAndSave("Finalizing...", 92, "Step 11: Finalizing");

    const emailCount = await storage.countLeadsByRunWithEmail(runId);
    const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);

    await storage.updateRun(runId, {
      leadsWithEmail: emailCount,
      leadsWithValidEmail: validEmailCount,
    });

    const sourcesUsed = (params.enabledSources || []).map((s: string) => {
      const labels: Record<string, string> = { patreon: "Patreon", meetup: "Meetup", youtube: "YouTube", reddit: "Reddit", eventbrite: "Eventbrite", facebook: "Facebook", google: "Google Search" };
      return labels[s] || s;
    });

    await appendAndSave(
      `Scoring complete: ${createdCount} leads, ${emailCount} with email (${validEmailCount} valid)`,
      96,
      "Step 11: Finalizing"
    );

    await markStepComplete(runId, PIPELINE_STEPS.SCORING);

    if (isAutonomousRun && globalEmailTarget > 0 && validEmailCount < globalEmailTarget) {
      let expansionRound = 0;
      const usedExpansionKeywords = new Set<string>();

      const EXPANSION_SUFFIX_POOLS = [
        ["community leader", "group organizer"],
        ["travel group", "adventure community"],
        ["retreat host", "trip organizer"],
        ["club founder", "meetup organizer"],
        ["group admin", "community founder"],
        ["event organizer", "workshop host"],
        ["group leader near me", "community events"],
        ["online community", "membership group"],
      ];

      while (true) {
        if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);

        const budgetInfo = await getRunBudgetInfo(runId);
        const remainingBudget = budgetUsd - budgetInfo.spentUsd;
        const currentValidCount = await storage.countLeadsByRunWithValidEmail(runId);
        await storage.updateRun(runId, { leadsWithValidEmail: currentValidCount });
        const deficit = globalEmailTarget - currentValidCount;

        if (await isValidEmailTargetReached(runId)) {
          await appendAndSave(`Target reached: ${currentValidCount}/${globalEmailTarget} valid emails.`);
          break;
        }
        if (remainingBudget <= 0.50) {
          await appendAndSave(`Budget exhausted: ${currentValidCount}/${globalEmailTarget} valid emails. $${budgetInfo.spentUsd.toFixed(2)}/$${budgetUsd.toFixed(2)} spent.`);
          break;
        }
        if (deficit <= 3) {
          await appendAndSave(`Near target: ${currentValidCount}/${globalEmailTarget} valid emails. Deficit too small for expansion.`);
          break;
        }

        expansionRound++;
        await appendAndSave(
          `Expansion round ${expansionRound}: ${currentValidCount}/${globalEmailTarget} valid emails (${deficit} short). $${remainingBudget.toFixed(2)} remaining. Running deeper discovery + enrichment...`,
          93,
          `Expansion round ${expansionRound}`
        );

        const suffixPoolIndex = Math.min(expansionRound - 1, EXPANSION_SUFFIX_POOLS.length - 1);
        const expandSuffixes = EXPANSION_SUFFIX_POOLS[suffixPoolIndex];
        const expandedKws = keywords.flatMap(kw => expandSuffixes.map(s => `${kw} ${s}`));
        const uniqueExpKws = Array.from(new Set(expandedKws))
          .filter(kw => !usedExpansionKeywords.has(kw))
          .slice(0, 8);

        if (uniqueExpKws.length === 0) {
          await appendAndSave(`Expansion: all keyword variations exhausted after ${expansionRound - 1} rounds. ${currentValidCount}/${globalEmailTarget} valid emails.`);
          break;
        }
        for (const kw of uniqueExpKws) usedExpansionKeywords.add(kw);

        let newPlatformLeads: PlatformLead[] = [];

        for (const platform of enabledSources) {
          if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
          if (await isBudgetExhausted(runId, 0.10)) {
            await appendAndSave(`Expansion: budget limit reached, stopping discovery.`);
            break;
          }

          const maxExpLeads = Math.min(Math.ceil(deficit * 4), 500);
          try {
            if (platform === "patreon") {
              await appendAndSave(`Expansion: deeper Patreon search (${uniqueExpKws.length} queries)...`);
              const results = await scrapePatreonCreators(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                minPostCount: params.minPostCount || 0,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "facebook") {
              await appendAndSave(`Expansion: deeper Facebook search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeFacebookGroups(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                geos,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "substack") {
              await appendAndSave(`Expansion: deeper Substack search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeSubstackWriters(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg));
              newPlatformLeads.push(...results);
            } else if (platform === "mighty") {
              await appendAndSave(`Expansion: deeper Mighty Networks search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeMightyNetworks(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), geos);
              newPlatformLeads.push(...results);
            } else if (platform === "linkedin") {
              await appendAndSave(`Expansion: deeper LinkedIn Groups search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeLinkedInGroups(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                geos,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "podcast" && run.podcastEnabled !== false) {
              await appendAndSave(`Expansion: deeper podcast search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeApplePodcasts(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minEpisodeCount: params.minEpisodeCount || 0,
                podcastCountry: params.podcastCountry || "US",
              });
              newPlatformLeads.push(...results);
            }
          } catch (err: any) {
            await appendAndSave(`Expansion: ${platform} search error: ${err.message}`);
          }
        }

        await appendAndSave(`Expansion: discovered ${newPlatformLeads.length} new platform results`);

        if (newPlatformLeads.length === 0) {
          await appendAndSave(`Expansion: no new leads discovered, lead pool exhausted. Stopping expansion.`);
          break;
        }

        // --- Enrichment chain on PlatformLead[] before creating DB leads ---

        // Step E0.5: LinkedIn group page scrape for admin names
        const expLinkedInNoName = newPlatformLeads.filter(l => l.source === "linkedin" && !l.leaderName && l.ownedChannels?.linkedin_group);
        if (expLinkedInNoName.length > 0 && !(await isBudgetExhausted(runId, 0.05))) {
          try { await enrichLinkedInGroupPages(runId, newPlatformLeads, appendAndSave); } catch {}
        }

        // Step E1: Google Bridge for Facebook/LinkedIn groups
        const expFbLeads = newPlatformLeads.filter(l => (l.source === "facebook" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
        if (expFbLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          await appendAndSave(`Expansion: Google Bridge for ${expFbLeads.length} Facebook/LinkedIn groups...`);
          try {
            await googleBridgeEnrichFacebookGroups(runId, newPlatformLeads, appendAndSave);
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion Google Bridge failed: ${err.message}`);
          }
        }

        // Step E1b: RSS feed extraction
        const expRssLeads = newPlatformLeads.filter(l => l.ownedChannels?.rss && l.ownedChannels.rss.startsWith("http") && !l.email);
        if (expRssLeads.length > 0) {
          await appendAndSave(`Expansion: RSS feed extraction for ${expRssLeads.length} leads...`);
          try {
            await enrichFromRssFeeds(runId, newPlatformLeads, appendAndSave);
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion RSS feed extraction failed: ${err.message}`);
          }
        }

        // Step E1c: Link aggregator scrape (pass 1)
        const expLinktreeLeads = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http"));
        if (expLinktreeLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          await appendAndSave(`Expansion: Link aggregator scrape for ${expLinktreeLeads.length} leads...`);
          try {
            await enrichFromLinkAggregators(runId, newPlatformLeads, appendAndSave);
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion link aggregator scrape failed: ${err.message}`);
          }
        }

        // Step E2: Social scraping (YouTube, Instagram, Twitter) — budget-gated
        if (!(await isBudgetExhausted(runId, 0.10))) {
          const expSocialTasks: { name: string; promise: Promise<void> }[] = [];

          const expYtLeads = newPlatformLeads.filter(l => l.ownedChannels?.youtube && l.ownedChannels.youtube.startsWith("http"));
          if (expYtLeads.length > 0) {
            await appendAndSave(`Expansion: YouTube about pages for ${expYtLeads.length} leads...`);
            expSocialTasks.push({ name: "YouTube", promise: enrichFromYouTubeAboutPages(runId, newPlatformLeads, appendAndSave) });
          }

          const expIgLeads = newPlatformLeads.filter(l => l.ownedChannels?.instagram && l.ownedChannels.instagram.startsWith("http"));
          if (expIgLeads.length > 0) {
            await appendAndSave(`Expansion: Instagram bios for ${expIgLeads.length} leads...`);
            expSocialTasks.push({ name: "Instagram", promise: enrichFromInstagramBios(runId, newPlatformLeads, appendAndSave) });
          }

          const expTwLeads = newPlatformLeads.filter(l => {
            const tw = l.ownedChannels?.twitter;
            return tw && (tw.startsWith("http") || tw.startsWith("@"));
          });
          if (expTwLeads.length > 0) {
            await appendAndSave(`Expansion: Twitter bios for ${expTwLeads.length} leads...`);
            expSocialTasks.push({ name: "Twitter", promise: enrichFromTwitterBios(runId, newPlatformLeads, appendAndSave) });
          }

          if (expSocialTasks.length > 0) {
            await appendAndSave(`Expansion: running ${expSocialTasks.length} social scrape tasks in parallel...`);
            const socialResults = await Promise.allSettled(expSocialTasks.map(t => t.promise));
            for (let i = 0; i < socialResults.length; i++) {
              if (socialResults[i].status === "rejected") {
                await appendAndSave(`[WARN] Expansion ${expSocialTasks[i].name} failed: ${(socialResults[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
              }
            }
          }

          // Step E2d: Link aggregator pass 2 (new URLs from social scraping)
          const expNewLinktreeLeads = newPlatformLeads.filter(l =>
            !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http") &&
            !expLinktreeLeads.includes(l)
          );
          if (expNewLinktreeLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
            await appendAndSave(`Expansion: Link aggregator pass 2 for ${expNewLinktreeLeads.length} new URLs...`);
            try {
              await enrichFromLinkAggregators(runId, newPlatformLeads, appendAndSave);
            } catch (err: any) {
              await appendAndSave(`[WARN] Expansion link aggregator pass 2 failed: ${err.message}`);
            }
          }
        }

        // Step E2e: Google Contact Search for leads without contact info
        const expNoContactLeads = newPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
        if (expNoContactLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          await appendAndSave(`Expansion: Google contact search for ${expNoContactLeads.length} leads...`);
          try {
            await googleSearchEnrichCreators(runId, newPlatformLeads, appendAndSave);
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion Google contact search failed: ${err.message}`);
          }
        }

        // Step E2f: Slug domain probe for Patreon creators without websites
        let expSlugProbed = 0;
        for (const pl of newPlatformLeads) {
          if (pl.ownedChannels?.website || pl.email) continue;
          const patreonUrl = pl.website || pl.ownedChannels?.patreon || "";
          if (!patreonUrl.includes("patreon.com/")) continue;
          const slug = patreonUrl.split("patreon.com/")[1]?.split(/[?#/]/)[0]?.toLowerCase();
          if (!slug || slug.length < 3 || (slug.startsWith("u") && /^\d+$/.test(slug.slice(1)))) continue;
          if (/[^a-z0-9_-]/.test(slug)) continue;
          const cleanSlug = slug.replace(/[_-]/g, "");
          if (cleanSlug.length < 4) continue;
          const candidateDomain = `${cleanSlug}.com`;
          if (!pl.ownedChannels) pl.ownedChannels = {} as Record<string, string>;
          pl.ownedChannels.website = `https://${candidateDomain}`;
          expSlugProbed++;
        }
        if (expSlugProbed > 0) {
          await appendAndSave(`Expansion: slug domain probe tried ${expSlugProbed} Patreon slugs as .com domains`);
        }

        // Step E3: Website contact crawl for leads with websites but no email
        const expWebsiteLeads = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
        if (expWebsiteLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          await appendAndSave(`Expansion: crawling ${expWebsiteLeads.length} websites for contact emails...`);
          try {
            const websiteEmailMap = await crawlCreatorWebsitesForEmails(runId, expWebsiteLeads, appendAndSave);
            let expWebEmails = 0;
            for (const pl of newPlatformLeads) {
              if (!pl.email && pl.ownedChannels?.website) {
                const domain = extractDomain(pl.ownedChannels.website);
                if (domain && websiteEmailMap.has(domain)) {
                  pl.email = websiteEmailMap.get(domain)!;
                  expWebEmails++;
                }
              }
            }
            await appendAndSave(`Expansion: website crawl found ${expWebEmails} emails from contact pages`);
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion website crawl failed: ${err.message}`);
          }
        }

        const expEmailsAfterEnrichment = newPlatformLeads.filter(l => l.email).length;
        await appendAndSave(`Expansion: after enrichment chain, ${expEmailsAfterEnrichment}/${newPlatformLeads.length} leads have emails`);

        // --- Now create DB leads from enriched PlatformLeads ---
        let expansionCreated = 0;
        for (const pl of newPlatformLeads) {
          try {
            let existingLead = null;
            if (pl.email) existingLead = await storage.findLeadByEmail(pl.email);
            if (!existingLead && pl.website) existingLead = await storage.findLeadByWebsite(pl.website);
            if (!existingLead && pl.communityName) {
              existingLead = await storage.findLeadByNameAndLocation(pl.communityName, pl.location);
            }
            if (existingLead) continue;

            const community = await storage.createCommunity({
              name: pl.communityName,
              type: pl.communityType,
              description: pl.description,
              website: pl.website,
              ownedChannels: pl.ownedChannels,
              eventCadence: pl.engagementSignals,
              audienceSignals: pl.tripFitSignals,
              sourceUrls: [pl.website],
            });

            let leaderId: number | undefined;
            if (pl.leaderName) {
              const leader = await storage.createLeader({
                name: pl.leaderName, role: "", email: pl.email,
                phone: pl.phone, sourceUrl: pl.website, communityId: community.id,
              });
              leaderId = leader.id;
            }

            const breakdown = scoreLead({
              name: pl.communityName, description: pl.description, type: pl.communityType,
              location: pl.location, website: pl.website, email: pl.email, phone: pl.phone,
              linkedin: pl.ownedChannels?.linkedin || "", ownedChannels: pl.ownedChannels,
              monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals,
              tripFitSignals: pl.tripFitSignals, leaderName: pl.leaderName,
              memberCount: pl.memberCount, subscriberCount: pl.subscriberCount, raw: pl.raw,
            });

            await storage.createLead({
              leadType: pl.leaderName ? "leader" : "community",
              communityName: pl.communityName, communityType: pl.communityType,
              leaderName: pl.leaderName,
              firstName: extractHighConfidenceFirstName(pl.leaderName),
              researchSummary: generateResearchSummary({ communityName: pl.communityName, communityType: pl.communityType, leaderName: pl.leaderName, description: pl.description, source: pl.source, website: pl.website, location: pl.location, ownedChannels: pl.ownedChannels, engagementSignals: pl.engagementSignals, monetizationSignals: pl.monetizationSignals, tripFitSignals: pl.tripFitSignals, raw: pl.raw }),
              location: pl.location, website: pl.website,
              email: pl.email, phone: pl.phone, ownedChannels: pl.ownedChannels,
              monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals,
              tripFitSignals: pl.tripFitSignals, score: breakdown.total, scoreBreakdown: breakdown,
              status: "new", source: pl.source || "", raw: pl.raw, runId,
              communityId: community.id, leaderId,
            });
            expansionCreated++;
          } catch {}
        }

        await appendAndSave(`Expansion: created ${expansionCreated} new leads (deduped from ${newPlatformLeads.length})`);

        if (expansionCreated === 0) {
          await appendAndSave(`Expansion: all leads were duplicates, stopping expansion.`);
          break;
        }

        // --- Apollo enrichment on newly created DB leads ---
        if (params.enableApollo !== false && isApolloAvailable() && !(await isBudgetExhausted(runId, 0.10))) {
          const expDbLeads = await storage.listLeadsByRun(runId);
          const expApolloLeads = expDbLeads
            .filter(l => !l.email && !l.apolloEnrichedAt)
            .sort((a, b) => (b.score || 0) - (a.score || 0));

          if (expApolloLeads.length > 0) {
            await appendAndSave(`Expansion: Apollo enrichment for ${expApolloLeads.length} leads without email...`);
            let expApolloEnriched = 0;
            let expApolloCalls = 0;
            for (const lead of expApolloLeads) {
              try {
                if (await isBudgetExhausted(runId, 0.05)) break;
                const currentHash = computeApolloInputHash(lead);
                if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) continue;

                const leaderName = lead.leaderName || lead.communityName || "";
                if (!leaderName) continue;
                if (!isValidApolloCandidate(leaderName)) continue;

                const nameParts = leaderName.trim().split(/\s+/);
                const firstName = nameParts[0] || "";
                const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
                const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;

                const channels = (lead.ownedChannels as Record<string, string>) || {};
                let domain = apolloExtractDomain(lead.website || "");
                if (!domain || !apolloIsEnrichable(domain)) {
                  if (channels.website) domain = apolloExtractDomain(channels.website);
                }
                const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;
                const linkedinUrl = channels.linkedin && channels.linkedin.startsWith("http") ? channels.linkedin : undefined;

                expApolloCalls++;
                let result = await apolloPersonMatch({
                  name: leaderName,
                  firstName: hasRealName ? firstName : undefined,
                  lastName: hasRealName ? lastName : undefined,
                  domain: enrichableDomain,
                  organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
                  linkedinUrl,
                });

                if (!result && hasRealName && !linkedinUrl) {
                  expApolloCalls++;
                  result = await apolloPersonMatch({ firstName, lastName, domain: enrichableDomain });
                  await new Promise((r) => setTimeout(r, 300));
                }

                if (!result) {
                  await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
                  continue;
                }

                const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };
                if (result.email && !isBlockedEmail(result.email)) {
                  updateData.email = result.email;
                  expApolloEnriched++;
                }
                if (result.phone) updateData.phone = result.phone;
                if (result.linkedin) updateData.linkedin = result.linkedin;
                if (result.title) updateData.title = result.title;
                if (result.orgName) updateData.company = result.orgName;
                await storage.updateLead(lead.id, updateData);
                await new Promise((r) => setTimeout(r, 300));
              } catch (err: any) {
                await appendAndSave(`[WARN] Expansion Apollo failed for lead ${lead.id}: ${err.message}`);
              }
            }
            await appendAndSave(`Expansion: Apollo enriched ${expApolloEnriched} of ${expApolloLeads.length} leads (${expApolloCalls} API calls)`);
          }
        }

        // --- Leads Finder fallback for leads still missing email ---
        const refreshedExpLeads = await storage.listLeadsByRun(runId);
        const leadsForExpFinder = refreshedExpLeads
          .filter(l => !l.email && !l.emailValidation)
          .filter(l => {
            const ch = (l.ownedChannels as Record<string, string>) || {};
            const websiteUrl = ch.website || l.website || "";
            const domain = extractDomainFromUrl(websiteUrl);
            return domain && isEnrichableDomain(domain);
          });

        if (leadsForExpFinder.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          await appendAndSave(`Expansion: enriching ${leadsForExpFinder.length} leads via Leads Finder (batched)...`);

          const expDomains = Array.from(new Set(
            leadsForExpFinder.map(l => {
              const ch = (l.ownedChannels as Record<string, string>) || {};
              return extractDomainFromUrl(ch.website || l.website || "");
            }).filter((d): d is string => !!d && isEnrichableDomain(d))
          ));

          if (expDomains.length > 0) {
            try {
              const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", {
                company_domain: expDomains,
                email_status: ["validated"],
                fetch_count: Math.min(expDomains.length * 5, 200),
              }, 120000, 0.0015);
              await storage.incrementApifySpend(runId, actorCost);

              const emailByDomain = new Map<string, any>();
              for (const r of finderResults) {
                const email = r.email || r.work_email || r.personal_email || "";
                if (!email || isBlockedEmail(email)) continue;
                const domain = r.company_domain || r.domain || "";
                if (domain && !emailByDomain.has(domain.toLowerCase())) {
                  emailByDomain.set(domain.toLowerCase(), r);
                }
              }

              let expEnriched = 0;
              for (const lead of leadsForExpFinder) {
                const ch = (lead.ownedChannels as Record<string, string>) || {};
                const websiteUrl = ch.website || lead.website || "";
                const domain = extractDomainFromUrl(websiteUrl);
                if (!domain) continue;
                const match = emailByDomain.get(domain.toLowerCase());
                if (!match) continue;
                const email = match.email || match.work_email || match.personal_email || "";
                if (!email || isBlockedEmail(email)) continue;

                const updateData: Record<string, any> = { email };
                if (match.phone && !lead.phone) updateData.phone = match.phone;
                if (match.linkedin_url && !lead.linkedin) updateData.linkedin = match.linkedin_url;
                if (!lead.leaderName && match.first_name && match.last_name) {
                  updateData.leaderName = `${match.first_name} ${match.last_name}`;
                }
                await storage.updateLead(lead.id, updateData);
                expEnriched++;
              }

              await appendAndSave(`Expansion: Leads Finder enriched ${expEnriched} of ${leadsForExpFinder.length} leads from ${expDomains.length} domains`);
            } catch (err: any) {
              if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
              await appendAndSave(`Expansion: Leads Finder error: ${err.message}`);
            }
          }
        }

        const expLeadsForValidation = (await storage.listLeadsByRun(runId))
          .filter(l => l.email && !l.emailValidation);

        if (expLeadsForValidation.length > 0 && process.env.MILLIONVERIFIER_API_KEY) {
          await appendAndSave(`Expansion: validating ${expLeadsForValidation.length} newly found emails...`);
          try {
            const emailsToVerify = expLeadsForValidation.map(l => ({ email: l.email!, leadId: l.id }));
            const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
              if (verified % 20 === 0 || verified === total) {
                await appendAndSave(`Expansion validation: ${verified}/${total} verified...`);
              }
            });

            let expValid = 0, expInvalid = 0;
            for (const [leadId, result] of Array.from(results.entries())) {
              const validation = mapResultToValidation(result.result);
              await storage.updateLead(leadId, { emailValidation: validation });
              if (validation === "valid" || validation === "catch-all") expValid++;
              else expInvalid++;
            }
            await appendAndSave(`Expansion validation: ${expValid} valid, ${expInvalid} invalid`);
          } catch (err: any) {
            await appendAndSave(`Expansion validation error: ${err.message}`);
          }
        }

        if (isHubspotConfigured()) {
          try {
            const expHubLeads = (await storage.listLeadsByRun(runId)).filter(l => l.email && l.emailValidation === "valid" && !l.hubspotStatus);
            if (expHubLeads.length > 0) {
              await appendAndSave(`Expansion: checking ${expHubLeads.length} new valid emails against HubSpot...`);
              const emailMap = new Map(expHubLeads.map(l => [l.email!.toLowerCase(), l.id]));
              const hubResults = await checkEmailsInHubspot(Array.from(emailMap.keys()));
              let existing = 0, netNew = 0;
              for (const [email, isExisting] of Array.from(hubResults.entries())) {
                const leadId = emailMap.get(email.toLowerCase());
                if (leadId) {
                  await storage.updateLead(leadId, { hubspotStatus: isExisting ? "existing" : "net_new" });
                  if (isExisting) existing++; else netNew++;
                }
              }
              await appendAndSave(`Expansion HubSpot: ${existing} existing, ${netNew} net new`);
            }
          } catch (err: any) {
            await appendAndSave(`[WARN] Expansion HubSpot check failed: ${err.message}`);
          }
        }

        const postExpansionValid = await storage.countLeadsByRunWithValidEmail(runId);
        const postExpansionEmail = await storage.countLeadsByRunWithEmail(runId);
        await storage.updateRun(runId, {
          leadsWithEmail: postExpansionEmail,
          leadsWithValidEmail: postExpansionValid,
          leadsExtracted: (await storage.listLeadsByRun(runId)).length,
        });

        await appendAndSave(
          `Expansion round ${expansionRound} complete: ${postExpansionValid}/${globalEmailTarget} valid emails (${postExpansionEmail} total emails, ${(await storage.listLeadsByRun(runId)).length} total leads).`,
          95
        );

        if (await isValidEmailTargetReached(runId)) {
          await appendAndSave(`Target reached after expansion: ${postExpansionValid}/${globalEmailTarget} valid emails.`);
          break;
        }
      }
    }

    const finalEmailCount = await storage.countLeadsByRunWithEmail(runId);
    const finalValidCount = await storage.countLeadsByRunWithValidEmail(runId);
    await storage.updateRun(runId, {
      leadsWithEmail: finalEmailCount,
      leadsWithValidEmail: finalValidCount,
    });

    let budgetSummary = "";
    if (isAutonomousRun && budgetUsd > 0) {
      const finalBudget = await getRunBudgetInfo(runId);
      budgetSummary = ` Budget: $${finalBudget.spentUsd.toFixed(2)}/$${budgetUsd.toFixed(2)} spent.`;
    }

    const validSummary = finalValidCount > 0 ? ` (${finalValidCount} valid)` : "";

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Complete",
      finishedAt: new Date(),
      logs: appendLog(
        currentLogs,
        `Pipeline complete! ${createdCount} leads discovered, ${finalEmailCount} with email${validSummary}.${budgetSummary} Sources: ${sourcesUsed.join(", ")}.`
      ),
    });

    log(`Pipeline run ${runId} completed successfully`, "pipeline");
  } catch (err: any) {
    if (err instanceof RunCancelledError || cancelledRunIds.has(runId)) {
      log(`Pipeline run ${runId} stopped by user`, "pipeline");
      const emailCount = await storage.countLeadsByRunWithEmail(runId);
      const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);
      await storage.updateRun(runId, {
        status: "stopped",
        step: "Stopped by user",
        leadsWithEmail: emailCount,
        leadsWithValidEmail: validEmailCount,
        logs: appendLog(currentLogs, `Run stopped by user. ${emailCount} leads with email preserved.`),
        finishedAt: new Date(),
      });
    } else {
      log(`Pipeline run ${runId} failed: ${err.message}`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Failed",
        logs: appendLog(currentLogs, `[ERROR] Pipeline failed: ${err.message}`),
        finishedAt: new Date(),
      });
    }
  } finally {
    if (stopHeartbeat) stopHeartbeat();
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
  }
}

export async function reEnrichRun(runId: number): Promise<void> {
  const run = await storage.getRun(runId);
  if (!run) throw new Error(`Run ${runId} not found`);

  const params = run.params as RunParams;
  let currentLogs = "";
  let stopHeartbeat: (() => void) | null = null;

  const appendAndSave = async (msg: string, progress?: number, step?: string) => {
    if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
    currentLogs = appendLog(currentLogs, msg);
    const update: Record<string, any> = { logs: currentLogs };
    if (progress !== undefined) update.progress = progress;
    if (step) update.step = step;
    await storage.updateRun(runId, update);
  };

  try {
    activeRunIds.add(runId);
    stopHeartbeat = startHeartbeat(runId);

    const leads = await storage.listLeadsByRun(runId);
    if (leads.length === 0) {
      const msg = `Re-enrichment aborted: no leads found for run ${runId}. The original pipeline may have been interrupted before leads were created.`;
      log(msg, "pipeline");
      await storage.updateRun(runId, {
        status: "interrupted",
        step: "Re-enrichment aborted (no leads)",
        finishedAt: new Date(),
        logs: appendLog(run.logs || "", msg),
      });
      return;
    }

    currentLogs = appendLog(run.logs || "", "--- Re-enrichment started ---");
    await storage.updateRun(runId, {
      status: "running",
      progress: 0,
      step: "Re-enrichment: Loading leads",
      finishedAt: null,
      logs: currentLogs,
    });

    await appendAndSave(`Loaded ${leads.length} leads from run ${runId}`, 5, "Re-enrichment: Converting leads");

    const platformLeads: (PlatformLead & { dbId: number })[] = leads.map((lead) => ({
      dbId: lead.id,
      source: "patreon",
      communityName: lead.communityName || "",
      communityType: lead.communityType || "",
      description: "",
      location: lead.location || "",
      website: lead.website || "",
      email: lead.email || "",
      phone: lead.phone || "",
      leaderName: lead.leaderName || "",
      memberCount: (lead.engagementSignals as any)?.member_count || 0,
      subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
      ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
      monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
      engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
      tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
      raw: (lead.raw as Record<string, any>) || {},
    }));

    await appendAndSave(`Step 1: Updating leads in database...`, 25, "Re-enrichment: Saving results");
    let crawlUpdated = 0;
    for (const pl of platformLeads) {
      const original = leads.find((l) => l.id === pl.dbId);
      if (!original) continue;

      const updateData: Record<string, any> = {};
      const cleanedEmail = cleanEmail(pl.email);
      if (cleanedEmail && cleanedEmail !== original.email) updateData.email = cleanedEmail;
      if (pl.leaderName && pl.leaderName !== original.leaderName) updateData.leaderName = pl.leaderName;
      if (pl.phone && pl.phone !== original.phone) updateData.phone = pl.phone;

      const origChannels = (original.ownedChannels as Record<string, string>) || {};
      const newChannels = pl.ownedChannels || {};
      if (JSON.stringify(newChannels) !== JSON.stringify(origChannels)) {
        updateData.ownedChannels = { ...origChannels, ...newChannels };
      }

      if (Object.keys(updateData).length > 0) {
        await storage.updateLead(pl.dbId, updateData);
        crawlUpdated++;
      }
    }
    await appendAndSave(`Updated ${crawlUpdated} leads from data`, 45);

    if (params.enableApollo !== false) {
      const refreshedLeads = await storage.listLeadsByRun(runId);
      const leadsToEnrich = refreshedLeads
        .filter((l) => (!l.email || l.email === ""))
        .sort((a, b) => (b.score || 0) - (a.score || 0));

      await appendAndSave(`Step 2: Apollo enrichment for ${leadsToEnrich.length} leads without email...`, 50, "Re-enrichment: Apollo enrichment");

      let enrichedCount = 0;
      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        let apolloSkipped = 0;
        let apolloCalls = 0;
        let apolloDeduped = 0;
        for (const lead of leadsToEnrich) {
          try {
            const currentHash = computeApolloInputHash(lead);
            if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) {
              apolloDeduped++;
              continue;
            }

            const leaderName = lead.leaderName || lead.communityName || "";
            if (!leaderName) continue;

            if (!isValidApolloCandidate(leaderName)) {
              apolloSkipped++;
              continue;
            }

            const nameParts = leaderName.trim().split(/\s+/);
            const firstName = nameParts[0] || "";
            const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
            const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;

            const channels = (lead.ownedChannels as Record<string, string>) || {};
            let domain = apolloExtractDomain(lead.website || "");
            if (!domain || !apolloIsEnrichable(domain)) {
              if (channels.website) {
                domain = apolloExtractDomain(channels.website);
              }
            }
            const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;

            const linkedinUrl = channels.linkedin && channels.linkedin.startsWith("http") ? channels.linkedin : undefined;

            log(`[APOLLO RE] Lead ${lead.id} "${leaderName}": domain=${enrichableDomain || "none"}, linkedin=${linkedinUrl ? "yes" : "no"}, hasRealName=${hasRealName}`, "apollo");

            apolloCalls++;
            let result = await apolloPersonMatch({
              name: leaderName,
              firstName: hasRealName ? firstName : undefined,
              lastName: hasRealName ? lastName : undefined,
              domain: enrichableDomain,
              organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
              linkedinUrl,
            });

            if (!result && hasRealName && !linkedinUrl) {
              apolloCalls++;
              result = await apolloPersonMatch({
                firstName,
                lastName,
                domain: enrichableDomain,
              });
              await new Promise((r) => setTimeout(r, 300));
            }

            if (!result) {
              log(`[APOLLO RE] Lead ${lead.id} "${leaderName}": no match found`, "apollo");
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
              continue;
            }

            log(`[APOLLO RE] Lead ${lead.id} "${leaderName}": MATCH email=${result.email ? "yes" : "no"} linkedin=${result.linkedin ? "yes" : "no"}`, "apollo");

            const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };
            if (result.email) updateData.email = result.email;
            if (result.phone && !lead.phone) updateData.phone = result.phone;
            if (result.linkedin && !lead.linkedin) updateData.linkedin = result.linkedin;
            if (result.location && !lead.location) updateData.location = result.location;
            if (!lead.leaderName && result.fullName) updateData.leaderName = result.fullName;

            const existingChannels = (lead.ownedChannels as Record<string, string>) || {};
            const updatedChannels = { ...existingChannels };
            if (result.twitter && !existingChannels.twitter) updatedChannels.twitter = result.twitter;
            if (result.facebook && !existingChannels.facebook) updatedChannels.facebook = result.facebook;
            if (result.linkedin && !existingChannels.linkedin) updatedChannels.linkedin = result.linkedin;
            if (Object.keys(updatedChannels).length > Object.keys(existingChannels).length) {
              updateData.ownedChannels = updatedChannels;
            }

            if (Object.keys(updateData).length > 2) {
              await storage.updateLead(lead.id, updateData);
              enrichedCount++;
            } else {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
            }

            await new Promise((r) => setTimeout(r, 300));
          } catch (err: any) {
            await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
          }
        }
        await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped invalid names, ${apolloDeduped} unchanged/skipped, ${apolloCalls} API calls used)`, 70);
      } else {
        await appendAndSave("Apollo enrichment skipped (no API key configured)", 70);
      }
    } else {
      await appendAndSave("Apollo enrichment skipped (disabled by user)", 70);
    }

    const finderLeads = await storage.listLeadsByRun(runId);
    const leadsForFinderReEnrich = finderLeads
      .filter((l) => !l.email || l.email === "")
      .filter((l) => {
        const ch = (l.ownedChannels as Record<string, string>) || {};
        const websiteUrl = ch.website || l.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        return domain && isEnrichableDomain(domain);
      })
      .sort((a, b) => (b.score || 0) - (a.score || 0));

    if (leadsForFinderReEnrich.length > 0) {
      await appendAndSave(`Step 4: Leads Finder enrichment for ${leadsForFinderReEnrich.length} leads...`, 72, "Re-enrichment: Leads Finder");

      const domains = Array.from(new Set(
        leadsForFinderReEnrich.map((l) => {
          const ch = (l.ownedChannels as Record<string, string>) || {};
          return extractDomainFromUrl(ch.website || l.website || "");
        }).filter((d) => d && isEnrichableDomain(d))
      ));

      if (domains.length > 0) {
        try {
          const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", {
            company_domain: domains,
            email_status: ["validated"],
            fetch_count: Math.min(domains.length * 5, 200),
          }, 120000, 0.0015);
          await storage.incrementApifySpend(runId, actorCost);

          const emailByDomain = new Map<string, any>();
          for (const r of finderResults) {
            const email = r.email || r.work_email || r.personal_email || "";
            if (!email || isBlockedEmail(email)) continue;
            const domain = r.company_domain || r.domain || "";
            if (domain && !emailByDomain.has(domain.toLowerCase())) {
              emailByDomain.set(domain.toLowerCase(), r);
            }
          }

          let finderEnriched = 0;
          for (const lead of leadsForFinderReEnrich) {
            const ch = (lead.ownedChannels as Record<string, string>) || {};
            const websiteUrl = ch.website || lead.website || "";
            const domain = extractDomainFromUrl(websiteUrl);
            if (!domain) continue;

            const match = emailByDomain.get(domain.toLowerCase());
            if (!match) continue;

            const email = match.email || match.work_email || match.personal_email || "";
            if (!email || isBlockedEmail(email)) continue;

            const updateData: Record<string, any> = { email };
            if (match.phone && !lead.phone) updateData.phone = match.phone;
            if (match.linkedin_url && !lead.linkedin) updateData.linkedin = match.linkedin_url;

            if (Object.keys(updateData).length > 0) {
              await storage.updateLead(lead.id, updateData);
              finderEnriched++;
            }
          }
          await appendAndSave(`Leads Finder: enriched ${finderEnriched} leads from ${domains.length} domains`);
        } catch (err: any) {
          if (err.costUsd) {
            await storage.incrementApifySpend(runId, err.costUsd);
          }
          await appendAndSave(`[WARN] Leads Finder re-enrichment failed: ${err.message}`);
        }
      }
    }

    if (process.env.MILLIONVERIFIER_API_KEY) {
      await appendAndSave("Step 5: Validating emails...", 75, "Re-enrichment: Email validation");
      if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);

      const leadsForValidation = await storage.listLeadsByRun(runId);
      const leadsNeedingValidation = leadsForValidation.filter(l => l.email && !l.emailValidation);

      if (leadsNeedingValidation.length > 0) {
        await appendAndSave(`Email validation: verifying ${leadsNeedingValidation.length} emails...`);
        const emailsToVerify = leadsNeedingValidation.map(l => ({ email: l.email!, leadId: l.id }));

        try {
          const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
            if (verified % 20 === 0 || verified === total) {
              await appendAndSave(`Email validation: ${verified}/${total} verified...`);
            }
          });

          let validCount = 0, invalidCount = 0, catchAllCount = 0, unknownCount = 0;
          for (const [leadId, result] of Array.from(results.entries())) {
            const validation = mapResultToValidation(result.result);
            await storage.updateLead(leadId, { emailValidation: validation });
            if (validation === "valid") validCount++;
            else if (validation === "invalid") invalidCount++;
            else if (validation === "catch-all") catchAllCount++;
            else unknownCount++;
          }

          await appendAndSave(
            `Email validation: ${validCount} valid, ${invalidCount} invalid, ${catchAllCount} catch-all, ${unknownCount} unknown`
          );
        } catch (err: any) {
          await appendAndSave(`[WARN] Email validation failed: ${err.message}`);
        }
      } else {
        await appendAndSave("Email validation: no new emails to verify");
      }
    }

    if (isHubspotConfigured()) {
      await appendAndSave("Checking HubSpot for existing contacts...", 78, "Re-enrichment: HubSpot check");
      try {
        const hubLeads = await storage.listLeadsByRun(runId);
        const hubValid = hubLeads.filter(l => l.email && l.emailValidation === "valid");
        if (hubValid.length > 0) {
          const emailMap = new Map(hubValid.map(l => [l.email!.toLowerCase(), l.id]));
          const hubResults = await checkEmailsInHubspot(Array.from(emailMap.keys()));
          let existingCount = 0, netNewCount = 0;
          for (const entry of Array.from(hubResults.entries())) {
            const leadId = emailMap.get(entry[0].toLowerCase());
            if (leadId) {
              await storage.updateLead(leadId, { hubspotStatus: entry[1] ? "existing" : "net_new" });
              if (entry[1]) existingCount++; else netNewCount++;
            }
          }
          await appendAndSave(`HubSpot check: ${existingCount} existing, ${netNewCount} net new`);
        }
      } catch (err: any) {
        await appendAndSave(`[WARN] HubSpot check failed: ${err.message}`);
      }
    }

    await appendAndSave(`Step 6: Re-scoring all leads...`, 80, "Re-enrichment: Scoring");
    const finalLeads = await storage.listLeadsByRun(runId);
    let reScored = 0;
    for (const lead of finalLeads) {
      const breakdown = scoreLead({
        name: lead.communityName || "",
        description: "",
        type: lead.communityType || "",
        location: lead.location || "",
        website: lead.website || "",
        email: lead.email || "",
        phone: lead.phone || "",
        linkedin: lead.linkedin || "",
        ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
        monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
        engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
        tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
        leaderName: lead.leaderName || "",
        memberCount: (lead.engagementSignals as any)?.member_count || 0,
        subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
        raw: (lead.raw as Record<string, any>) || {},
        emailValidation: lead.emailValidation || "",
      });

      await storage.updateLead(lead.id, {
        score: breakdown.total,
        scoreBreakdown: breakdown,
        lastSeenAt: new Date(),
      });
      reScored++;
    }

    const emailCount = await storage.countLeadsByRunWithEmail(runId);
    const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);

    await storage.updateRun(runId, {
      leadsWithEmail: emailCount,
      leadsWithValidEmail: validEmailCount,
    });

    await appendAndSave(
      `Re-enrichment complete! ${reScored} leads re-scored, ${emailCount} have emails (${validEmailCount} valid)`,
      100,
      "Re-enrichment complete"
    );

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Re-enrichment complete",
      finishedAt: new Date(),
      logs: currentLogs,
    });

    log(`Re-enrichment of run ${runId} completed successfully`, "pipeline");
  } catch (err: any) {
    if (err instanceof RunCancelledError || cancelledRunIds.has(runId)) {
      log(`Re-enrichment of run ${runId} stopped by user`, "pipeline");
      const emailCount = await storage.countLeadsByRunWithEmail(runId);
      const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);
      await storage.updateRun(runId, {
        status: "stopped",
        step: "Stopped by user",
        leadsWithEmail: emailCount,
        leadsWithValidEmail: validEmailCount,
        logs: appendLog(currentLogs, `Re-enrichment stopped by user. ${emailCount} leads with email preserved.`),
        finishedAt: new Date(),
      });
    } else {
      log(`Re-enrichment of run ${runId} failed: ${err.message}`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Re-enrichment failed",
        logs: appendLog(currentLogs, `[ERROR] Re-enrichment failed: ${err.message}`),
        finishedAt: new Date(),
      });
    }
  } finally {
    if (stopHeartbeat) stopHeartbeat();
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
  }
}

const STEP_ORDER: PipelineStep[] = [
  PIPELINE_STEPS.DISCOVERY,
  PIPELINE_STEPS.FB_GOOGLE_BRIDGE,
  PIPELINE_STEPS.INSTAGRAM_BIOS,
  PIPELINE_STEPS.GOOGLE_CONTACT_SEARCH,
  PIPELINE_STEPS.WEBSITE_CRAWL,
  PIPELINE_STEPS.LEAD_CREATION,
  PIPELINE_STEPS.APOLLO,
  PIPELINE_STEPS.LEADS_FINDER,
  PIPELINE_STEPS.EMAIL_VALIDATION,
  PIPELINE_STEPS.SCORING,
];

function shouldRunStep(lastCompleted: string, target: PipelineStep): boolean {
  if (!lastCompleted) return true;
  const lastIdx = STEP_ORDER.indexOf(lastCompleted as PipelineStep);
  const targetIdx = STEP_ORDER.indexOf(target);
  if (lastIdx === -1) return true;
  return targetIdx > lastIdx;
}

function isResumable(lastCompleted: string): boolean {
  if (!lastCompleted) return false;
  const idx = STEP_ORDER.indexOf(lastCompleted as PipelineStep);
  if (idx === -1) return false;
  const leadCreationIdx = STEP_ORDER.indexOf(PIPELINE_STEPS.LEAD_CREATION);
  return idx >= leadCreationIdx;
}

function dbLeadsToPlatformLeads(leads: any[]): (PlatformLead & { dbId: number })[] {
  return leads.map((lead) => ({
    dbId: lead.id,
    source: lead.source || "patreon",
    communityName: lead.communityName || "",
    communityType: lead.communityType || "",
    description: "",
    location: lead.location || "",
    website: lead.website || "",
    email: lead.email || "",
    phone: lead.phone || "",
    leaderName: lead.leaderName || "",
    memberCount: (lead.engagementSignals as any)?.member_count || 0,
    subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
    ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
    monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
    engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
    tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
    raw: (lead.raw as Record<string, any>) || {},
  }));
}

async function resumeFromCheckpoint(
  runId: number,
  allPlatformLeads: PlatformLead[],
  completedSubs: Set<string>,
  params: RunParams,
  appendAndSave: (msg: string, progress?: number, step?: string) => Promise<void>,
): Promise<void> {
  try {
    const enabledSources = params.enabledSources || [];

    if (!completedSubs.has("fb_google_bridge")) {
      const enrichGroup1: { name: string; promise: Promise<void> }[] = [];

      const hasLinkedInLeadsAuto = allPlatformLeads.some(l => l.source === "linkedin" && !l.leaderName && l.ownedChannels?.linkedin_group);
      if (hasLinkedInLeadsAuto) {
        enrichGroup1.push({ name: "LinkedIn Group Pages", promise: enrichLinkedInGroupPages(runId, allPlatformLeads, appendAndSave) });
      }

      const hasFbOrMeetupLeads = allPlatformLeads.some(l => l.source === "facebook" || l.source === "meetup" || l.source === "linkedin");
      if (hasFbOrMeetupLeads) {
        const groupLeadsNeedingEnrich = allPlatformLeads.filter(l => (l.source === "facebook" || l.source === "meetup" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
        if (groupLeadsNeedingEnrich.length > 0) {
          await appendAndSave(`Resume: Google Bridge for ${groupLeadsNeedingEnrich.length} Facebook/Meetup/LinkedIn groups`, 32, "Resume: Google Bridge + Link Aggregators");
          enrichGroup1.push({ name: "Google Bridge", promise: googleBridgeEnrichFacebookGroups(runId, allPlatformLeads, appendAndSave) });
        }
      }

      const leadsWithLinktree = allPlatformLeads.filter(l =>
        !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http")
      );
      if (leadsWithLinktree.length > 0) {
        enrichGroup1.push({ name: "Link aggregators (pass 1)", promise: enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave) });
      }

      if (enrichGroup1.length > 0) {
        const g1Results = await Promise.allSettled(enrichGroup1.map(t => t.promise));
        for (let i = 0; i < g1Results.length; i++) {
          if (g1Results[i].status === "rejected") {
            await appendAndSave(`[WARN] ${enrichGroup1[i].name} failed: ${(g1Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
          }
        }
      }
      await markStepComplete(runId, PIPELINE_STEPS.FB_GOOGLE_BRIDGE);
      await saveCheckpoint(runId, allPlatformLeads, "fb_google_bridge");
    } else {
      await appendAndSave("Resume: Google Bridge already completed, skipping");
    }

    if (!completedSubs.has("social_scraping")) {
      const enrichGroup2: { name: string; promise: Promise<void> }[] = [];
      const budgetExhausted = await isBudgetExhausted(runId, 0.05);
      const emailTargetReached = await isEmailTargetReached(runId);

      if (!budgetExhausted && !emailTargetReached) {
        const leadsWithYouTube = allPlatformLeads.filter(l => l.ownedChannels?.youtube?.startsWith("http"));
        if (leadsWithYouTube.length > 0) {
          enrichGroup2.push({ name: "YouTube about pages", promise: enrichFromYouTubeAboutPages(runId, allPlatformLeads, appendAndSave) });
        }
        const leadsWithInstagram = allPlatformLeads.filter(l => l.ownedChannels?.instagram?.startsWith("http"));
        if (leadsWithInstagram.length > 0) {
          enrichGroup2.push({ name: "Instagram bios", promise: enrichFromInstagramBios(runId, allPlatformLeads, appendAndSave) });
        }
        const leadsWithTwitter = allPlatformLeads.filter(l => {
          const tw = l.ownedChannels?.twitter;
          return tw && (tw.startsWith("http") || tw.startsWith("@"));
        });
        if (leadsWithTwitter.length > 0) {
          enrichGroup2.push({ name: "Twitter/X bios", promise: enrichFromTwitterBios(runId, allPlatformLeads, appendAndSave) });
        }
      }

      if (enrichGroup2.length > 0) {
        await appendAndSave(`Resume: Running ${enrichGroup2.length} social scrape tasks`, 36, "Resume: Social scraping");
        const g2Results = await Promise.allSettled(enrichGroup2.map(t => t.promise));
        for (let i = 0; i < g2Results.length; i++) {
          if (g2Results[i].status === "rejected") {
            await appendAndSave(`[WARN] ${enrichGroup2[i].name} failed: ${(g2Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
          }
        }
      }
      await markStepComplete(runId, PIPELINE_STEPS.INSTAGRAM_BIOS);
      await saveCheckpoint(runId, allPlatformLeads, "social_scraping");
    } else {
      await appendAndSave("Resume: Social scraping already completed, skipping");
    }

    if (!completedSubs.has("google_contact_search")) {
      const enrichGroup3: { name: string; promise: Promise<void> }[] = [];

      const newLinktreeLeads = allPlatformLeads.filter(l =>
        !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http")
      );
      if (newLinktreeLeads.length > 0) {
        enrichGroup3.push({ name: "Link aggregators (pass 2)", promise: enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave) });
      }

      const leadsWithoutContactInfo = allPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
      if (leadsWithoutContactInfo.length > 0) {
        enrichGroup3.push({ name: "Google contact search", promise: googleSearchEnrichCreators(runId, allPlatformLeads, appendAndSave) });
      }

      if (enrichGroup3.length > 0) {
        await appendAndSave(`Resume: Running ${enrichGroup3.length} post-social tasks`, 40, "Resume: Google contact search");
        const g3Results = await Promise.allSettled(enrichGroup3.map(t => t.promise));
        for (let i = 0; i < g3Results.length; i++) {
          if (g3Results[i].status === "rejected") {
            await appendAndSave(`[WARN] ${enrichGroup3[i].name} failed: ${(g3Results[i] as PromiseRejectedResult).reason?.message || "Unknown error"}`);
          }
        }
      }
      await markStepComplete(runId, PIPELINE_STEPS.GOOGLE_CONTACT_SEARCH);
      await saveCheckpoint(runId, allPlatformLeads, "google_contact_search");
    } else {
      await appendAndSave("Resume: Google contact search already completed, skipping");
    }

    if (!completedSubs.has("website_crawl")) {
      let slugDomainsProbed = 0;
      for (const pl of allPlatformLeads) {
        if (pl.ownedChannels?.website || pl.email) continue;
        const patreonUrl = pl.website || pl.ownedChannels?.patreon || "";
        if (!patreonUrl.includes("patreon.com/")) continue;
        const slug = patreonUrl.split("patreon.com/")[1]?.split(/[?#/]/)[0]?.toLowerCase();
        if (!slug || slug.length < 3 || slug.startsWith("u") && /^\d+$/.test(slug.slice(1))) continue;
        if (/[^a-z0-9_-]/.test(slug)) continue;
        const cleanSlug = slug.replace(/[_-]/g, "");
        if (cleanSlug.length < 4) continue;
        const candidateDomain = `${cleanSlug}.com`;
        if (!pl.ownedChannels) pl.ownedChannels = {};
        pl.ownedChannels.website = `https://${candidateDomain}`;
        slugDomainsProbed++;
      }
      if (slugDomainsProbed > 0) {
        await appendAndSave(`Resume: Slug domain probe tried ${slugDomainsProbed} Patreon slugs as .com domains`);
      }

      const leadsNeedingEmail = allPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
      if (leadsNeedingEmail.length > 0) {
        await appendAndSave(`Resume: Crawling ${leadsNeedingEmail.length} creator websites for contact emails...`, 45, "Resume: Website contact crawl");
        const websiteEmailMap = await crawlCreatorWebsitesForEmails(runId, leadsNeedingEmail, appendAndSave);
        let websiteEmailsMerged = 0;
        for (const pl of allPlatformLeads) {
          if (!pl.email && pl.ownedChannels?.website) {
            const domain = extractDomain(pl.ownedChannels.website);
            if (domain && websiteEmailMap.has(domain)) {
              pl.email = websiteEmailMap.get(domain)!;
              websiteEmailsMerged++;
            }
          }
        }
        await appendAndSave(`Resume: Website crawl found ${websiteEmailsMerged} emails`);
      }
      await markStepComplete(runId, PIPELINE_STEPS.WEBSITE_CRAWL);
      await saveCheckpoint(runId, allPlatformLeads, "website_crawl");
    } else {
      await appendAndSave("Resume: Website crawl already completed, skipping");
    }

    await appendAndSave(`Resume: Creating ${allPlatformLeads.length} leads in database...`, 55, "Resume: Lead creation");

    let createdCount = 0;
    for (const pl of allPlatformLeads) {
      try {
        let existingLead = null;
        if (pl.email) existingLead = await storage.findLeadByEmail(pl.email);
        if (!existingLead && pl.website) existingLead = await storage.findLeadByWebsite(pl.website);
        if (!existingLead && pl.communityName) {
          existingLead = await storage.findLeadByNameAndLocation(pl.communityName, pl.location);
        }

        if (existingLead) {
          await storage.updateLead(existingLead.id, { lastSeenAt: new Date() });
          continue;
        }

        const community = await storage.createCommunity({
          name: pl.communityName,
          type: pl.communityType,
          description: pl.description,
          website: pl.website,
          ownedChannels: pl.ownedChannels,
          eventCadence: pl.engagementSignals,
          audienceSignals: pl.tripFitSignals,
          sourceUrls: [pl.website],
        });

        let leaderId: number | undefined;
        if (pl.leaderName) {
          const leader = await storage.createLeader({
            name: pl.leaderName,
            role: "",
            email: pl.email,
            phone: pl.phone,
            sourceUrl: pl.website,
            communityId: community.id,
          });
          leaderId = leader.id;
        }

        const breakdown = scoreLead({
          name: pl.communityName,
          description: pl.description,
          type: pl.communityType,
          location: pl.location,
          website: pl.website,
          email: pl.email,
          phone: pl.phone,
          linkedin: pl.ownedChannels?.linkedin || "",
          ownedChannels: pl.ownedChannels || {},
          monetizationSignals: pl.monetizationSignals || {},
          engagementSignals: pl.engagementSignals || {},
          tripFitSignals: pl.tripFitSignals || {},
          leaderName: pl.leaderName,
          memberCount: pl.memberCount || 0,
          subscriberCount: pl.subscriberCount || 0,
          raw: pl.raw || {},
          emailValidation: "",
        });

        await storage.createLead({
          communityName: pl.communityName,
          communityType: pl.communityType,
          location: pl.location,
          website: pl.website,
          email: pl.email,
          phone: pl.phone,
          leaderName: pl.leaderName,
          firstName: extractHighConfidenceFirstName(pl.leaderName),
          researchSummary: generateResearchSummary({ communityName: pl.communityName, communityType: pl.communityType, leaderName: pl.leaderName, description: pl.description, source: pl.source, website: pl.website, location: pl.location, ownedChannels: pl.ownedChannels || {}, engagementSignals: pl.engagementSignals || {}, monetizationSignals: pl.monetizationSignals || {}, tripFitSignals: pl.tripFitSignals || {}, raw: pl.raw || {} }),
          source: pl.source,
          score: breakdown.total,
          scoreBreakdown: breakdown,
          runId,
          communityId: community.id,
          leaderId,
          ownedChannels: pl.ownedChannels || {},
          monetizationSignals: pl.monetizationSignals || {},
          engagementSignals: pl.engagementSignals || {},
          tripFitSignals: pl.tripFitSignals || {},
          raw: pl.raw || {},
        });
        createdCount++;
      } catch (err: any) {
        log(`[WARN] Failed to create lead from checkpoint: ${err.message}`, "pipeline");
      }
    }

    await storage.updateRun(runId, { leadsExtracted: createdCount });
    await appendAndSave(`Resume: Created ${createdCount} leads from checkpoint`, 70, "Resume: Contact enrichment");
    await markStepComplete(runId, PIPELINE_STEPS.LEAD_CREATION);
    await clearCheckpoint(runId);

    const runLeads = await storage.listLeadsByRun(runId);

    if (params.enableApollo !== false) {
      const leadsToEnrich = runLeads
        .filter((l) => !l.email)
        .sort((a, b) => (b.score || 0) - (a.score || 0));

      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        await appendAndSave(`Resume: Apollo.io enriching ${leadsToEnrich.length} leads without email...`, 75, "Resume: Apollo enrichment");

        let enrichedCount = 0, apolloSkipped = 0, apolloCalls = 0, apolloDeduped = 0;
        for (const lead of leadsToEnrich) {
          try {
            if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
            const currentHash = computeApolloInputHash(lead);
            if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) { apolloDeduped++; continue; }

            const leaderName = lead.leaderName || lead.communityName || "";
            if (!leaderName) continue;
            if (!isValidApolloCandidate(leaderName)) { apolloSkipped++; continue; }

            const nameParts = leaderName.trim().split(/\s+/);
            const firstName = nameParts[0] || "";
            const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
            const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;

            const channels = (lead.ownedChannels as Record<string, string>) || {};
            let domain = apolloExtractDomain(lead.website || "");
            if (!domain || !apolloIsEnrichable(domain)) {
              if (channels.website) domain = apolloExtractDomain(channels.website);
            }
            const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;
            const linkedinUrl = channels.linkedin && channels.linkedin.startsWith("http") ? channels.linkedin : undefined;

            apolloCalls++;
            let result = await apolloPersonMatch({
              name: leaderName,
              firstName: hasRealName ? firstName : undefined,
              lastName: hasRealName ? lastName : undefined,
              domain: enrichableDomain,
              organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
              linkedinUrl,
            });

            if (!result && hasRealName && !linkedinUrl) {
              apolloCalls++;
              result = await apolloPersonMatch({ firstName, lastName, domain: enrichableDomain });
              await new Promise((r) => setTimeout(r, 300));
            }

            if (!result) {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
              continue;
            }

            const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };
            if (result.email) updateData.email = result.email;
            if (result.phone && !lead.phone) updateData.phone = result.phone;
            if (result.linkedin && !lead.linkedin) updateData.linkedin = result.linkedin;
            if (result.location && !lead.location) updateData.location = result.location;
            if (!lead.leaderName && result.fullName) updateData.leaderName = result.fullName;

            const existingChannels = (lead.ownedChannels as Record<string, string>) || {};
            const updatedChannels = { ...existingChannels };
            if (result.twitter && !existingChannels.twitter) updatedChannels.twitter = result.twitter;
            if (result.facebook && !existingChannels.facebook) updatedChannels.facebook = result.facebook;
            if (result.linkedin && !existingChannels.linkedin) updatedChannels.linkedin = result.linkedin;
            if (Object.keys(updatedChannels).length > Object.keys(existingChannels).length) {
              updateData.ownedChannels = updatedChannels;
            }

            if (Object.keys(updateData).length > 2) {
              await storage.updateLead(lead.id, updateData);
              enrichedCount++;
            } else {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
            }
            await new Promise((r) => setTimeout(r, 300));
          } catch (err: any) {
            if (err instanceof RunCancelledError) throw err;
            await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
          }
        }
        await appendAndSave(`Resume: Apollo.io enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped, ${apolloDeduped} deduped, ${apolloCalls} API calls)`);
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.APOLLO);

    {
      const refreshedLeads = await storage.listLeadsByRun(runId);
      const leadsForFinder = refreshedLeads
        .filter((l) => !l.email)
        .filter((l) => {
          const channels = (l.ownedChannels as Record<string, string>) || {};
          const websiteUrl = channels.website || l.website || "";
          const domain = extractDomainFromUrl(websiteUrl);
          return domain && isEnrichableDomain(domain);
        });

      if (leadsForFinder.length > 0) {
        await appendAndSave(`Resume: Leads Finder enriching ${leadsForFinder.length} leads by domain...`, 80, "Resume: Leads Finder");
        const domains = Array.from(new Set(
          leadsForFinder.map((l) => {
            const ch = (l.ownedChannels as Record<string, string>) || {};
            return extractDomainFromUrl(ch.website || l.website || "");
          }).filter((d) => d && isEnrichableDomain(d))
        ));

        if (domains.length > 0) {
          try {
            const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", {
              company_domain: domains,
              email_status: ["validated"],
              fetch_count: Math.min(domains.length * 5, 200),
            }, 120000, 0.0015);
            await storage.incrementApifySpend(runId, actorCost);

            const emailByDomain = new Map<string, any>();
            for (const r of finderResults) {
              const email = r.email || r.work_email || r.personal_email || "";
              if (!email || isBlockedEmail(email)) continue;
              const domain = r.company_domain || r.domain || "";
              if (domain && !emailByDomain.has(domain.toLowerCase())) {
                emailByDomain.set(domain.toLowerCase(), r);
              }
            }

            let finderEnriched = 0;
            for (const lead of leadsForFinder) {
              const ch = (lead.ownedChannels as Record<string, string>) || {};
              const websiteUrl = ch.website || lead.website || "";
              const domain = extractDomainFromUrl(websiteUrl);
              if (!domain) continue;
              const result = emailByDomain.get(domain.toLowerCase());
              if (!result) continue;
              const foundEmail = result.email || result.work_email || result.personal_email || "";
              if (!foundEmail || isBlockedEmail(foundEmail)) continue;
              const cleaned = cleanEmail(foundEmail);
              if (!cleaned) continue;

              const updateData: Record<string, any> = { email: cleaned };
              if (result.first_name && result.last_name && !lead.leaderName) {
                updateData.leaderName = `${result.first_name} ${result.last_name}`.trim();
              }
              await storage.updateLead(lead.id, updateData);
              finderEnriched++;
            }
            await appendAndSave(`Resume: Leads Finder enriched ${finderEnriched} leads from ${domains.length} domains`);
          } catch (err: any) {
            if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
            await appendAndSave(`[WARN] Leads Finder failed: ${err.message}`);
          }
        }
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.LEADS_FINDER);

    if (process.env.MILLIONVERIFIER_API_KEY) {
      const allLeadsForValidation = await storage.listLeadsByRun(runId);
      const leadsWithEmail = allLeadsForValidation.filter(l => l.email && !l.emailValidation);

      if (leadsWithEmail.length > 0) {
        await appendAndSave(`Resume: Validating ${leadsWithEmail.length} emails...`, 85, "Resume: Email validation");
        const emailsToVerify = leadsWithEmail.map(l => ({ email: l.email!, leadId: l.id }));
        try {
          const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
            if (verified % 20 === 0 || verified === total) {
              await appendAndSave(`Email validation: ${verified}/${total} verified...`);
            }
          });
          let validCount = 0, invalidCount = 0, catchAllCount = 0, unknownCount = 0;
          for (const [leadId, result] of Array.from(results.entries())) {
            const validation = mapResultToValidation(result.result);
            await storage.updateLead(leadId, { emailValidation: validation });
            if (validation === "valid") validCount++;
            else if (validation === "invalid") invalidCount++;
            else if (validation === "catch-all") catchAllCount++;
            else unknownCount++;
          }
          await appendAndSave(`Resume: Email validation: ${validCount} valid, ${invalidCount} invalid, ${catchAllCount} catch-all, ${unknownCount} unknown`);
        } catch (err: any) {
          await appendAndSave(`[WARN] Email validation failed: ${err.message}`);
        }
      }
    }
    await markStepComplete(runId, PIPELINE_STEPS.EMAIL_VALIDATION);

    if (isHubspotConfigured()) {
      const hubspotLeads = await storage.listLeadsByRun(runId);
      const leadsWithValidEmail = hubspotLeads.filter(l => l.email && l.emailValidation === "valid");
      if (leadsWithValidEmail.length > 0) {
        await appendAndSave(`Resume: Checking ${leadsWithValidEmail.length} emails against HubSpot...`, 88, "Resume: HubSpot CRM check");
        try {
          const emails = leadsWithValidEmail.map(l => l.email!);
          const hubspotResults = await checkEmailsInHubspot(emails);
          for (const lead of leadsWithValidEmail) {
            const isExisting = hubspotResults.get(lead.email!) === true;
            await storage.updateLead(lead.id, { hubspotStatus: isExisting ? "existing" : "net_new" });
          }
          const existingCount = Array.from(hubspotResults.values()).filter(v => v === true).length;
          await appendAndSave(`Resume: HubSpot: ${existingCount} existing, ${leadsWithValidEmail.length - existingCount} net new`);
        } catch (err: any) {
          await appendAndSave(`[WARN] HubSpot check failed: ${err.message}`);
        }
      }
    }

    await appendAndSave("Resume: Re-scoring all leads...", 92, "Resume: Final scoring");
    const finalLeads = await storage.listLeadsByRun(runId);
    for (const lead of finalLeads) {
      const breakdown = scoreLead({
        name: lead.communityName || "",
        description: "",
        type: lead.communityType || "",
        location: lead.location || "",
        website: lead.website || "",
        email: lead.email || "",
        phone: lead.phone || "",
        linkedin: lead.linkedin || "",
        ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
        monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
        engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
        tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
        leaderName: lead.leaderName || "",
        memberCount: (lead.engagementSignals as any)?.member_count || 0,
        subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
        raw: (lead.raw as Record<string, any>) || {},
        emailValidation: lead.emailValidation || "",
      });
      await storage.updateLead(lead.id, {
        score: breakdown.total,
        scoreBreakdown: breakdown,
        lastSeenAt: new Date(),
      });
    }
    await markStepComplete(runId, PIPELINE_STEPS.SCORING);

    const run2 = await storage.getRun(runId);
    const isAutonomousResume = run2?.isAutonomous === true;
    const resumeEmailTarget = run2?.emailTarget || 0;
    const resumeBudgetUsd = run2?.budgetUsd || 0;
    const resumeKeywords = params.seedKeywords || [];
    const resumeEnabledSources = params.enabledSources || [];
    const resumeGeos = params.seedGeos || [];
    let resumeValidEmailCount = await storage.countLeadsByRunWithValidEmail(runId);

    if (isAutonomousResume && resumeEmailTarget > 0 && resumeValidEmailCount < resumeEmailTarget && resumeBudgetUsd > 0) {
      let expansionRound = 0;
      const usedExpansionKeywords = new Set<string>();

      const EXPANSION_SUFFIX_POOLS = [
        ["community leader", "group organizer"],
        ["travel group", "adventure community"],
        ["retreat host", "trip organizer"],
        ["club founder", "meetup organizer"],
        ["group admin", "community founder"],
        ["event organizer", "workshop host"],
        ["group leader near me", "community events"],
        ["online community", "membership group"],
      ];

      while (true) {
        if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);

        const budgetInfo = await getRunBudgetInfo(runId);
        const remainingBudget = resumeBudgetUsd - budgetInfo.spentUsd;
        const currentValidCount = await storage.countLeadsByRunWithValidEmail(runId);
        await storage.updateRun(runId, { leadsWithValidEmail: currentValidCount });
        const deficit = resumeEmailTarget - currentValidCount;

        if (await isValidEmailTargetReached(runId)) {
          await appendAndSave(`Resume expansion: Target reached: ${currentValidCount}/${resumeEmailTarget} valid emails.`);
          break;
        }
        if (remainingBudget <= 0.50) {
          await appendAndSave(`Resume expansion: Budget exhausted: ${currentValidCount}/${resumeEmailTarget} valid emails. $${budgetInfo.spentUsd.toFixed(2)}/$${resumeBudgetUsd.toFixed(2)} spent.`);
          break;
        }
        if (deficit <= 3) {
          await appendAndSave(`Resume expansion: Near target: ${currentValidCount}/${resumeEmailTarget} valid emails. Deficit too small.`);
          break;
        }

        expansionRound++;
        await appendAndSave(
          `Resume expansion round ${expansionRound}: ${currentValidCount}/${resumeEmailTarget} valid emails (${deficit} short). $${remainingBudget.toFixed(2)} remaining.`,
          93,
          `Resume: Expansion round ${expansionRound}`
        );

        const suffixPoolIndex = Math.min(expansionRound - 1, EXPANSION_SUFFIX_POOLS.length - 1);
        const expandSuffixes = EXPANSION_SUFFIX_POOLS[suffixPoolIndex];
        const expandedKws = resumeKeywords.flatMap((kw: string) => expandSuffixes.map(s => `${kw} ${s}`));
        const uniqueExpKws = Array.from(new Set(expandedKws))
          .filter(kw => !usedExpansionKeywords.has(kw))
          .slice(0, 8);

        if (uniqueExpKws.length === 0) {
          await appendAndSave(`Resume expansion: all keyword variations exhausted after ${expansionRound - 1} rounds.`);
          break;
        }
        for (const kw of uniqueExpKws) usedExpansionKeywords.add(kw);

        let newPlatformLeads: PlatformLead[] = [];

        for (const platform of resumeEnabledSources) {
          if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
          if (await isBudgetExhausted(runId, 0.10)) {
            await appendAndSave(`Resume expansion: budget limit reached, stopping discovery.`);
            break;
          }

          const maxExpLeads = Math.min(Math.ceil(deficit * 4), 500);
          try {
            if (platform === "patreon") {
              await appendAndSave(`Resume expansion: deeper Patreon search (${uniqueExpKws.length} queries)...`);
              const results = await scrapePatreonCreators(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                minPostCount: params.minPostCount || 0,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "facebook") {
              await appendAndSave(`Resume expansion: deeper Facebook search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeFacebookGroups(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                geos: resumeGeos,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "substack") {
              await appendAndSave(`Resume expansion: deeper Substack search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeSubstackWriters(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg));
              newPlatformLeads.push(...results);
            } else if (platform === "mighty") {
              await appendAndSave(`Resume expansion: deeper Mighty Networks search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeMightyNetworks(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), resumeGeos);
              newPlatformLeads.push(...results);
            } else if (platform === "linkedin") {
              await appendAndSave(`Resume expansion: deeper LinkedIn Groups search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeLinkedInGroups(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minMemberCount: params.minMemberCount || 0,
                maxMemberCount: params.maxMemberCount || 0,
                geos: resumeGeos,
              });
              newPlatformLeads.push(...results);
            } else if (platform === "podcast" && run2?.podcastEnabled !== false) {
              await appendAndSave(`Resume expansion: deeper podcast search (${uniqueExpKws.length} queries)...`);
              const results = await scrapeApplePodcasts(runId, uniqueExpKws, maxExpLeads, (msg) => appendAndSave(msg), {
                minEpisodeCount: params.minEpisodeCount || 0,
                podcastCountry: params.podcastCountry || "US",
              });
              newPlatformLeads.push(...results);
            }
          } catch (err: any) {
            await appendAndSave(`Resume expansion: ${platform} search error: ${err.message}`);
          }
        }

        await appendAndSave(`Resume expansion: discovered ${newPlatformLeads.length} new platform results`);

        if (newPlatformLeads.length === 0) {
          await appendAndSave(`Resume expansion: no new leads discovered, lead pool exhausted.`);
          break;
        }

        const expFbLeads2 = newPlatformLeads.filter(l => (l.source === "facebook" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
        if (expFbLeads2.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          try { await googleBridgeEnrichFacebookGroups(runId, newPlatformLeads, appendAndSave); } catch {}
        }

        const expRssLeads = newPlatformLeads.filter(l => l.ownedChannels?.rss && l.ownedChannels.rss.startsWith("http") && !l.email);
        if (expRssLeads.length > 0) {
          try { await enrichFromRssFeeds(runId, newPlatformLeads, appendAndSave); } catch {}
        }

        const expLinktreeLeads = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http"));
        if (expLinktreeLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          try { await enrichFromLinkAggregators(runId, newPlatformLeads, appendAndSave); } catch {}
        }

        if (!(await isBudgetExhausted(runId, 0.10))) {
          const socialTasks: { name: string; promise: Promise<void> }[] = [];
          const ytLeads = newPlatformLeads.filter(l => l.ownedChannels?.youtube?.startsWith("http"));
          if (ytLeads.length > 0) socialTasks.push({ name: "YouTube", promise: enrichFromYouTubeAboutPages(runId, newPlatformLeads, appendAndSave) });
          const igLeads = newPlatformLeads.filter(l => l.ownedChannels?.instagram?.startsWith("http"));
          if (igLeads.length > 0) socialTasks.push({ name: "Instagram", promise: enrichFromInstagramBios(runId, newPlatformLeads, appendAndSave) });
          const twLeads = newPlatformLeads.filter(l => { const tw = l.ownedChannels?.twitter; return tw && (tw.startsWith("http") || tw.startsWith("@")); });
          if (twLeads.length > 0) socialTasks.push({ name: "Twitter", promise: enrichFromTwitterBios(runId, newPlatformLeads, appendAndSave) });

          if (socialTasks.length > 0) {
            const results = await Promise.allSettled(socialTasks.map(t => t.promise));
            for (let i = 0; i < results.length; i++) {
              if (results[i].status === "rejected") {
                await appendAndSave(`[WARN] Resume expansion ${socialTasks[i].name} failed: ${(results[i] as PromiseRejectedResult).reason?.message}`);
              }
            }
          }
        }

        const noContactLeads = newPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
        if (noContactLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          try { await googleSearchEnrichCreators(runId, newPlatformLeads, appendAndSave); } catch {}
        }

        const websiteLeads = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
        if (websiteLeads.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          try {
            const websiteEmailMap = await crawlCreatorWebsitesForEmails(runId, websiteLeads, appendAndSave);
            for (const pl of newPlatformLeads) {
              if (!pl.email && pl.ownedChannels?.website) {
                const domain = extractDomain(pl.ownedChannels.website);
                if (domain && websiteEmailMap.has(domain)) pl.email = websiteEmailMap.get(domain)!;
              }
            }
          } catch {}
        }

        let expansionCreated = 0;
        for (const pl of newPlatformLeads) {
          try {
            let existingLead = null;
            if (pl.email) existingLead = await storage.findLeadByEmail(pl.email);
            if (!existingLead && pl.website) existingLead = await storage.findLeadByWebsite(pl.website);
            if (!existingLead && pl.communityName) existingLead = await storage.findLeadByNameAndLocation(pl.communityName, pl.location);
            if (existingLead) continue;

            const community = await storage.createCommunity({
              name: pl.communityName, type: pl.communityType, description: pl.description,
              website: pl.website, ownedChannels: pl.ownedChannels,
              eventCadence: pl.engagementSignals, audienceSignals: pl.tripFitSignals, sourceUrls: [pl.website],
            });
            let leaderId: number | undefined;
            if (pl.leaderName) {
              const leader = await storage.createLeader({
                name: pl.leaderName, role: "", email: pl.email, phone: pl.phone, sourceUrl: pl.website, communityId: community.id,
              });
              leaderId = leader.id;
            }
            const breakdown = scoreLead({
              name: pl.communityName, description: pl.description, type: pl.communityType,
              location: pl.location, website: pl.website, email: pl.email, phone: pl.phone,
              linkedin: pl.ownedChannels?.linkedin || "", ownedChannels: pl.ownedChannels,
              monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals,
              tripFitSignals: pl.tripFitSignals, leaderName: pl.leaderName,
              memberCount: pl.memberCount, subscriberCount: pl.subscriberCount, raw: pl.raw,
            });
            await storage.createLead({
              leadType: pl.leaderName ? "leader" : "community",
              communityName: pl.communityName, communityType: pl.communityType,
              leaderName: pl.leaderName, location: pl.location, website: pl.website,
              email: pl.email, phone: pl.phone, ownedChannels: pl.ownedChannels,
              monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals,
              tripFitSignals: pl.tripFitSignals, score: breakdown.total, scoreBreakdown: breakdown,
              status: "new", source: pl.source || "", raw: pl.raw, runId,
              communityId: community.id, leaderId,
            });
            expansionCreated++;
          } catch {}
        }

        await appendAndSave(`Resume expansion: created ${expansionCreated} new leads (deduped from ${newPlatformLeads.length})`);
        if (expansionCreated === 0) {
          await appendAndSave(`Resume expansion: all leads were duplicates, stopping.`);
          break;
        }

        if (params.enableApollo !== false && isApolloAvailable() && !(await isBudgetExhausted(runId, 0.10))) {
          const expDbLeads = await storage.listLeadsByRun(runId);
          const expApolloLeads = expDbLeads.filter(l => !l.email && !l.apolloEnrichedAt).sort((a, b) => (b.score || 0) - (a.score || 0));
          if (expApolloLeads.length > 0) {
            await appendAndSave(`Resume expansion: Apollo enrichment for ${expApolloLeads.length} leads...`);
            let expApolloEnriched = 0;
            for (const lead of expApolloLeads) {
              try {
                if (await isBudgetExhausted(runId, 0.05)) break;
                const currentHash = computeApolloInputHash(lead);
                if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) continue;
                const leaderName = lead.leaderName || lead.communityName || "";
                if (!leaderName || !isValidApolloCandidate(leaderName)) continue;
                const nameParts = leaderName.trim().split(/\s+/);
                const firstName = nameParts[0] || "";
                const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
                const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;
                const channels = (lead.ownedChannels as Record<string, string>) || {};
                let domain = apolloExtractDomain(lead.website || "");
                if (!domain || !apolloIsEnrichable(domain)) { if (channels.website) domain = apolloExtractDomain(channels.website); }
                const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;
                const linkedinUrl = channels.linkedin?.startsWith("http") ? channels.linkedin : undefined;
                let result = await apolloPersonMatch({ name: leaderName, firstName: hasRealName ? firstName : undefined, lastName: hasRealName ? lastName : undefined, domain: enrichableDomain, linkedinUrl });
                if (!result && hasRealName && !linkedinUrl) {
                  result = await apolloPersonMatch({ firstName, lastName, domain: enrichableDomain });
                  await new Promise(r => setTimeout(r, 300));
                }
                if (!result) { await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash }); continue; }
                const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };
                if (result.email && !isBlockedEmail(result.email)) { updateData.email = result.email; expApolloEnriched++; }
                if (result.phone) updateData.phone = result.phone;
                if (result.linkedin) updateData.linkedin = result.linkedin;
                await storage.updateLead(lead.id, updateData);
                await new Promise(r => setTimeout(r, 300));
              } catch {}
            }
            await appendAndSave(`Resume expansion: Apollo enriched ${expApolloEnriched} leads`);
          }
        }

        const refreshedExpLeads = await storage.listLeadsByRun(runId);
        const leadsForExpFinder = refreshedExpLeads.filter(l => !l.email && !l.emailValidation).filter(l => {
          const ch = (l.ownedChannels as Record<string, string>) || {};
          const domain = extractDomainFromUrl(ch.website || l.website || "");
          return domain && isEnrichableDomain(domain);
        });
        if (leadsForExpFinder.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
          const expDomains = Array.from(new Set(leadsForExpFinder.map(l => { const ch = (l.ownedChannels as Record<string, string>) || {}; return extractDomainFromUrl(ch.website || l.website || ""); }).filter((d): d is string => !!d && isEnrichableDomain(d))));
          if (expDomains.length > 0) {
            try {
              const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", { company_domain: expDomains, email_status: ["validated"], fetch_count: Math.min(expDomains.length * 5, 200) }, 120000, 0.0015);
              await storage.incrementApifySpend(runId, actorCost);
              const emailByDomain = new Map<string, any>();
              for (const r of finderResults) { const email = r.email || r.work_email || ""; if (!email || isBlockedEmail(email)) continue; const domain = r.company_domain || r.domain || ""; if (domain && !emailByDomain.has(domain.toLowerCase())) emailByDomain.set(domain.toLowerCase(), r); }
              let expEnriched = 0;
              for (const lead of leadsForExpFinder) { const ch = (lead.ownedChannels as Record<string, string>) || {}; const domain = extractDomainFromUrl(ch.website || lead.website || ""); if (!domain) continue; const match = emailByDomain.get(domain.toLowerCase()); if (!match) continue; const email = match.email || match.work_email || ""; if (!email || isBlockedEmail(email)) continue; await storage.updateLead(lead.id, { email }); expEnriched++; }
              await appendAndSave(`Resume expansion: Leads Finder enriched ${expEnriched} leads`);
            } catch (err: any) {
              if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
            }
          }
        }

        const expLeadsForValidation = (await storage.listLeadsByRun(runId)).filter(l => l.email && !l.emailValidation);
        if (expLeadsForValidation.length > 0 && process.env.MILLIONVERIFIER_API_KEY) {
          await appendAndSave(`Resume expansion: validating ${expLeadsForValidation.length} emails...`);
          try {
            const emailsToVerify = expLeadsForValidation.map(l => ({ email: l.email!, leadId: l.id }));
            const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
              if (verified % 20 === 0 || verified === total) await appendAndSave(`Resume expansion validation: ${verified}/${total}`);
            });
            for (const [leadId, result] of Array.from(results.entries())) {
              const validation = mapResultToValidation(result.result);
              await storage.updateLead(leadId, { emailValidation: validation });
            }
          } catch {}
        }

        if (isHubspotConfigured()) {
          try {
            const expHubLeads = (await storage.listLeadsByRun(runId)).filter(l => l.email && l.emailValidation === "valid" && !l.hubspotStatus);
            if (expHubLeads.length > 0) {
              await appendAndSave(`Resume expansion: checking ${expHubLeads.length} new valid emails against HubSpot...`);
              const emailMap = new Map(expHubLeads.map(l => [l.email!.toLowerCase(), l.id]));
              const hubResults = await checkEmailsInHubspot(Array.from(emailMap.keys()));
              let existing = 0, netNew = 0;
              for (const [email, isExisting] of Array.from(hubResults.entries())) {
                const leadId = emailMap.get(email.toLowerCase());
                if (leadId) {
                  await storage.updateLead(leadId, { hubspotStatus: isExisting ? "existing" : "net_new" });
                  if (isExisting) existing++; else netNew++;
                }
              }
              await appendAndSave(`Resume expansion HubSpot: ${existing} existing, ${netNew} net new`);
            }
          } catch (err: any) {
            await appendAndSave(`[WARN] Resume expansion HubSpot check failed: ${err.message}`);
          }
        }

        const postExpansionValid = await storage.countLeadsByRunWithValidEmail(runId);
        const postExpansionEmail = await storage.countLeadsByRunWithEmail(runId);
        await storage.updateRun(runId, {
          leadsWithEmail: postExpansionEmail,
          leadsWithValidEmail: postExpansionValid,
          leadsExtracted: (await storage.listLeadsByRun(runId)).length,
        });
        await appendAndSave(`Resume expansion round ${expansionRound} complete: ${postExpansionValid}/${resumeEmailTarget} valid emails.`, 95);

        if (await isValidEmailTargetReached(runId)) break;
      }
    }

    const emailCount = await storage.countLeadsByRunWithEmail(runId);
    const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Complete",
      finishedAt: new Date(),
      leadsWithEmail: emailCount,
      leadsWithValidEmail: validEmailCount,
    });
    const totalResumeLeads = (await storage.listLeadsByRun(runId)).length;
    await appendAndSave(`Resume complete! ${totalResumeLeads} leads, ${emailCount} with email, ${validEmailCount} validated.`);
    log(`Checkpoint resume of run ${runId} completed successfully`, "pipeline");
  } catch (err: any) {
    if (err instanceof RunCancelledError || cancelledRunIds.has(runId)) {
      log(`Checkpoint resume of run ${runId} stopped by user`, "pipeline");
      const emailCount = await storage.countLeadsByRunWithEmail(runId);
      const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);
      await storage.updateRun(runId, {
        status: "stopped",
        step: "Stopped by user",
        leadsWithEmail: emailCount,
        leadsWithValidEmail: validEmailCount,
        finishedAt: new Date(),
      });
    } else {
      log(`Checkpoint resume of run ${runId} failed: ${err.message}`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Resume failed",
        finishedAt: new Date(),
      });
    }
  } finally {
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
  }
}

export async function restartRun(runId: number): Promise<void> {
  const run = await storage.getRun(runId);
  if (!run) throw new Error(`Run ${runId} not found`);

  await storage.updateRun(runId, {
    status: "queued",
    progress: 0,
    step: "Restarting...",
    logs: "",
    lastCompletedStep: "",
    startedAt: null,
    finishedAt: null,
    urlsDiscovered: 0,
    leadsExtracted: 0,
    leadsWithEmail: 0,
    leadsWithValidEmail: 0,
    apifySpendUsd: 0,
    checkpoint: null,
    completedSubSteps: [],
  });

  await storage.deleteLeadsByRun(runId);
  await storage.deleteSourceUrlsByRun(runId);

  await runPipeline(runId);
}

export async function resumeRun(runId: number): Promise<void> {
  if (activeRunIds.has(runId) || resumingRunIds.has(runId)) {
    log(`Resume skipped: run ${runId} is already active or being resumed`, "pipeline");
    return;
  }
  resumingRunIds.add(runId);

  const run = await storage.getRun(runId);
  if (!run) { resumingRunIds.delete(runId); throw new Error(`Run ${runId} not found`); }

  const params = run.params as RunParams;
  const lastCompleted = run.lastCompletedStep || "";
  let currentLogs = "";
  let stopHeartbeat: (() => void) | null = null;

  const appendAndSave = async (msg: string, progress?: number, step?: string) => {
    if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
    currentLogs = appendLog(currentLogs, msg);
    log(msg, "pipeline");
    const update: Record<string, any> = { logs: currentLogs };
    if (progress !== undefined) update.progress = progress;
    if (step) update.step = step;
    await storage.updateRun(runId, update);
  };

  try {
    activeRunIds.add(runId);
    stopHeartbeat = startHeartbeat(runId);

    const checkpoint = await loadCheckpoint(runId);
    const hasCheckpoint = checkpoint && checkpoint.platformLeads && checkpoint.platformLeads.length > 0;

    if (!isResumable(lastCompleted) && !hasCheckpoint) {
      const msg = `Resume aborted: pipeline was interrupted before any data was saved (last step: ${lastCompleted || "none"}). Use Restart to re-run from scratch.`;
      log(msg, "pipeline");
      await storage.updateRun(runId, {
        status: "interrupted",
        step: "Resume aborted (no checkpoint)",
        finishedAt: new Date(),
        logs: appendLog(run.logs || "", msg),
      });
      activeRunIds.delete(runId);
      return;
    }

    if (!isResumable(lastCompleted) && hasCheckpoint) {
      currentLogs = appendLog(run.logs || "", `--- Resume from checkpoint (${checkpoint!.completedSubSteps.length} sub-steps completed, ${checkpoint!.platformLeads.length} leads saved) ---`);
      await storage.updateRun(runId, {
        status: "running",
        progress: Math.max(run.progress || 0, 30),
        step: "Resuming from checkpoint",
        finishedAt: null,
        logs: currentLogs,
      });

      await appendAndSave(`Restored ${checkpoint!.platformLeads.length} platform leads from checkpoint (Apify spend preserved: $${(checkpoint!.apifySpendAtCheckpoint || 0).toFixed(2)})`);

      const allPlatformLeads: PlatformLead[] = checkpoint!.platformLeads;
      const completedSubs = new Set(checkpoint!.completedSubSteps);

      await resumeFromCheckpoint(runId, allPlatformLeads, completedSubs, params, appendAndSave);
      return;
    }

    const leads = await storage.listLeadsByRun(runId);

    if (leads.length === 0) {
      const msg = `Resume aborted: no leads found for run ${runId}. Use Restart to re-run from scratch.`;
      log(msg, "pipeline");
      await storage.updateRun(runId, {
        status: "interrupted",
        step: "Resume aborted (no leads)",
        finishedAt: new Date(),
        logs: appendLog(run.logs || "", msg),
      });
      activeRunIds.delete(runId);
      return;
    }

    currentLogs = appendLog(run.logs || "", `--- Resume started (from after ${lastCompleted || "start"}) ---`);
    await storage.updateRun(runId, {
      status: "running",
      progress: 50,
      step: "Resuming pipeline",
      finishedAt: null,
      logs: currentLogs,
    });

    await appendAndSave(`Loaded ${leads.length} leads from run ${runId}`, 55, "Resume: Running enrichment chain");

    if (shouldRunStep(lastCompleted, PIPELINE_STEPS.APOLLO)) {
      if (params.enableApollo !== false) {
        const runLeads = await storage.listLeadsByRun(runId);
        const leadsToEnrich = runLeads
          .filter((l) => !l.email)
          .sort((a, b) => (b.score || 0) - (a.score || 0));

        if (isApolloAvailable() && leadsToEnrich.length > 0) {
          await appendAndSave(`Apollo.io: enriching ${leadsToEnrich.length} leads without email...`, 60, "Resume: Apollo enrichment");

          let enrichedCount = 0;
          let apolloSkipped = 0;
          let apolloCalls = 0;
          let apolloDeduped = 0;
          for (const lead of leadsToEnrich) {
            try {
              const currentHash = computeApolloInputHash(lead);
              if (lead.apolloEnrichedAt && lead.apolloInputHash === currentHash) {
                apolloDeduped++;
                continue;
              }

              const leaderName = lead.leaderName || lead.communityName || "";
              if (!leaderName) continue;

              if (!isValidApolloCandidate(leaderName)) {
                apolloSkipped++;
                continue;
              }

              const nameParts = leaderName.trim().split(/\s+/);
              const firstName = nameParts[0] || "";
              const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : "";
              const hasRealName = nameParts.length >= 2 && firstName.length > 1 && lastName.length > 1;

              const channels = (lead.ownedChannels as Record<string, string>) || {};
              let domain = apolloExtractDomain(lead.website || "");
              if (!domain || !apolloIsEnrichable(domain)) {
                if (channels.website) {
                  domain = apolloExtractDomain(channels.website);
                }
              }
              const enrichableDomain = domain && apolloIsEnrichable(domain) ? domain : undefined;
              const linkedinUrl = channels.linkedin && channels.linkedin.startsWith("http") ? channels.linkedin : undefined;

              apolloCalls++;
              let result = await apolloPersonMatch({
                name: leaderName,
                firstName: hasRealName ? firstName : undefined,
                lastName: hasRealName ? lastName : undefined,
                domain: enrichableDomain,
                organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
                linkedinUrl,
              });

              if (!result && hasRealName && !linkedinUrl) {
                apolloCalls++;
                result = await apolloPersonMatch({ firstName, lastName, domain: enrichableDomain });
                await new Promise((r) => setTimeout(r, 300));
              }

              if (!result) {
                await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
                continue;
              }

              const updateData: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: currentHash };
              if (result.email) updateData.email = result.email;
              if (result.phone && !lead.phone) updateData.phone = result.phone;
              if (result.linkedin && !lead.linkedin) updateData.linkedin = result.linkedin;
              if (result.location && !lead.location) updateData.location = result.location;
              if (!lead.leaderName && result.fullName) updateData.leaderName = result.fullName;

              const existingChannels = (lead.ownedChannels as Record<string, string>) || {};
              const updatedChannels = { ...existingChannels };
              if (result.twitter && !existingChannels.twitter) updatedChannels.twitter = result.twitter;
              if (result.facebook && !existingChannels.facebook) updatedChannels.facebook = result.facebook;
              if (result.linkedin && !existingChannels.linkedin) updatedChannels.linkedin = result.linkedin;
              if (Object.keys(updatedChannels).length > Object.keys(existingChannels).length) {
                updateData.ownedChannels = updatedChannels;
              }

              if (Object.keys(updateData).length > 2) {
                await storage.updateLead(lead.id, updateData);
                enrichedCount++;
              } else {
                await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: currentHash });
              }

              await new Promise((r) => setTimeout(r, 300));
            } catch (err: any) {
              await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
            }
          }
          await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped, ${apolloDeduped} deduped, ${apolloCalls} API calls)`);
        } else {
          await appendAndSave("Apollo enrichment: skipped (no API key or no eligible leads)");
        }
      } else {
        await appendAndSave("Apollo enrichment skipped (disabled by user)");
      }
      await markStepComplete(runId, PIPELINE_STEPS.APOLLO);
    } else {
      await appendAndSave("Apollo enrichment: already completed, skipping");
    }

    if (shouldRunStep(lastCompleted, PIPELINE_STEPS.LEADS_FINDER)) {
      const refreshedLeads = await storage.listLeadsByRun(runId);
      const leadsForFinder = refreshedLeads
        .filter((l) => !l.email)
        .filter((l) => {
          const channels = (l.ownedChannels as Record<string, string>) || {};
          const websiteUrl = channels.website || l.website || "";
          const domain = extractDomainFromUrl(websiteUrl);
          return domain && isEnrichableDomain(domain);
        })
        .sort((a, b) => (b.score || 0) - (a.score || 0));

      if (leadsForFinder.length > 0) {
        await appendAndSave(`Leads Finder: enriching ${leadsForFinder.length} leads by domain...`, 72, "Resume: Leads Finder enrichment");

        const domains = Array.from(new Set(
          leadsForFinder.map((l) => {
            const ch = (l.ownedChannels as Record<string, string>) || {};
            return extractDomainFromUrl(ch.website || l.website || "");
          }).filter((d) => d && isEnrichableDomain(d))
        ));

        if (domains.length > 0) {
          try {
            const { items: finderResults, costUsd: actorCost } = await runActorAndGetResults("code_crafter~leads-finder", {
              company_domain: domains,
              email_status: ["validated"],
              fetch_count: Math.min(domains.length * 5, 200),
            }, 120000, 0.0015);
            await storage.incrementApifySpend(runId, actorCost);

            const emailByDomain = new Map<string, any>();
            for (const r of finderResults) {
              const email = r.email || r.work_email || r.personal_email || "";
              if (!email || isBlockedEmail(email)) continue;
              const domain = r.company_domain || r.domain || "";
              if (domain && !emailByDomain.has(domain.toLowerCase())) {
                emailByDomain.set(domain.toLowerCase(), r);
              }
            }

            let finderEnriched = 0;
            for (const lead of leadsForFinder) {
              const ch = (lead.ownedChannels as Record<string, string>) || {};
              const websiteUrl = ch.website || lead.website || "";
              const domain = extractDomainFromUrl(websiteUrl);
              if (!domain) continue;
              const result = emailByDomain.get(domain.toLowerCase());
              if (!result) continue;
              const foundEmail = result.email || result.work_email || result.personal_email || "";
              if (!foundEmail || isBlockedEmail(foundEmail)) continue;
              const cleaned = cleanEmail(foundEmail);
              if (!cleaned) continue;

              const updateData: Record<string, any> = { email: cleaned };
              if (result.first_name && result.last_name && !lead.leaderName) {
                updateData.leaderName = `${result.first_name} ${result.last_name}`.trim();
              }
              await storage.updateLead(lead.id, updateData);
              finderEnriched++;
            }
            await appendAndSave(`Leads Finder: enriched ${finderEnriched} leads from ${domains.length} domains`);
          } catch (err: any) {
            if (err.costUsd) await storage.incrementApifySpend(runId, err.costUsd);
            await appendAndSave(`[WARN] Leads Finder failed: ${err.message}`);
          }
        }
      } else {
        await appendAndSave("Leads Finder: no eligible leads to enrich");
      }
      await markStepComplete(runId, PIPELINE_STEPS.LEADS_FINDER);
    } else {
      await appendAndSave("Leads Finder: already completed, skipping");
    }

    if (shouldRunStep(lastCompleted, PIPELINE_STEPS.EMAIL_VALIDATION)) {
      if (process.env.MILLIONVERIFIER_API_KEY) {
        await appendAndSave("Validating emails...", 80, "Resume: Email validation");

        const allLeadsForValidation = await storage.listLeadsByRun(runId);
        const leadsWithEmail = allLeadsForValidation.filter(l => l.email && !l.emailValidation);

        if (leadsWithEmail.length > 0) {
          await appendAndSave(`Email validation: verifying ${leadsWithEmail.length} emails via MillionVerifier...`);
          const emailsToVerify = leadsWithEmail.map(l => ({ email: l.email!, leadId: l.id }));

          try {
            const results = await verifyEmailBatch(emailsToVerify, async (verified, total) => {
              if (verified % 20 === 0 || verified === total) {
                await appendAndSave(`Email validation: ${verified}/${total} verified...`);
              }
            });

            let validCount = 0, invalidCount = 0, catchAllCount = 0, unknownCount = 0;
            for (const [leadId, result] of Array.from(results.entries())) {
              const validation = mapResultToValidation(result.result);
              await storage.updateLead(leadId, { emailValidation: validation });
              if (validation === "valid") validCount++;
              else if (validation === "invalid") invalidCount++;
              else if (validation === "catch-all") catchAllCount++;
              else unknownCount++;
            }
            await appendAndSave(`Email validation: ${validCount} valid, ${invalidCount} invalid, ${catchAllCount} catch-all, ${unknownCount} unknown`);
          } catch (err: any) {
            await appendAndSave(`[WARN] Email validation failed: ${err.message}`);
          }
        } else {
          await appendAndSave("Email validation: no new emails to verify");
        }
      }
      await markStepComplete(runId, PIPELINE_STEPS.EMAIL_VALIDATION);
    } else {
      await appendAndSave("Email validation: already completed, skipping");
    }

    if (shouldRunStep(lastCompleted, PIPELINE_STEPS.SCORING)) {
      await appendAndSave("Re-scoring all leads...", 90, "Resume: Scoring");
      const finalLeads = await storage.listLeadsByRun(runId);
      let reScored = 0;
      for (const lead of finalLeads) {
        const breakdown = scoreLead({
          name: lead.communityName || "",
          description: "",
          type: lead.communityType || "",
          location: lead.location || "",
          website: lead.website || "",
          email: lead.email || "",
          phone: lead.phone || "",
          linkedin: lead.linkedin || "",
          ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
          monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
          engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
          tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
          leaderName: lead.leaderName || "",
          memberCount: (lead.engagementSignals as any)?.member_count || 0,
          subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
          raw: (lead.raw as Record<string, any>) || {},
          emailValidation: lead.emailValidation || "",
        });

        await storage.updateLead(lead.id, {
          score: breakdown.total,
          scoreBreakdown: breakdown,
          lastSeenAt: new Date(),
        });
        reScored++;
      }

      const emailCount = await storage.countLeadsByRunWithEmail(runId);
      const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);
      await storage.updateRun(runId, { leadsWithEmail: emailCount, leadsWithValidEmail: validEmailCount });

      await markStepComplete(runId, PIPELINE_STEPS.SCORING);

      const dbRun2 = await storage.getRun(runId);
      const dbIsAutonomous = dbRun2?.isAutonomous === true;
      const dbEmailTarget = dbRun2?.emailTarget || 0;
      const dbBudgetUsd = dbRun2?.budgetUsd || 0;
      const dbKeywords = params.seedKeywords || [];
      const dbEnabledSources = params.enabledSources || [];
      const dbGeos = params.seedGeos || [];
      const latestValidCount = await storage.countLeadsByRunWithValidEmail(runId);

      if (dbIsAutonomous && dbEmailTarget > 0 && latestValidCount < dbEmailTarget && dbBudgetUsd > 0) {
        let expansionRound = 0;
        const usedExpKws = new Set<string>();
        const EXP_SUFFIXES = [
          ["community leader", "group organizer"],
          ["travel group", "adventure community"],
          ["retreat host", "trip organizer"],
          ["club founder", "meetup organizer"],
          ["group admin", "community founder"],
          ["event organizer", "workshop host"],
          ["group leader near me", "community events"],
          ["online community", "membership group"],
        ];

        while (true) {
          if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
          const budgetInfo = await getRunBudgetInfo(runId);
          const remainingBudget = dbBudgetUsd - budgetInfo.spentUsd;
          const curValid = await storage.countLeadsByRunWithValidEmail(runId);
          await storage.updateRun(runId, { leadsWithValidEmail: curValid });
          const deficit = dbEmailTarget - curValid;

          if (await isValidEmailTargetReached(runId)) { await appendAndSave(`DB resume expansion: Target reached: ${curValid}/${dbEmailTarget} valid emails.`); break; }
          if (remainingBudget <= 0.50) { await appendAndSave(`DB resume expansion: Budget exhausted. ${curValid}/${dbEmailTarget} valid emails.`); break; }
          if (deficit <= 3) { await appendAndSave(`DB resume expansion: Near target. ${curValid}/${dbEmailTarget} valid emails.`); break; }

          expansionRound++;
          await appendAndSave(`DB resume expansion round ${expansionRound}: ${curValid}/${dbEmailTarget} valid emails (${deficit} short). $${remainingBudget.toFixed(2)} remaining.`, 93, `Resume: Expansion round ${expansionRound}`);

          const suffixIdx = Math.min(expansionRound - 1, EXP_SUFFIXES.length - 1);
          const suffixes = EXP_SUFFIXES[suffixIdx];
          const expKws = dbKeywords.flatMap((kw: string) => suffixes.map(s => `${kw} ${s}`));
          const uniqueKws = Array.from(new Set(expKws)).filter(kw => !usedExpKws.has(kw)).slice(0, 8);
          if (uniqueKws.length === 0) { await appendAndSave(`DB resume expansion: all keyword variations exhausted.`); break; }
          for (const kw of uniqueKws) usedExpKws.add(kw);

          let newPlatformLeads: PlatformLead[] = [];
          for (const platform of dbEnabledSources) {
            if (cancelledRunIds.has(runId)) throw new RunCancelledError(runId);
            if (await isBudgetExhausted(runId, 0.10)) break;
            const maxExpLeads = Math.min(Math.ceil(deficit * 4), 500);
            try {
              if (platform === "patreon") {
                const results = await scrapePatreonCreators(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg), { minMemberCount: params.minMemberCount || 0, maxMemberCount: params.maxMemberCount || 0, minPostCount: params.minPostCount || 0 });
                newPlatformLeads.push(...results);
              } else if (platform === "facebook") {
                const results = await scrapeFacebookGroups(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg), { minMemberCount: params.minMemberCount || 0, maxMemberCount: params.maxMemberCount || 0, geos: dbGeos });
                newPlatformLeads.push(...results);
              } else if (platform === "substack") {
                const results = await scrapeSubstackWriters(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg));
                newPlatformLeads.push(...results);
              } else if (platform === "mighty") {
                const results = await scrapeMightyNetworks(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg), dbGeos);
                newPlatformLeads.push(...results);
              } else if (platform === "linkedin") {
                const results = await scrapeLinkedInGroups(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg), { minMemberCount: params.minMemberCount || 0, maxMemberCount: params.maxMemberCount || 0, geos: dbGeos });
                newPlatformLeads.push(...results);
              } else if (platform === "podcast" && dbRun2?.podcastEnabled !== false) {
                const results = await scrapeApplePodcasts(runId, uniqueKws, maxExpLeads, (msg) => appendAndSave(msg), { minEpisodeCount: params.minEpisodeCount || 0, podcastCountry: params.podcastCountry || "US" });
                newPlatformLeads.push(...results);
              }
            } catch (err: any) { await appendAndSave(`DB expansion: ${platform} error: ${err.message}`); }
          }

          if (newPlatformLeads.length === 0) { await appendAndSave(`DB expansion: no new leads, stopping.`); break; }

          const fbLeads2 = newPlatformLeads.filter(l => (l.source === "facebook" || l.source === "linkedin") && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
          if (fbLeads2.length > 0 && !(await isBudgetExhausted(runId, 0.10))) { try { await googleBridgeEnrichFacebookGroups(runId, newPlatformLeads, appendAndSave); } catch {} }
          const rssLeads2 = newPlatformLeads.filter(l => l.ownedChannels?.rss?.startsWith("http") && !l.email);
          if (rssLeads2.length > 0) { try { await enrichFromRssFeeds(runId, newPlatformLeads, appendAndSave); } catch {} }
          const ltLeads2 = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.linktree?.startsWith("http"));
          if (ltLeads2.length > 0 && !(await isBudgetExhausted(runId, 0.10))) { try { await enrichFromLinkAggregators(runId, newPlatformLeads, appendAndSave); } catch {} }

          if (!(await isBudgetExhausted(runId, 0.10))) {
            const tasks2: Promise<void>[] = [];
            if (newPlatformLeads.some(l => l.ownedChannels?.youtube?.startsWith("http"))) tasks2.push(enrichFromYouTubeAboutPages(runId, newPlatformLeads, appendAndSave));
            if (newPlatformLeads.some(l => l.ownedChannels?.instagram?.startsWith("http"))) tasks2.push(enrichFromInstagramBios(runId, newPlatformLeads, appendAndSave));
            if (newPlatformLeads.some(l => { const tw = l.ownedChannels?.twitter; return tw && (tw.startsWith("http") || tw.startsWith("@")); })) tasks2.push(enrichFromTwitterBios(runId, newPlatformLeads, appendAndSave));
            if (tasks2.length > 0) await Promise.allSettled(tasks2);
          }

          const noContact2 = newPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
          if (noContact2.length > 0 && !(await isBudgetExhausted(runId, 0.10))) { try { await googleSearchEnrichCreators(runId, newPlatformLeads, appendAndSave); } catch {} }
          const web2 = newPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
          if (web2.length > 0 && !(await isBudgetExhausted(runId, 0.10))) {
            try {
              const emailMap = await crawlCreatorWebsitesForEmails(runId, web2, appendAndSave);
              for (const pl of newPlatformLeads) { if (!pl.email && pl.ownedChannels?.website) { const d = extractDomain(pl.ownedChannels.website); if (d && emailMap.has(d)) pl.email = emailMap.get(d)!; } }
            } catch {}
          }

          let created2 = 0;
          for (const pl of newPlatformLeads) {
            try {
              let exist = null;
              if (pl.email) exist = await storage.findLeadByEmail(pl.email);
              if (!exist && pl.website) exist = await storage.findLeadByWebsite(pl.website);
              if (!exist && pl.communityName) exist = await storage.findLeadByNameAndLocation(pl.communityName, pl.location);
              if (exist) continue;
              const community = await storage.createCommunity({ name: pl.communityName, type: pl.communityType, description: pl.description, website: pl.website, ownedChannels: pl.ownedChannels, eventCadence: pl.engagementSignals, audienceSignals: pl.tripFitSignals, sourceUrls: [pl.website] });
              let leaderId2: number | undefined;
              if (pl.leaderName) { const leader = await storage.createLeader({ name: pl.leaderName, role: "", email: pl.email, phone: pl.phone, sourceUrl: pl.website, communityId: community.id }); leaderId2 = leader.id; }
              const breakdown = scoreLead({ name: pl.communityName, description: pl.description, type: pl.communityType, location: pl.location, website: pl.website, email: pl.email, phone: pl.phone, linkedin: pl.ownedChannels?.linkedin || "", ownedChannels: pl.ownedChannels, monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals, tripFitSignals: pl.tripFitSignals, leaderName: pl.leaderName, memberCount: pl.memberCount, subscriberCount: pl.subscriberCount, raw: pl.raw });
              await storage.createLead({ leadType: pl.leaderName ? "leader" : "community", communityName: pl.communityName, communityType: pl.communityType, leaderName: pl.leaderName, firstName: extractHighConfidenceFirstName(pl.leaderName), researchSummary: generateResearchSummary({ communityName: pl.communityName, communityType: pl.communityType, leaderName: pl.leaderName, description: pl.description, source: pl.source, website: pl.website, location: pl.location, ownedChannels: pl.ownedChannels, engagementSignals: pl.engagementSignals, monetizationSignals: pl.monetizationSignals, tripFitSignals: pl.tripFitSignals, raw: pl.raw }), location: pl.location, website: pl.website, email: pl.email, phone: pl.phone, ownedChannels: pl.ownedChannels, monetizationSignals: pl.monetizationSignals, engagementSignals: pl.engagementSignals, tripFitSignals: pl.tripFitSignals, score: breakdown.total, scoreBreakdown: breakdown, status: "new", source: pl.source || "", raw: pl.raw, runId, communityId: community.id, leaderId: leaderId2 });
              created2++;
            } catch {}
          }
          await appendAndSave(`DB expansion: created ${created2} new leads`);
          if (created2 === 0) { await appendAndSave(`DB expansion: all duplicates, stopping.`); break; }

          if (params.enableApollo !== false && isApolloAvailable() && !(await isBudgetExhausted(runId, 0.10))) {
            const dbLeads3 = await storage.listLeadsByRun(runId);
            const apolloLeads3 = dbLeads3.filter(l => !l.email && !l.apolloEnrichedAt);
            for (const lead of apolloLeads3) {
              try {
                if (await isBudgetExhausted(runId, 0.05)) break;
                const hash = computeApolloInputHash(lead);
                if (lead.apolloEnrichedAt && lead.apolloInputHash === hash) continue;
                const name = lead.leaderName || lead.communityName || "";
                if (!name || !isValidApolloCandidate(name)) continue;
                const parts = name.trim().split(/\s+/);
                const fn = parts[0], ln = parts.length > 1 ? parts.slice(1).join(" ") : "";
                const hasName = parts.length >= 2 && fn.length > 1 && ln.length > 1;
                const ch = (lead.ownedChannels as Record<string, string>) || {};
                let dom = apolloExtractDomain(lead.website || "");
                if (!dom || !apolloIsEnrichable(dom)) { if (ch.website) dom = apolloExtractDomain(ch.website); }
                const eDom = dom && apolloIsEnrichable(dom) ? dom : undefined;
                const li = ch.linkedin?.startsWith("http") ? ch.linkedin : undefined;
                let res = await apolloPersonMatch({ name, firstName: hasName ? fn : undefined, lastName: hasName ? ln : undefined, domain: eDom, linkedinUrl: li });
                if (!res && hasName && !li) { res = await apolloPersonMatch({ firstName: fn, lastName: ln, domain: eDom }); await new Promise(r => setTimeout(r, 300)); }
                if (!res) { await storage.updateLead(lead.id, { apolloEnrichedAt: new Date(), apolloInputHash: hash }); continue; }
                const ud: Record<string, any> = { apolloEnrichedAt: new Date(), apolloInputHash: hash };
                if (res.email && !isBlockedEmail(res.email)) ud.email = res.email;
                if (res.phone) ud.phone = res.phone;
                if (res.linkedin) ud.linkedin = res.linkedin;
                await storage.updateLead(lead.id, ud);
                await new Promise(r => setTimeout(r, 300));
              } catch {}
            }
          }

          const expValid3 = (await storage.listLeadsByRun(runId)).filter(l => l.email && !l.emailValidation);
          if (expValid3.length > 0 && process.env.MILLIONVERIFIER_API_KEY) {
            try {
              const emailsV = expValid3.map(l => ({ email: l.email!, leadId: l.id }));
              const vRes = await verifyEmailBatch(emailsV, async () => {});
              for (const [lid, r] of Array.from(vRes.entries())) { await storage.updateLead(lid, { emailValidation: mapResultToValidation(r.result) }); }
            } catch {}
          }

          if (isHubspotConfigured()) {
            try {
              const dbExpHubLeads = (await storage.listLeadsByRun(runId)).filter(l => l.email && l.emailValidation === "valid" && !l.hubspotStatus);
              if (dbExpHubLeads.length > 0) {
                const emailMap = new Map(dbExpHubLeads.map(l => [l.email!.toLowerCase(), l.id]));
                const hubResults = await checkEmailsInHubspot(Array.from(emailMap.keys()));
                let existing = 0, netNew = 0;
                for (const [email, isExisting] of Array.from(hubResults.entries())) {
                  const leadId = emailMap.get(email.toLowerCase());
                  if (leadId) {
                    await storage.updateLead(leadId, { hubspotStatus: isExisting ? "existing" : "net_new" });
                    if (isExisting) existing++; else netNew++;
                  }
                }
                await appendAndSave(`DB expansion HubSpot: ${existing} existing, ${netNew} net new`);
              }
            } catch {}
          }

          const pv = await storage.countLeadsByRunWithValidEmail(runId);
          const pe = await storage.countLeadsByRunWithEmail(runId);
          await storage.updateRun(runId, { leadsWithEmail: pe, leadsWithValidEmail: pv, leadsExtracted: (await storage.listLeadsByRun(runId)).length });
          await appendAndSave(`DB expansion round ${expansionRound} complete: ${pv}/${dbEmailTarget} valid emails.`, 95);
          if (await isValidEmailTargetReached(runId)) break;
        }
      }

      const finalEmailCount = await storage.countLeadsByRunWithEmail(runId);
      const finalValidCount = await storage.countLeadsByRunWithValidEmail(runId);
      const totalLeads = (await storage.listLeadsByRun(runId)).length;

      await storage.updateRun(runId, {
        status: "succeeded",
        progress: 100,
        step: "Complete",
        finishedAt: new Date(),
        leadsWithEmail: finalEmailCount,
        leadsWithValidEmail: finalValidCount,
        logs: appendLog(currentLogs, `Resume complete! ${totalLeads} leads, ${finalEmailCount} with email, ${finalValidCount} validated.`),
      });

      log(`Resume of run ${runId} completed successfully`, "pipeline");
    }
  } catch (err: any) {
    if (err instanceof RunCancelledError || cancelledRunIds.has(runId)) {
      log(`Resume of run ${runId} stopped by user`, "pipeline");
      const emailCount = await storage.countLeadsByRunWithEmail(runId);
      const validEmailCount = await storage.countLeadsByRunWithValidEmail(runId);
      await storage.updateRun(runId, {
        status: "stopped",
        step: "Stopped by user",
        leadsWithEmail: emailCount,
        leadsWithValidEmail: validEmailCount,
        logs: appendLog(currentLogs, `Resume stopped by user. ${emailCount} leads with email preserved.`),
        finishedAt: new Date(),
      });
    } else {
      log(`Resume of run ${runId} failed: ${err.message}`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Resume failed",
        logs: appendLog(currentLogs, `[ERROR] Resume failed: ${err.message}`),
        finishedAt: new Date(),
      });
    }
  } finally {
    if (stopHeartbeat) stopHeartbeat();
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
    resumingRunIds.delete(runId);
  }
}
