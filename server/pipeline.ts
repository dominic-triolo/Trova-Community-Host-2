import { storage } from "./storage";
import { runActorAndGetResults } from "./apify";
import { scoreLead } from "./scoring";
import { apolloPersonMatch, isApolloAvailable, extractDomainFromUrl as apolloExtractDomain, isEnrichableDomain as apolloIsEnrichable } from "./apollo";
import { extractDomainFromUrl, isEnrichableDomain } from "./hunter";
import { log } from "./index";
import type { RunParams, InsertSourceUrl, InsertLead, InsertLeader } from "@shared/schema";

export const activeRunIds = new Set<number>();
export const cancelledRunIds = new Set<number>();

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

function extractEmailsFromText(text: string): string[] {
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const matches = text.match(emailRegex) || [];
  return Array.from(new Set(matches)).filter(
    (e) => !e.endsWith(".png") && !e.endsWith(".jpg") && !e.endsWith(".gif")
  );
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
  ];
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      if (match[3]) {
        results.push(`${match[1]}@${match[2]}.${match[3]}`);
      } else if (match[2]) {
        results.push(`${match[1]}@${match[2]}`);
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

async function scrapeMeetupGroups(
  runId: number,
  keywords: string[],
  geos: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const searchUrls: string[] = [];

  const locations = geos.length > 0 ? geos : [""];
  for (const kw of keywords) {
    for (const geo of locations) {
      const locationParam = geo ? `&location=us--${encodeURIComponent(geo)}` : "";
      searchUrls.push(
        `https://www.meetup.com/find/?keywords=${encodeURIComponent(kw)}${locationParam}&source=GROUPS&distance=anyDistance`
      );
    }
  }

  const urlBatches: string[][] = [];
  for (let i = 0; i < searchUrls.length; i += 5) {
    urlBatches.push(searchUrls.slice(i, i + 5));
  }

  for (const batch of urlBatches) {
    try {
      await appendAndSave(`Meetup: searching ${batch.length} queries...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("easyapi~meetup-groups-scraper", {
        searchUrls: batch,
        maxItems: Math.min(maxItems - leads.length, 200),
      }, 300000);
      await storage.incrementApifySpend(runId, actorCost);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        const memberCount = item.stats?.memberCounts?.all || 0;
        const description = item.description || "";
        const fullText = `${item.name || ""} ${description}`;
        const city = item.city || "";
        const country = item.country || "";
        const location = [city, item.state, country].filter(Boolean).join(", ");

        const organizer = item.organizer || item.organizerProfile || {};
        const organizerName = organizer.name || item.organizerName || "";
        const organizerBio = organizer.bio || organizer.description || "";
        const organizerEmail = extractEmailsFromText(`${description} ${organizerBio}`)[0] || "";

        const meetupChannels: Record<string, string> = { meetup: item.link || "active" };
        const socialsText = `${description} ${organizerBio}`;
        if (socialsText.includes("instagram.com")) meetupChannels.instagram = "detected";
        if (socialsText.includes("facebook.com")) meetupChannels.facebook = "detected";
        if (socialsText.includes("discord")) meetupChannels.discord = "detected";

        leads.push({
          source: "meetup",
          communityName: item.name || "",
          communityType: detectCommunityType(fullText),
          description,
          location,
          website: item.link || "",
          email: organizerEmail,
          phone: "",
          leaderName: organizerName,
          memberCount,
          subscriberCount: 0,
          ownedChannels: meetupChannels,
          monetizationSignals: detectMonetization(description),
          engagementSignals: {
            ...detectEngagement(description),
            member_count: memberCount,
            attendance_proxy: memberCount,
          },
          tripFitSignals: detectTripFit(description),
          raw: item,
        });
      }

      await appendAndSave(`Meetup: found ${leads.length} groups so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Meetup batch failed: ${err.message}`);
    }
  }

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
    /([\d,.]+)\s*[Kk]\s*(?:members|people|followers)/i,
    /([\d,.]+)\s*[Mm](?:illion)?\s*(?:members|people|followers)/i,
    /([\d,.]+)\s*(?:members|people|followers)/i,
    /(?:members|people|followers)[:\s]*([\d,.]+)\s*[Kk]?/i,
  ];
  for (const pat of memberPatterns) {
    const m = text.match(pat);
    if (m) {
      const raw = m[1].replace(/,/g, "");
      let num = parseFloat(raw);
      const afterNum = text.substring(text.indexOf(m[0]) + m[0].indexOf(m[1]) + m[1].length, text.indexOf(m[0]) + m[0].length);
      if (/^\s*[Kk]/.test(afterNum)) num *= 1000;
      else if (/^\s*[Mm]/.test(afterNum)) num *= 1000000;
      return Math.round(num);
    }
  }
  return 0;
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

async function scrapeFacebookGroups(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minMemberCount?: number; maxMemberCount?: number } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenGroupUrls = new Set<string>();
  const minMembers = filters.minMemberCount || 0;
  const maxMembers = filters.maxMemberCount || 0;

  const googleQueries = keywords.map(kw => `site:facebook.com/groups "${kw}"`);
  const batchSize = 5;

  for (let i = 0; i < googleQueries.length; i += batchSize) {
    if (leads.length >= maxItems) break;
    const batch = googleQueries.slice(i, i + batchSize);

    try {
      await appendAndSave(`Facebook (via Google): searching ${batch.length} queries...`);
      const { items, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.join("\n"),
        maxPagesPerQuery: 2,
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

          const groupName = title
            .replace(/\s*\|\s*Facebook$/i, "")
            .replace(/\s*[-–—]\s*Facebook$/i, "")
            .replace(/\s*Facebook\s*$/i, "")
            .trim();

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

      await appendAndSave(`Facebook: found ${leads.length} groups so far${filteredOut > 0 ? ` (${filteredOut} filtered by member count)` : ""}`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Facebook Google search failed: ${err.message}`);
    }
  }

  return leads;
}

async function scrapeApplePodcasts(
  runId: number,
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
  filters: { minEpisodeCount?: number; podcastCountry?: string } = {},
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];
  const seenPodcastIds = new Set<string>();
  const minEpisodes = filters.minEpisodeCount || 0;
  const country = filters.podcastCountry || "US";

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
  await appendAndSave(`Podcasts: searching ${dedupedKeywords.length} keywords in ${country} store: ${keywordPreview}`);

  const maxResultsPerQuery = Math.max(10, Math.min(200, Math.ceil(maxItems * 1.5 / dedupedKeywords.length)));

  for (const kw of dedupedKeywords) {
    if (leads.length >= maxItems) break;

    try {
      await appendAndSave(`Podcasts: searching "${kw}" (max ${maxResultsPerQuery} results)...`);

      const { items, costUsd: actorCost } = await runActorAndGetResults("benthepythondev~podcast-intelligence-aggregator", {
        mode: "search",
        searchQuery: kw,
        country: country,
        maxResults: maxResultsPerQuery,
        includeEpisodes: false,
      }, 180000);
      await storage.incrementApifySpend(runId, actorCost);

      let skippedDupe = 0;
      let skippedFilter = 0;

      for (const item of items) {
        if (leads.length >= maxItems) break;

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

        leads.push({
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
        });
      }

      await appendAndSave(`Podcasts: ${leads.length} podcasts found so far (skipped ${skippedDupe} dupes, ${skippedFilter} filtered by episode count)`);
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
  await appendAndSave(`Podcasts: ${leads.length} total (${withWebsite} with website, ${withRss} with RSS feed, ${withEmail} with email from description)`);

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
      maxConcurrency: 5,
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

const GOOGLE_ENRICHMENT_MAX = Infinity;
const GOOGLE_ENRICHMENT_SOCIAL_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com", "yelp.com", "tripadvisor.com", "bbb.org"];

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

    if (hasRealName) {
      queries.push({ term: `"${realName}" email website`, leadIdx: idx });
      queries.push({ term: `site:linkedin.com "${realName}"`, leadIdx: idx });
    } else if (isValidApolloCandidate(brandName)) {
      queries.push({ term: `"${brandName}" contact website email`, leadIdx: idx });
      queries.push({ term: `site:linkedin.com "${brandName}"`, leadIdx: idx });
    } else {
      const patreonUrl = l.website || l.ownedChannels?.patreon || "";
      if (patreonUrl.includes("patreon.com/")) {
        const slug = patreonUrl.split("patreon.com/")[1]?.split(/[?#/]/)[0];
        if (slug && slug.length >= 3) {
          queries.push({ term: `"${slug}" email contact website`, leadIdx: idx });
        }
      } else if (brandName.length >= 3) {
        queries.push({ term: `"${brandName}" contact email`, leadIdx: idx });
      }
    }
  }

  const batchSize = 5;
  const enrichedLeadIndices = new Set<number>();

  for (let i = 0; i < queries.length; i += batchSize) {
    const batch = queries.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(queries.length / batchSize);

    await appendAndSave(`Google enrichment: batch ${batchNum}/${totalBatches} (${batch.length} searches)...`);

    try {
      const searchQueries = batch.map(q => ({ term: q.term, countryCode: "us", languageCode: "en", maxPagesPerQuery: 1, resultsPerPage: 5 }));

      const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: searchQueries.map(q => q.term).join("\n"),
        maxPagesPerQuery: 1,
        resultsPerPage: 5,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      const resultsByQuery = new Map<number, any[]>();
      for (const r of results) {
        const searchQuery = r.searchQuery?.term || r.searchQuery || "";
        const matchIdx = batch.findIndex(q => q.term === searchQuery);
        if (matchIdx >= 0) {
          const existing = resultsByQuery.get(matchIdx) || [];
          existing.push(r);
          resultsByQuery.set(matchIdx, existing);
        }
      }

      for (let j = 0; j < batch.length; j++) {
        const lead = leads[batch[j].leadIdx];
        const searchResults = resultsByQuery.get(j) || results.filter(r => {
          const q = r.searchQuery?.term || r.searchQuery || "";
          return q.includes(lead.communityName || lead.leaderName || "NOMATCH");
        });

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

          const emails = extractEmailsFromText(fullResult);
          if (emails.length > 0 && !lead.email) {
            lead.email = emails[0];
            foundAnything = true;
          }
        }

        if (foundAnything) enrichedLeadIndices.add(batch[j].leadIdx);
      }
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Google enrichment batch ${batchNum} failed: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 1000));
  }

  await appendAndSave(`Google enrichment: found new data for ${enrichedLeadIndices.size}/${leadsToSearch.length} creators`);
}

async function googleBridgeEnrichFacebookGroups(
  runId: number,
  leads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const fbLeads = leads.filter(l => l.source === "facebook" && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
  if (fbLeads.length === 0) {
    await appendAndSave("Google Bridge: all Facebook leads already have website/LinkedIn or email");
    return;
  }

  await appendAndSave(`Google Bridge: searching for leaders/orgs behind ${fbLeads.length} Facebook groups...`);

  const queries: { term: string; leadIdx: number }[] = [];
  for (const l of fbLeads) {
    const groupName = l.communityName || "";
    if (!groupName || groupName.length < 3) continue;
    const idx = leads.indexOf(l);

    queries.push({ term: `"${groupName}" website contact email`, leadIdx: idx });

    if (l.leaderName && l.leaderName.length > 2) {
      queries.push({ term: `"${l.leaderName}" "${groupName}" linkedin OR email`, leadIdx: idx });
    } else {
      queries.push({ term: `"${groupName}" organizer OR founder OR leader linkedin`, leadIdx: idx });
    }
  }

  const batchSize = 5;
  const enrichedLeadIndices = new Set<number>();

  for (let i = 0; i < queries.length; i += batchSize) {
    const batch = queries.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(queries.length / batchSize);

    await appendAndSave(`Google Bridge: batch ${batchNum}/${totalBatches} (${batch.length} searches)...`);

    try {
      const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~google-search-scraper", {
        queries: batch.map(q => q.term).join("\n"),
        maxPagesPerQuery: 1,
        resultsPerPage: 5,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);
      await storage.incrementApifySpend(runId, actorCost);

      const resultsByQuery = new Map<number, any[]>();
      for (const r of results) {
        const searchQuery = r.searchQuery?.term || r.searchQuery || "";
        const matchIdx = batch.findIndex(q => q.term === searchQuery);
        if (matchIdx >= 0) {
          const existing = resultsByQuery.get(matchIdx) || [];
          existing.push(r);
          resultsByQuery.set(matchIdx, existing);
        }
      }

      for (let j = 0; j < batch.length; j++) {
        const lead = leads[batch[j].leadIdx];
        const searchResults = resultsByQuery.get(j) || results.filter(r => {
          const q = r.searchQuery?.term || r.searchQuery || "";
          return q.includes(lead.communityName || "NOMATCH");
        });

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

            const nameMatch = title.match(/^([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*[-|–·]/);
            if (nameMatch && !lead.leaderName && nameMatch[1].length > 4) {
              lead.leaderName = nameMatch[1].trim();
              foundAnything = true;
            }
          } catch {}

          const emails = extractEmailsFromText(fullResult);
          if (emails.length > 0 && !lead.email) {
            lead.email = emails[0];
            foundAnything = true;
          }
        }

        if (foundAnything) enrichedLeadIndices.add(batch[j].leadIdx);
      }
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Google Bridge batch ${batchNum} failed: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 1000));
  }

  await appendAndSave(`Google Bridge: found new data for ${enrichedLeadIndices.size}/${fbLeads.length} Facebook groups`);
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
    const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~cheerio-scraper", {
      startUrls,
      maxCrawlPages: leadsWithAggregator.length,
      maxConcurrency: 3,
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
    const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~cheerio-scraper", {
      startUrls,
      maxCrawlPages: leadsWithYouTube.length,
      maxConcurrency: 3,
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
    const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~instagram-profile-scraper", {
      usernames,
    }, 120000);
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
        if (words.length >= 2 && words.every((w: string) => /^[A-Z][a-z]+$/.test(w))) {
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
    const { items: results, costUsd: actorCost } = await runActorAndGetResults("apidojo~twitter-user-scraper", {
      twitterHandles: paddedHandles,
      getFollowers: false,
      getFollowing: false,
      getRetweeters: false,
      includeUnavailableUsers: false,
      maxItems: Math.max(handles.length + 5, 10),
    }, 120000);
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
        if (words.length >= 2 && words.every((w: string) => /^[A-Z][a-z]+$/.test(w))) {
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

  const batchSize = 5;
  for (let i = 0; i < websiteEntries.length; i += batchSize) {
    const batch = websiteEntries.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(websiteEntries.length / batchSize);

    await appendAndSave(`Website crawl: processing batch ${batchNum}/${totalBatches} (${batch.length} sites)...`);

    const startUrls: { url: string }[] = [];
    const globs: { glob: string }[] = [];

    for (const [domain, url] of batch) {
      const baseUrl = url.replace(/\/+$/, "");
      startUrls.push({ url: baseUrl });
      const subpages = ["/contact", "/about", "/about-us", "/contact-us", "/team"];
      for (const page of subpages) {
        startUrls.push({ url: `${baseUrl}${page}` });
      }
      globs.push({ glob: `https://${domain}/contact*` });
      globs.push({ glob: `https://${domain}/about*` });
      globs.push({ glob: `https://www.${domain}/contact*` });
      globs.push({ glob: `https://www.${domain}/about*` });
    }

    try {
      const { items: results, costUsd: actorCost } = await runActorAndGetResults("apify~cheerio-scraper", {
        startUrls,
        globs,
        maxCrawlPages: 5 * batch.length,
        maxConcurrency: 3,
        pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  const text = $('body').text();
  return { url: request.url, text: text.substring(0, 5000) };
}`,
      }, 180000);
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

      await appendAndSave(`Website crawl batch ${batchNum}: found ${emailMap.size} total emails so far`);
    } catch (err: any) {
      if (err.costUsd) {
        await storage.incrementApifySpend(runId, err.costUsd);
      }
      await appendAndSave(`[WARN] Website crawl batch ${batchNum} failed: ${err.message}`);
    }
  }

  await appendAndSave(`Website crawl complete: found ${emailMap.size} emails from ${websiteEntries.length} websites`);
  return emailMap;
}

export async function runPipeline(runId: number): Promise<void> {
  let currentLogs = "";

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
    await storage.updateRun(runId, { status: "running", startedAt: new Date() });

    await appendAndSave("Pipeline started", 2, "Step 1: Platform-specific discovery");

    const keywords = params.seedKeywords;
    const geos = params.seedGeos;
    const enabledSources = params.enabledSources || ["meetup", "youtube", "reddit", "eventbrite", "google"];
    const platformSources = enabledSources.filter((s) => s !== "google");
    const maxPerPlatform = Math.floor(params.maxDiscoveredUrls / Math.max(1, platformSources.length));

    const platformTasks: { name: string; promise: Promise<PlatformLead[]> }[] = [];
    if (enabledSources.includes("meetup")) {
      platformTasks.push({ name: "Meetup", promise: scrapeMeetupGroups(runId, keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("youtube")) {
      platformTasks.push({ name: "YouTube", promise: scrapeYouTubeChannels(runId, keywords, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("reddit")) {
      platformTasks.push({ name: "Reddit", promise: scrapeRedditCommunities(runId, keywords, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("eventbrite")) {
      platformTasks.push({ name: "Eventbrite", promise: scrapeEventbriteEvents(runId, keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("facebook")) {
      platformTasks.push({ name: "Facebook", promise: scrapeFacebookGroups(runId, keywords, maxPerPlatform, (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
      }) });
    }
    if (enabledSources.includes("patreon")) {
      platformTasks.push({ name: "Patreon", promise: scrapePatreonCreators(runId, keywords, maxPerPlatform, (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
        minPostCount: params.minPostCount || 0,
      }) });
    }
    if (enabledSources.includes("podcast")) {
      platformTasks.push({ name: "Podcasts", promise: scrapeApplePodcasts(runId, keywords, maxPerPlatform, (msg) => appendAndSave(msg), {
        minEpisodeCount: params.minEpisodeCount || 0,
        podcastCountry: params.podcastCountry || "US",
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

    const realNameCount = allPlatformLeads.filter(l => {
      const aboutText = l.raw?.about || "";
      return extractRealNameFromAbout(aboutText, l.communityName || "") !== null;
    }).length;
    if (realNameCount > 0) {
      await appendAndSave(`Real name extraction: found real names for ${realNameCount}/${allPlatformLeads.length} leads from about text`);
    }

    const hasFacebookLeads = allPlatformLeads.some(l => l.source === "facebook");
    if (hasFacebookLeads) {
      const fbLeadsNeedingEnrich = allPlatformLeads.filter(l => l.source === "facebook" && !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
      if (fbLeadsNeedingEnrich.length > 0) {
        await appendAndSave(`Google Bridge: ${fbLeadsNeedingEnrich.length} Facebook groups need leader/org lookup`, 29, "Step 1b: Google Bridge for Facebook groups");
        await googleBridgeEnrichFacebookGroups(runId, allPlatformLeads, appendAndSave);
      } else {
        await appendAndSave("Google Bridge: skipped (all Facebook leads already have contact paths)");
      }
    }

    const hasPodcastLeads = allPlatformLeads.some(l => l.source === "podcast");
    if (hasPodcastLeads) {
      const podcastLeadsWithRss = allPlatformLeads.filter(l => l.source === "podcast" && l.ownedChannels?.rss);
      if (podcastLeadsWithRss.length > 0) {
        await appendAndSave(`RSS feed scrape: ${podcastLeadsWithRss.length} podcasts have RSS feeds for email extraction`, 29, "Step 1c: RSS feed email extraction");
        await enrichFromRssFeeds(runId, allPlatformLeads, appendAndSave);
      }
    }

    const leadsWithLinktreeInitial = allPlatformLeads.filter(l =>
      !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http")
    );
    if (leadsWithLinktreeInitial.length > 0) {
      await appendAndSave(`Link aggregator scrape: ${leadsWithLinktreeInitial.length} leads have Linktree/Beacons pages`, 28, "Step 1b: Link aggregator scrape");
      await enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave);
    }

    const leadsWithYouTube = allPlatformLeads.filter(l =>
      l.ownedChannels?.youtube && l.ownedChannels.youtube.startsWith("http")
    );
    if (leadsWithYouTube.length > 0) {
      await appendAndSave(`YouTube about pages: ${leadsWithYouTube.length} leads have YouTube channels`, 30, "Step 2a: YouTube about page scrape");
      await enrichFromYouTubeAboutPages(runId, allPlatformLeads, appendAndSave);
    }

    const leadsWithInstagram = allPlatformLeads.filter(l =>
      l.ownedChannels?.instagram && l.ownedChannels.instagram.startsWith("http")
    );
    if (leadsWithInstagram.length > 0) {
      await appendAndSave(`Instagram bios: ${leadsWithInstagram.length} leads have Instagram profiles`, 31, "Step 2b: Instagram bio scrape");
      await enrichFromInstagramBios(runId, allPlatformLeads, appendAndSave);
    }

    const leadsWithTwitter = allPlatformLeads.filter(l => {
      const tw = l.ownedChannels?.twitter;
      return tw && (tw.startsWith("http") || tw.startsWith("@"));
    });
    if (leadsWithTwitter.length > 0) {
      await appendAndSave(`Twitter bios: ${leadsWithTwitter.length} leads have Twitter/X profiles`, 32, "Step 2c: Twitter/X bio scrape");
      await enrichFromTwitterBios(runId, allPlatformLeads, appendAndSave);
    }

    const newLinktreeLeads = allPlatformLeads.filter(l =>
      !l.email && l.ownedChannels?.linktree && l.ownedChannels.linktree.startsWith("http") &&
      !leadsWithLinktreeInitial.includes(l)
    );
    if (newLinktreeLeads.length > 0) {
      await appendAndSave(`Link aggregator scrape (pass 2): ${newLinktreeLeads.length} new aggregator URLs from YouTube/IG/Twitter`, 33, "Step 2d: Link aggregator scrape (new)");
      await enrichFromLinkAggregators(runId, allPlatformLeads, appendAndSave);
    }

    const leadsWithoutContactInfo = allPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
    if (leadsWithoutContactInfo.length > 0) {
      await appendAndSave(`Google enrichment: ${leadsWithoutContactInfo.length} leads need website/LinkedIn lookup`, 35, "Step 2e: Google contact search");
      await googleSearchEnrichCreators(runId, allPlatformLeads, appendAndSave);
    } else {
      await appendAndSave("Google enrichment: skipped (all leads already have contact info)");
    }

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
            maxConcurrency: 10,
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
          linkedin: "",
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
          location: pl.location,
          website: pl.website,
          email: pl.email,
          phone: pl.phone,
          ownedChannels: pl.ownedChannels,
          monetizationSignals: pl.monetizationSignals,
          engagementSignals: pl.engagementSignals,
          tripFitSignals: pl.tripFitSignals,
          score: breakdown.total,
          scoreBreakdown: breakdown,
          status: "new",
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

    const runLeads = await storage.listLeadsByRun(runId);
    const APOLLO_MIN_SCORE = 15;
    const leadsToEnrich = runLeads
      .filter((l) => !l.email && (l.score || 0) >= APOLLO_MIN_SCORE)
      .sort((a, b) => (b.score || 0) - (a.score || 0));
    let enrichedCount = 0;

    if (params.enableApollo !== false) {
      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        const totalWithoutEmail = runLeads.filter((l) => !l.email).length;
        const skippedLowScore = totalWithoutEmail - leadsToEnrich.length;
        await appendAndSave(`Apollo.io: enriching ${leadsToEnrich.length} of ${totalWithoutEmail} leads without email (${skippedLowScore} below score ${APOLLO_MIN_SCORE})...`);

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
          }, 120000);
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

    await appendAndSave("Finalizing...", 92, "Step 10: Finalizing");

    const emailCount = await storage.countLeadsByRunWithEmail(runId);

    await storage.updateRun(runId, {
      leadsWithEmail: emailCount,
    });

    const sourcesUsed = (params.enabledSources || []).map((s: string) => {
      const labels: Record<string, string> = { patreon: "Patreon", meetup: "Meetup", youtube: "YouTube", reddit: "Reddit", eventbrite: "Eventbrite", facebook: "Facebook", google: "Google Search" };
      return labels[s] || s;
    });

    await appendAndSave(
      `Scoring complete: ${createdCount} leads, ${emailCount} with email`,
      96,
      "Step 10: Finalizing"
    );

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Complete",
      finishedAt: new Date(),
      logs: appendLog(
        currentLogs,
        `Pipeline complete! ${createdCount} leads discovered, ${emailCount} with email. Sources: ${sourcesUsed.join(", ")}.`
      ),
    });

    log(`Pipeline run ${runId} completed successfully`, "pipeline");
  } catch (err: any) {
    if (err instanceof RunCancelledError || cancelledRunIds.has(runId)) {
      log(`Pipeline run ${runId} cancelled by user`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Cancelled by user",
        logs: appendLog(currentLogs, `Run cancelled by user`),
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
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
  }
}

export async function reEnrichRun(runId: number): Promise<void> {
  const run = await storage.getRun(runId);
  if (!run) throw new Error(`Run ${runId} not found`);

  const params = run.params as RunParams;
  let currentLogs = "";

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
      const RE_APOLLO_MIN_SCORE = 15;
      const leadsToEnrich = refreshedLeads
        .filter((l) => (!l.email || l.email === "") && (l.score || 0) >= RE_APOLLO_MIN_SCORE)
        .sort((a, b) => (b.score || 0) - (a.score || 0));
      const totalWithoutEmail = refreshedLeads.filter((l) => !l.email || l.email === "").length;

      await appendAndSave(`Step 2: Apollo enrichment for ${leadsToEnrich.length} of ${totalWithoutEmail} leads without email...`, 50, "Re-enrichment: Apollo enrichment");

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
          }, 120000);
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

    await appendAndSave(`Step 5: Re-scoring all leads...`, 80, "Re-enrichment: Scoring");
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
      });

      await storage.updateLead(lead.id, {
        score: breakdown.total,
        scoreBreakdown: breakdown,
        lastSeenAt: new Date(),
      });
      reScored++;
    }

    const emailCount = await storage.countLeadsByRunWithEmail(runId);

    await storage.updateRun(runId, {
      leadsWithEmail: emailCount,
    });

    await appendAndSave(
      `Re-enrichment complete! ${reScored} leads re-scored, ${emailCount} have emails`,
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
      log(`Re-enrichment of run ${runId} cancelled by user`, "pipeline");
      await storage.updateRun(runId, {
        status: "failed",
        step: "Cancelled by user",
        logs: appendLog(currentLogs, `Run cancelled by user`),
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
    activeRunIds.delete(runId);
    cancelledRunIds.delete(runId);
  }
}
