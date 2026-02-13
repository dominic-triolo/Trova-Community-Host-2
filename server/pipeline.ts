import { storage } from "./storage";
import { runActorAndGetResults } from "./apify";
import { scoreLead, determineStatus } from "./scoring";
import { apolloPersonMatch, isApolloAvailable, extractDomainFromUrl as apolloExtractDomain, isEnrichableDomain as apolloIsEnrichable } from "./apollo";
import { hunterDomainSearch, extractDomainFromUrl, isEnrichableDomain, isHunterAvailable } from "./hunter";
import { log } from "./index";
import type { RunParams, InsertSourceUrl, InsertLead, InsertLeader } from "@shared/schema";

export const activeRunIds = new Set<number>();

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

function normalizePatreonUrl(url: string): string {
  if (!url) return "";
  let normalized = url.split("?")[0].split("#")[0].toLowerCase().trim();
  normalized = normalized.replace(/^http:\/\//, "https://");
  normalized = normalized.replace(/^https?:\/\/www\./, "https://");
  if (!normalized.startsWith("https://")) {
    if (normalized.startsWith("patreon.com")) normalized = "https://" + normalized;
  }
  normalized = normalized.replace(/\/+$/, "");
  return normalized;
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
      const items = await runActorAndGetResults("easyapi~meetup-groups-scraper", {
        searchUrls: batch,
        maxItems: Math.min(maxItems - leads.length, 200),
      }, 300000);

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
      await appendAndSave(`[WARN] Meetup batch failed: ${err.message}`);
    }
  }

  return leads;
}

async function scrapeYouTubeChannels(
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  for (const kw of keywords) {
    if (leads.length >= maxItems) break;
    try {
      await appendAndSave(`YouTube: searching for "${kw}"...`);
      const items = await runActorAndGetResults("streamers~youtube-scraper", {
        searchQueries: [kw],
        maxResults: Math.min(50, maxItems - leads.length),
        maxResultsShorts: 0,
        maxResultStreams: 0,
        proxyConfiguration: { useApifyProxy: true },
      }, 180000);

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
      await appendAndSave(`[WARN] YouTube search failed for "${kw}": ${err.message}`);
    }
  }

  return leads;
}

async function scrapeRedditCommunities(
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  try {
    await appendAndSave(`Reddit: searching ${keywords.length} keywords...`);
    const items = await runActorAndGetResults("trudax~reddit-scraper-lite", {
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
    await appendAndSave(`[WARN] Reddit search failed: ${err.message}`);
  }

  return leads;
}

async function scrapeEventbriteEvents(
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
      const items = await runActorAndGetResults("aitorsm~eventbrite", {
        country: "united-states",
        city: city || "all",
        category: "custom",
        keyword: kw,
        maxItems: Math.min(50, maxItems - leads.length),
      }, 300000);

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

function expandKeywordsForPatreon(keywords: string[]): string[] {
  const suffixes = [
    "",
    "community",
    "creator",
    "podcast",
    "club",
    "group",
    "travel",
    "adventure",
    "outdoor",
    "fitness",
    "wellness",
  ];

  const expanded: string[] = [];
  const seen = new Set<string>();

  for (const kw of keywords) {
    const base = kw.toLowerCase().trim();
    if (!seen.has(base)) {
      seen.add(base);
      expanded.push(kw);
    }
  }

  for (const kw of keywords) {
    for (const suffix of suffixes) {
      if (!suffix) continue;
      const words = kw.toLowerCase().trim().split(/\s+/);
      if (words.includes(suffix)) continue;
      const combo = `${kw} ${suffix}`.trim();
      const key = combo.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        expanded.push(combo);
      }
    }
  }

  return expanded;
}

async function scrapePatreonEmails(
  keywords: string[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<Map<string, string>> {
  const emailMap = new Map<string, string>();

  try {
    await appendAndSave(`Email scraper: searching Patreon for emails with ${keywords.length} keywords...`);

    for (const kw of keywords) {
      try {
        const items = await runActorAndGetResults("scraper-mind~all-social-media-email-scraper", {
          keywords: [kw],
          platform: "Patreon",
          customDomains: ["@gmail.com", "@yahoo.com", "@hotmail.com", "@outlook.com", "@icloud.com", "@protonmail.com", "@aol.com", "@me.com", "@live.com", "@mail.com"],
          proxyConfiguration: { useApifyProxy: true },
        }, 180000);

        let batchEmails = 0;
        for (const item of items) {
          const email = cleanEmail(item.email || "");
          if (!email) continue;

          const patreonUrl = normalizePatreonUrl(item.url || "");
          if (patreonUrl && patreonUrl.includes("patreon.com") && !emailMap.has(patreonUrl)) {
            emailMap.set(patreonUrl, email);
            batchEmails++;
          }
        }

        await appendAndSave(`Email scraper: "${kw}" found ${batchEmails} emails (${emailMap.size} total)`);
      } catch (err: any) {
        await appendAndSave(`[WARN] Email scraper failed for "${kw}": ${err.message}`);
      }
    }

    await appendAndSave(`Email scraper complete: ${emailMap.size} emails found across all keywords`);
  } catch (err: any) {
    await appendAndSave(`[WARN] Email scraper failed: ${err.message}`);
  }

  return emailMap;
}

function isValidApolloCandidate(leaderName: string): boolean {
  if (!leaderName) return false;
  const nameParts = leaderName.trim().split(/\s+/);
  if (nameParts.length < 2) return false;
  if (nameParts[0].length <= 1 || nameParts[1].length <= 1) return false;
  const lower = leaderName.toLowerCase();
  const brandIndicators = [
    "podcast", "walking is", "the ", "adventures of",
    " radio", " tv", " show",
  ];
  if (brandIndicators.some((b) => lower.startsWith(b) || lower.startsWith("the "))) {
    if (lower.startsWith("the ")) return false;
  }
  const exactBrandEndings = [" podcast", " radio", " show", " tv"];
  if (exactBrandEndings.some((e) => lower.endsWith(e))) return false;
  const allCaps = nameParts.length >= 3 && nameParts.every((p) => p === p.toUpperCase() && p.length > 2);
  if (allCaps) return false;
  return true;
}

async function scrapePatreonCreators(
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
  }

  const expandedKeywords = expandKeywordsForPatreon(keywords);
  await appendAndSave(`Patreon: expanded ${keywords.length} keywords to ${expandedKeywords.length} search queries`);

  for (const kw of expandedKeywords) {
    if (leads.length >= maxItems) break;
    try {
      await appendAndSave(`Patreon: searching for "${kw}"...`);
      const searchUrl = `https://www.patreon.com/search?q=${encodeURIComponent(kw)}`;
      const items = await runActorAndGetResults("powerai~patreon-creators-search-scraper", {
        searchUrl,
        maxItems: Math.min(50, maxItems - leads.length),
        proxyConfiguration: { useApifyProxy: true, apifyProxyGroups: ["RESIDENTIAL"] },
      }, 180000);

      let skippedDupe = 0;
      let skippedFilter = 0;

      for (const item of items) {
        if (leads.length >= maxItems) break;

        const creatorName = item.creatorName || item.name || item.title || item.fullName || "";
        if (!creatorName) continue;

        const creatorUrl = (item.creatorUrl || item.url || item.profileUrl || "").split("?")[0];
        if (creatorUrl && seenUrls.has(creatorUrl.toLowerCase())) {
          skippedDupe++;
          continue;
        }

        const description = item.description || item.about || item.summary || "";
        const memberCount = parsePatreonCount(item.membersCount || item.patronCount || item.memberCount || item.patrons);
        const postCount = parsePatreonCount(item.postsCount || item.postCount || 0);

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
        const socialsText = `${description} ${JSON.stringify(item.socialLinks || item.socials || {})}`;
        if (socialsText.includes("youtube.com")) channels.youtube = "detected";
        if (socialsText.includes("instagram.com")) channels.instagram = "detected";
        if (socialsText.includes("twitter.com") || socialsText.includes("x.com")) channels.twitter = "detected";
        if (socialsText.includes("discord")) channels.discord = "detected";
        if (socialsText.includes("facebook.com")) channels.facebook = "detected";

        const creatorEmail = item.email || extractEmailsFromText(description)[0] || "";
        const creatorWebsite = item.website || item.externalUrl || "";
        if (creatorWebsite) channels.website = creatorWebsite;

        const monetization: Record<string, any> = { patreon: true };
        if (item.tiers || item.membershipTiers) monetization.paid_membership = true;
        if (item.isMonthly !== undefined) monetization.recurring = true;

        leads.push({
          source: "patreon",
          communityName: creatorName,
          communityType: detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location: "",
          website: creatorUrl || creatorWebsite || "",
          email: creatorEmail,
          phone: "",
          leaderName: creatorName,
          memberCount,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: { ...monetization, ...detectMonetization(description) },
          engagementSignals: {
            member_count: memberCount,
            post_count: postCount,
            attendance_proxy: memberCount,
            recurring: true,
          },
          tripFitSignals: detectTripFit(fullText),
          raw: item,
        });
      }

      await appendAndSave(`Patreon: ${leads.length} creators (skipped ${skippedDupe} dupes, ${skippedFilter} filtered)`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Patreon search failed for "${kw}": ${err.message}`);
    }
  }

  return leads;
}

async function crawlPatreonProfiles(
  platformLeads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const patreonLeads = platformLeads.filter(
    (l) => l.source === "patreon" && l.website && l.website.includes("patreon.com")
  );
  if (patreonLeads.length === 0) return;

  await appendAndSave(`Crawling ${patreonLeads.length} Patreon profiles for contact info...`);

  const batchSize = 5;
  let profilesEnriched = 0;
  let emailsFound = 0;
  let websitesFound = 0;

  for (let i = 0; i < patreonLeads.length; i += batchSize) {
    const batch = patreonLeads.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(patreonLeads.length / batchSize);

    try {
      const items = await runActorAndGetResults("apify~puppeteer-scraper", {
        startUrls: batch.map((l) => ({ url: l.website })),
        maxRequestsPerCrawl: batch.length,
        maxConcurrency: 3,
        maxRequestRetries: 2,
        useChrome: true,
        proxyConfiguration: { useApifyProxy: true, apifyProxyGroups: ["RESIDENTIAL"] },
        preNavigationHooks: `[
          async ({ page }, goToOptions) => {
            goToOptions.waitUntil = 'networkidle2';
            goToOptions.timeout = 60000;
          }
        ]`,
        pageFunction: `async function pageFunction(context) {
          const { request, page, log } = context;

          await page.waitForTimeout(3000);

          try {
            await page.waitForSelector('a[href]', { timeout: 10000 });
          } catch(e) {
            log.info('No links found after waiting, page may not have loaded fully');
          }

          const data = await page.evaluate(() => {
            var blockedDomains = ['patreon.com','example.com','sentry.io','cloudflare.com','w3.org','schema.org','googleapis.com','gstatic.com'];
            function isBlockedEmail(e) {
              if (!e) return true;
              var lower = e.toLowerCase();
              return blockedDomains.some(function(d) { return lower.endsWith('@' + d); });
            }

            var emails = [];
            document.querySelectorAll('a[href^="mailto:"]').forEach(function(el) {
              var href = el.getAttribute('href') || '';
              var email = href.replace('mailto:', '').split('?')[0].trim();
              if (email && email.includes('@') && !isBlockedEmail(email)) emails.push(email);
            });

            var bodyText = document.body ? document.body.innerText : '';
            var emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
            var textEmails = bodyText.match(emailRegex) || [];
            textEmails.forEach(function(e) {
              if (e && !e.endsWith('.png') && !e.endsWith('.jpg') && !e.endsWith('.gif') && !isBlockedEmail(e) && emails.indexOf(e) === -1) {
                emails.push(e);
              }
            });

            var externalLinks = [];
            var socialLinks = {};
            document.querySelectorAll('a[href]').forEach(function(el) {
              var rawHref = (el.getAttribute('href') || '').trim();
              if (!rawHref || rawHref === '#' || rawHref.startsWith('javascript:')) return;
              var href;
              try { href = new URL(rawHref, document.baseURI).href; } catch(e) { return; }
              if (!href.startsWith('http')) return;
              if (href.includes('linkedin.com/in/')) socialLinks.linkedin = href;
              else if (href.includes('twitter.com/') || href.includes('x.com/')) socialLinks.twitter = href;
              else if (href.includes('instagram.com/')) socialLinks.instagram = href;
              else if (href.includes('facebook.com/')) socialLinks.facebook = href;
              else if (href.includes('youtube.com/') || href.includes('youtu.be/')) socialLinks.youtube = href;
              else if (href.includes('discord.gg') || href.includes('discord.com/')) socialLinks.discord = href;
              else if (href.includes('tiktok.com/')) socialLinks.tiktok = href;
              else if (href.startsWith('http') && !href.includes('patreon.com') && !href.includes('google.com') && !href.includes('apple.com') && !href.includes('spotify.com')) {
                externalLinks.push(href);
              }
            });

            var creatorName = '';
            var nameEl = document.querySelector('[data-tag="creator-name"]');
            if (nameEl) creatorName = nameEl.textContent.trim();
            if (!creatorName) {
              var h1 = document.querySelector('h1');
              if (h1) creatorName = h1.textContent.trim();
            }
            if (!creatorName) {
              var ogTitle = document.querySelector('meta[property="og:title"]');
              if (ogTitle) creatorName = (ogTitle.getAttribute('content') || '').replace(/ \\| creating.*| is creating.*/i, '').trim();
            }

            var aboutText = '';
            document.querySelectorAll('[data-tag="about-section"], [class*="about"], [class*="About"]').forEach(function(el) {
              aboutText += ' ' + el.textContent;
            });
            if (!aboutText.trim()) {
              var metaDesc = document.querySelector('meta[name="description"]');
              if (metaDesc) aboutText = metaDesc.getAttribute('content') || '';
            }
            if (!aboutText.trim()) {
              var ogDesc = document.querySelector('meta[property="og:description"]');
              if (ogDesc) aboutText = ogDesc.getAttribute('content') || '';
            }

            var socialPlatforms = ['patreon.com','google.com','apple.com','microsoft.com','spotify.com','amazon.com','facebook.com','twitter.com','x.com','instagram.com','youtube.com','tiktok.com','discord.com','discord.gg','reddit.com','tumblr.com','pinterest.com','linkedin.com','twitch.tv','github.com','medium.com','wordpress.com','blogspot.com','bit.ly','linktr.ee','beacons.ai','carrd.co','ko-fi.com','buymeacoffee.com','gumroad.com','etsy.com','redbubble.com','teepublic.com','paypal.com','venmo.com','cashapp.com','substack.com'];
            var personalWebsite = '';
            for (var i = 0; i < externalLinks.length; i++) {
              if (personalWebsite) break;
              try {
                var u = new URL(externalLinks[i]);
                var dominated = socialPlatforms.some(function(d) { return u.hostname.includes(d); });
                if (!dominated) personalWebsite = externalLinks[i];
              } catch(e) {}
            }

            return {
              emails: emails,
              socialLinks: socialLinks,
              personalWebsite: personalWebsite,
              externalLinks: externalLinks.slice(0, 20),
              creatorName: creatorName,
              aboutText: (aboutText || '').substring(0, 2000),
            };
          });

          return {
            url: request.url,
            ...data,
          };
        }`,
      }, 360000);

      for (const item of items) {
        const matchingLead = batch.find((l) => {
          const leadUrl = l.website.split("?")[0].toLowerCase().replace(/\/$/, "");
          const itemUrl = (item.url || "").split("?")[0].toLowerCase().replace(/\/$/, "");
          return leadUrl === itemUrl;
        });
        if (!matchingLead) continue;

        let changed = false;

        if (item.emails && item.emails.length > 0 && !matchingLead.email) {
          const validEmail = item.emails.map((e: string) => cleanEmail(e)).find((e: string) => e);
          if (validEmail) {
            matchingLead.email = validEmail;
            emailsFound++;
            changed = true;
          }
        }

        if (item.personalWebsite) {
          const channels = matchingLead.ownedChannels || {};
          channels.website = item.personalWebsite;
          matchingLead.ownedChannels = channels;
          websitesFound++;
          changed = true;
        }

        if (item.socialLinks) {
          const channels = matchingLead.ownedChannels || {};
          for (const [key, val] of Object.entries(item.socialLinks as Record<string, string>)) {
            if (val && !channels[key]) {
              channels[key] = val;
              changed = true;
            }
          }
          if (item.socialLinks.linkedin) {
            matchingLead.ownedChannels = channels;
          }
          matchingLead.ownedChannels = channels;
        }

        if (!matchingLead.email && item.aboutText) {
          const aboutEmails = extractEmailsFromText(item.aboutText);
          if (aboutEmails.length > 0) {
            matchingLead.email = aboutEmails[0];
            emailsFound++;
            changed = true;
          }
        }

        let realName = "";
        if (item.creatorName && item.creatorName.length > 2 && item.creatorName.includes(" ") && item.creatorName.length < 60) {
          realName = item.creatorName;
        }
        if (!realName && item.aboutText) {
          const namePatterns = [
            /(?:I'm|I am|My name is|Hi,? I'm|Hey,? I'm)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/,
            /(?:This is|Created by|Founded by|By)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/,
          ];
          for (const pattern of namePatterns) {
            const match = item.aboutText.match(pattern);
            if (match && match[1] && match[1].length < 40) {
              realName = match[1].trim();
              break;
            }
          }
        }
        if (realName) {
          const currentName = matchingLead.leaderName || "";
          const isBrandName = currentName === matchingLead.communityName || !currentName.includes(" ");
          if (isBrandName) {
            matchingLead.leaderName = realName;
            changed = true;
          }
        }

        if (changed) profilesEnriched++;
      }

      await appendAndSave(`Profile crawl batch ${batchNum}/${totalBatches}: enriched ${profilesEnriched} profiles, ${websitesFound} websites, ${emailsFound} emails so far (${items.length} pages crawled)`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Patreon profile crawl batch ${batchNum} failed: ${err.message}`);
    }
  }

  await appendAndSave(`Patreon profiles: ${websitesFound} websites found, ${emailsFound} emails found, ${profilesEnriched} profiles enriched`);
}

function normalizeUrl(url: string): string {
  if (!url) return "";
  let normalized = url.split("#")[0].split("?")[0].toLowerCase().trim();
  normalized = normalized.replace(/^http:\/\//, "https://");
  normalized = normalized.replace(/^https?:\/\/www\./, "https://");
  normalized = normalized.replace(/\/+$/, "");
  return normalized;
}

async function crossPlatformEmailLookup(
  platformLeads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const supportedHosts = ["youtube.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "tiktok.com", "twitch.tv", "linkedin.com"];

  const leadsWithSocialUrls: { lead: PlatformLead; urls: string[] }[] = [];
  const globalDedup = new Set<string>();

  for (const lead of platformLeads) {
    if (lead.email) continue;
    const channels = lead.ownedChannels || {};
    const urls: string[] = [];
    for (const [key, val] of Object.entries(channels)) {
      if (!val || key === "website" || key === "patreon") continue;
      if (typeof val !== "string" || !val.startsWith("http")) continue;
      try {
        const host = new URL(val).hostname.replace(/^www\./, "");
        if (!supportedHosts.some((h) => host === h || host.endsWith("." + h))) continue;
      } catch {
        continue;
      }
      const norm = normalizeUrl(val);
      if (norm && !globalDedup.has(norm)) {
        globalDedup.add(norm);
        urls.push(val);
      }
    }
    if (urls.length > 0) {
      leadsWithSocialUrls.push({ lead, urls });
    }
  }

  if (leadsWithSocialUrls.length === 0) {
    await appendAndSave("Cross-platform email lookup: no social URLs found to check");
    return;
  }

  const allUrls = leadsWithSocialUrls.flatMap((l) => l.urls);
  await appendAndSave(`Cross-platform email lookup: checking ${allUrls.length} unique social URLs from ${leadsWithSocialUrls.length} leads...`);

  const urlToEmail = new Map<string, string>();
  const batchSize = 25;
  let totalFound = 0;

  for (let i = 0; i < allUrls.length; i += batchSize) {
    const batch = allUrls.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(allUrls.length / batchSize);

    try {
      const items = await runActorAndGetResults("scraper-mind~all-social-media-email-scraper", {
        urls: batch.map((url) => ({ url })),
        proxyConfiguration: { useApifyProxy: true, apifyProxyGroups: ["RESIDENTIAL"] },
      }, 120000);

      for (const item of items) {
        const email = cleanEmail(item.email || "");
        if (!email) continue;
        const norm = normalizeUrl(item.url || "");
        if (norm && !urlToEmail.has(norm)) {
          urlToEmail.set(norm, email);
          totalFound++;
        }
      }

      await appendAndSave(`Cross-platform batch ${batchNum}/${totalBatches}: found ${totalFound} emails so far`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Cross-platform batch ${batchNum} failed: ${err.message}`);
    }
  }

  let merged = 0;
  for (const { lead, urls } of leadsWithSocialUrls) {
    if (lead.email) continue;
    for (const url of urls) {
      const norm = normalizeUrl(url);
      if (urlToEmail.has(norm)) {
        lead.email = urlToEmail.get(norm)!;
        merged++;
        break;
      }
    }
  }

  await appendAndSave(`Cross-platform email lookup: found ${totalFound} emails, merged ${merged} into leads`);
}

async function crawlCreatorWebsites(
  platformLeads: PlatformLead[],
  appendAndSave: (msg: string) => Promise<void>,
): Promise<void> {
  const skipDomains = ["patreon.com", "youtube.com", "instagram.com", "facebook.com", "twitter.com", "x.com", "tiktok.com", "discord.com", "discord.gg", "reddit.com", "linkedin.com", "linktr.ee", "beacons.ai", "tumblr.com", "pinterest.com", "twitch.tv", "github.com", "medium.com", "wordpress.com"];

  const leadsWithWebsites = platformLeads.filter((l) => {
    if (l.email) return false;
    const channels = l.ownedChannels || {};
    let website = channels.website || "";
    if (!website || !website.startsWith("http")) {
      website = l.website || "";
    }
    if (!website || !website.startsWith("http")) return false;
    const domain = extractDomain(website);
    if (!domain) return false;
    if (skipDomains.some((s) => domain.includes(s))) return false;
    if (!channels.website) channels.website = website;
    l.ownedChannels = channels;
    return true;
  });

  if (leadsWithWebsites.length === 0) return;

  await appendAndSave(`Crawling ${leadsWithWebsites.length} creator websites for email addresses...`);

  const batchSize = 15;
  let emailsFound = 0;

  for (let i = 0; i < leadsWithWebsites.length; i += batchSize) {
    const batch = leadsWithWebsites.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(leadsWithWebsites.length / batchSize);

    try {
      const startUrls = batch.map((l) => ({ url: l.ownedChannels.website }));

      const items = await runActorAndGetResults("apify~cheerio-scraper", {
        startUrls,
        maxRequestsPerCrawl: batch.length * 4,
        maxConcurrency: 10,
        maxRequestRetries: 1,
        linkSelector: "a[href]",
        pseudoUrls: startUrls.map((su) => {
          try {
            const base = new URL(su.url);
            return { purl: `${base.origin}/[.*]` };
          } catch {
            return { purl: su.url };
          }
        }),
        pageFunction: `async function pageFunction(context) {
          const { request, $, log, enqueueLinks } = context;
          var bodyText = $('body').text().replace(/\\s+/g, ' ').substring(0, 8000);

          var blockedDomains = ['patreon.com','example.com','sentry.io','cloudflare.com','w3.org','schema.org','googleapis.com','gstatic.com'];
          function isBlockedEmail(e) {
            if (!e) return true;
            var lower = e.toLowerCase();
            return blockedDomains.some(function(d) { return lower.endsWith('@' + d); });
          }

          var emails = [];
          $('a[href^="mailto:"]').each(function() {
            var href = $(this).attr('href') || '';
            var email = href.replace('mailto:', '').split('?')[0].trim();
            if (email && email.includes('@') && !isBlockedEmail(email)) emails.push(email);
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

          var contactPatterns = /\\b(contact|about|team|staff|who-we-are|connect|get-in-touch)\\b/i;
          if (!request.userData || !request.userData.isSubpage) {
            var contactLinks = [];
            $('a[href]').each(function() {
              var href = $(this).attr('href') || '';
              var text = $(this).text().toLowerCase().trim();
              if (contactPatterns.test(href) || contactPatterns.test(text)) {
                try { contactLinks.push(new URL(href, request.url).href); } catch(e) {}
              }
            });
            var unique = contactLinks.filter(function(v, i, a) { return a.indexOf(v) === i; }).slice(0, 3);
            if (unique.length > 0) {
              try {
                await enqueueLinks({ urls: unique, userData: { isSubpage: true, parentUrl: request.url } });
              } catch(e) {}
            }
          }

          return {
            url: request.url,
            parentUrl: (request.userData && request.userData.parentUrl) || request.url,
            isSubpage: !!(request.userData && request.userData.isSubpage),
            bodyText: bodyText + ' ' + footerText,
            emails: emails,
            schemaEmails: schemaEmails,
          };
        }`,
      }, 180000);

      const siteEmails = new Map<string, string[]>();

      for (const item of items) {
        const rootUrl = item.parentUrl || item.url;
        const rootDomain = extractDomain(rootUrl);
        if (!rootDomain) continue;

        const allEmails: string[] = siteEmails.get(rootDomain) || [];

        if (item.emails) allEmails.push(...item.emails);
        if (item.schemaEmails) allEmails.push(...item.schemaEmails);

        const textEmails = extractEmailsFromText(item.bodyText || "");
        allEmails.push(...textEmails);

        siteEmails.set(rootDomain, allEmails);
      }

      for (const lead of batch) {
        if (lead.email) continue;
        const websiteUrl = lead.ownedChannels.website || "";
        const domain = extractDomain(websiteUrl);
        if (!domain) continue;

        const emails = siteEmails.get(domain) || [];
        const unique = Array.from(new Set(emails))
          .map((e) => cleanEmail(e))
          .filter((e) => e && !e.endsWith(".png") && !e.endsWith(".jpg") && !e.endsWith(".gif"));

        if (unique.length > 0) {
          lead.email = unique[0];
          emailsFound++;
        }
      }

      await appendAndSave(`Website crawl batch ${batchNum}/${totalBatches}: ${emailsFound} emails found so far`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Website crawl batch ${batchNum} failed: ${err.message}`);
    }
  }

  await appendAndSave(`Website crawl complete: found ${emailsFound} emails from creator websites`);
}

async function scrapeFacebookGroups(
  keywords: string[],
  maxItems: number,
  appendAndSave: (msg: string) => Promise<void>,
): Promise<PlatformLead[]> {
  const leads: PlatformLead[] = [];

  for (const kw of keywords) {
    if (leads.length >= maxItems) break;
    try {
      await appendAndSave(`Facebook: searching for "${kw}"...`);
      const items = await runActorAndGetResults("apify/facebook-groups-scraper", {
        searchType: "groups",
        searchQuery: kw,
        maxGroups: Math.min(50, maxItems - leads.length),
        maxPostsPerGroup: 0,
      }, 180000);

      for (const item of items) {
        if (leads.length >= maxItems) break;

        const groupName = item.name || item.title || "";
        const description = item.description || item.about || "";
        const memberCount = item.membersCount || item.members || 0;
        const url = item.url || item.groupUrl || "";
        const fullText = `${groupName} ${description}`;

        const channels: Record<string, string> = { facebook: url || "active" };
        if (description.toLowerCase().includes("discord")) channels.discord = "detected";
        if (description.toLowerCase().includes("newsletter")) channels.newsletter = "detected";

        leads.push({
          source: "facebook",
          communityName: groupName,
          communityType: detectCommunityType(fullText),
          description: description.substring(0, 2000),
          location: item.location || "",
          website: url,
          email: extractEmailsFromText(description)[0] || "",
          phone: "",
          leaderName: item.adminName || "",
          memberCount,
          subscriberCount: 0,
          ownedChannels: channels,
          monetizationSignals: detectMonetization(description),
          engagementSignals: {
            member_count: memberCount,
            attendance_proxy: memberCount,
            ...detectEngagement(description),
          },
          tripFitSignals: detectTripFit(fullText),
          raw: item,
        });
      }

      await appendAndSave(`Facebook: found ${leads.length} groups so far`);
    } catch (err: any) {
      await appendAndSave(`[WARN] Facebook search failed for "${kw}": ${err.message}`);
    }
  }

  return leads;
}

export async function runPipeline(runId: number): Promise<void> {
  let currentLogs = "";

  const appendAndSave = async (msg: string, progress?: number, step?: string) => {
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
      platformTasks.push({ name: "Meetup", promise: scrapeMeetupGroups(keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("youtube")) {
      platformTasks.push({ name: "YouTube", promise: scrapeYouTubeChannels(keywords, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("reddit")) {
      platformTasks.push({ name: "Reddit", promise: scrapeRedditCommunities(keywords, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("eventbrite")) {
      platformTasks.push({ name: "Eventbrite", promise: scrapeEventbriteEvents(keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("facebook")) {
      platformTasks.push({ name: "Facebook", promise: scrapeFacebookGroups(keywords, maxPerPlatform, (msg) => appendAndSave(msg)) });
    }
    if (enabledSources.includes("patreon")) {
      platformTasks.push({ name: "Patreon", promise: scrapePatreonCreators(keywords, maxPerPlatform, (msg) => appendAndSave(msg), {
        minMemberCount: params.minMemberCount || 0,
        maxMemberCount: params.maxMemberCount || 0,
        minPostCount: params.minPostCount || 0,
      }) });
    }

    let emailScraperPromise: Promise<Map<string, string>> | null = null;
    if (enabledSources.includes("patreon")) {
      emailScraperPromise = scrapePatreonEmails(keywords, (msg) => appendAndSave(msg));
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

    if (emailScraperPromise) {
      try {
        const emailMap = await emailScraperPromise;
        let emailsMerged = 0;
        for (const pl of allPlatformLeads) {
          if (pl.email) continue;
          if (pl.source !== "patreon") continue;

          const candidateUrls: string[] = [];
          const mainUrl = normalizePatreonUrl(pl.website || "");
          if (mainUrl && mainUrl.includes("patreon.com")) candidateUrls.push(mainUrl);
          const patreonChannel = normalizePatreonUrl(pl.ownedChannels?.patreon || "");
          if (patreonChannel && patreonChannel.includes("patreon.com") && patreonChannel !== mainUrl) candidateUrls.push(patreonChannel);

          for (const url of candidateUrls) {
            if (emailMap.has(url)) {
              pl.email = emailMap.get(url)!;
              emailsMerged++;
              break;
            }
          }
        }
        await appendAndSave(`Email scraper: merged ${emailsMerged} emails into platform leads (map has ${emailMap.size} entries)`);
      } catch (err: any) {
        await appendAndSave(`[WARN] Email scraper merge failed: ${err.message}`);
      }
    }

    await appendAndSave(`Platform discovery complete: ${allPlatformLeads.length} results`, 35, "Step 2: Profile & website crawl");

    await crawlPatreonProfiles(allPlatformLeads, (msg) => appendAndSave(msg));
    await crossPlatformEmailLookup(allPlatformLeads, (msg) => appendAndSave(msg));
    await crawlCreatorWebsites(allPlatformLeads, (msg) => appendAndSave(msg));

    const emailsAfterCrawl = allPlatformLeads.filter((l) => l.email).length;
    await appendAndSave(`After crawl: ${emailsAfterCrawl}/${allPlatformLeads.length} leads have emails`, 45, "Step 3: Google Search discovery");

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

        const items = await runActorAndGetResults("apify~google-search-scraper", {
          queries: batch.join("\n"),
          maxPagesPerQuery: 1,
          resultsPerPage: params.maxGoogleResultsPerQuery,
          languageCode: "en",
          mobileResults: false,
        }, 120000);

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
        await appendAndSave(`[ERROR] Google batch ${batchIdx + 1} failed: ${err.message}`);
      }

      const progress = 35 + Math.round((batchIdx / queryBatches.length) * 15);
      await appendAndSave(`Progress update`, progress);
    }

    await appendAndSave(`Google discovery complete: ${allDiscoveredUrls.length} website URLs`);
    } else {
      await appendAndSave("Google Search skipped (not selected)");
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
      "Step 4: Extract website data"
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

          const items = await runActorAndGetResults("apify~cheerio-scraper", {
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
          await appendAndSave(`[ERROR] Web extraction batch ${batchNum} failed: ${err.message}`);
        }

        const progress = 50 + Math.round((batchNum / totalBatches) * 15);
        await appendAndSave(`Progress update`, progress);
      }
    }

    await appendAndSave(
      `Extraction complete: ${extractedPages.length} pages + ${allPlatformLeads.length} platform results`,
      65,
      "Step 5: Create & score leads"
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
        const hasContact = !!(pl.email || pl.website || pl.phone);
        const status = determineStatus(breakdown.total, params.threshold, hasContact);

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
          status,
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
        const hasContact = !!(email || url || phones[0] || linkedinUrl);
        const status = determineStatus(breakdown.total, params.threshold, hasContact);

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
          status,
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

    await appendAndSave(`Created ${createdCount} total leads`, 80, "Step 6: Contact enrichment");

    const runLeads = await storage.listLeadsByRun(runId);
    const leadsToEnrich = runLeads.filter((l) => !l.email);
    let enrichedCount = 0;

    if (isApolloAvailable() && leadsToEnrich.length > 0) {
      await appendAndSave(`Apollo.io: enriching ${leadsToEnrich.length} leads missing contact info...`);

      let apolloSkipped = 0;
      for (const lead of leadsToEnrich) {
        try {
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

          let result = await apolloPersonMatch({
            name: leaderName,
            firstName: hasRealName ? firstName : undefined,
            lastName: hasRealName ? lastName : undefined,
            domain: enrichableDomain,
            organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
            linkedinUrl,
          });

          if (!result && hasRealName && !linkedinUrl) {
            result = await apolloPersonMatch({
              firstName,
              lastName,
              domain: enrichableDomain,
            });
            await new Promise((r) => setTimeout(r, 300));
          }

          if (!result) continue;

          const updateData: Record<string, any> = {};

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

          if (Object.keys(updateData).length === 0) continue;

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

          const hasContact = !!(updateData.email || lead.website || updateData.phone || updateData.linkedin);
          updateData.score = breakdown.total;
          updateData.scoreBreakdown = breakdown;
          updateData.status = determineStatus(breakdown.total, params.threshold, hasContact);

          await storage.updateLead(lead.id, updateData);
          enrichedCount++;

          await new Promise((r) => setTimeout(r, 300));
        } catch (err: any) {
          await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
        }
      }

      await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped - invalid names)`);
    } else if (isHunterAvailable() && leadsToEnrich.length > 0) {
      await appendAndSave(`Hunter.io fallback: enriching ${leadsToEnrich.length} leads missing emails...`);
      const enrichedDomains = new Set<string>();

      for (const lead of leadsToEnrich) {
        const domain = extractDomainFromUrl(lead.website || "");
        if (!domain || !isEnrichableDomain(domain) || enrichedDomains.has(domain)) continue;
        enrichedDomains.add(domain);

        const result = await hunterDomainSearch(domain);
        if (!result || result.emails.length === 0) continue;

        const bestEmail = result.emails.sort((a, b) => b.confidence - a.confidence)[0];
        const updateData: Record<string, any> = { email: bestEmail.value };
        if (bestEmail.phone_number && !lead.phone) updateData.phone = bestEmail.phone_number;
        if (bestEmail.linkedin && !lead.linkedin) updateData.linkedin = bestEmail.linkedin;
        if (!lead.leaderName && bestEmail.first_name && bestEmail.last_name) {
          updateData.leaderName = `${bestEmail.first_name} ${bestEmail.last_name}`;
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
          ownedChannels: (lead.ownedChannels as Record<string, string>) || {},
          monetizationSignals: (lead.monetizationSignals as Record<string, any>) || {},
          engagementSignals: (lead.engagementSignals as Record<string, any>) || {},
          tripFitSignals: (lead.tripFitSignals as Record<string, any>) || {},
          leaderName: updateData.leaderName || lead.leaderName || "",
          memberCount: (lead.engagementSignals as any)?.member_count || 0,
          subscriberCount: (lead.engagementSignals as any)?.subscriber_count || 0,
          raw: (lead.raw as Record<string, any>) || {},
        });

        const hasContact = !!(updateData.email || lead.website || updateData.phone || updateData.linkedin);
        updateData.score = breakdown.total;
        updateData.scoreBreakdown = breakdown;
        updateData.status = determineStatus(breakdown.total, params.threshold, hasContact);

        await storage.updateLead(lead.id, updateData);
        enrichedCount++;
        await new Promise((r) => setTimeout(r, 200));
      }

      await appendAndSave(`Hunter.io: enriched ${enrichedCount} leads from ${enrichedDomains.size} domains`);
    } else {
      await appendAndSave("Email enrichment: skipped (no Apollo or Hunter API key configured)");
    }

    await appendAndSave("Recalculating scores...", 90, "Step 7: Scoring & qualification");

    const qualifiedCount = await storage.countLeadsByRunAndStatus(runId, "qualified");
    const watchlistCount = await storage.countLeadsByRunAndStatus(runId, "watchlist");

    await storage.updateRun(runId, {
      qualified: qualifiedCount,
      watchlist: watchlistCount,
    });

    await appendAndSave(
      `Scoring complete: ${qualifiedCount} qualified, ${watchlistCount} watchlist`,
      95,
      "Step 8: Finalizing"
    );

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Complete",
      finishedAt: new Date(),
      logs: appendLog(
        currentLogs,
        `Pipeline complete! ${createdCount} leads (${qualifiedCount} qualified, ${watchlistCount} watchlist). Sources: Meetup, YouTube, Reddit, Eventbrite + Google Search.`
      ),
    });

    log(`Pipeline run ${runId} completed successfully`, "pipeline");
  } catch (err: any) {
    log(`Pipeline run ${runId} failed: ${err.message}`, "pipeline");
    await storage.updateRun(runId, {
      status: "failed",
      step: "Failed",
      logs: appendLog(currentLogs, `[ERROR] Pipeline failed: ${err.message}`),
      finishedAt: new Date(),
    });
  } finally {
    activeRunIds.delete(runId);
  }
}

export async function reEnrichRun(runId: number): Promise<void> {
  const run = await storage.getRun(runId);
  if (!run) throw new Error(`Run ${runId} not found`);

  const params = run.params as RunParams;
  let currentLogs = "";

  const appendAndSave = async (msg: string, progress?: number, step?: string) => {
    currentLogs = appendLog(currentLogs, msg);
    const update: Record<string, any> = { logs: currentLogs };
    if (progress !== undefined) update.progress = progress;
    if (step) update.step = step;
    await storage.updateRun(runId, update);
  };

  try {
    activeRunIds.add(runId);
    await storage.updateRun(runId, {
      status: "running",
      progress: 0,
      step: "Re-enrichment: Loading leads",
      startedAt: new Date(),
      finishedAt: null,
      logs: "",
    });

    const leads = await storage.listLeadsByRun(runId);
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

    await appendAndSave(`Step 1: Email scraper for Patreon...`, 5, "Re-enrichment: Email scraper");
    const patreonKeywords = (params.seedKeywords || []);
    if (patreonKeywords.length > 0) {
      try {
        const emailMap = await scrapePatreonEmails(patreonKeywords, appendAndSave);
        let emailsMerged = 0;
        for (const pl of platformLeads) {
          if (pl.email) continue;

          const candidateUrls: string[] = [];
          const mainUrl = normalizePatreonUrl(pl.website || "");
          if (mainUrl && mainUrl.includes("patreon.com")) candidateUrls.push(mainUrl);
          const patreonChannel = normalizePatreonUrl(pl.ownedChannels?.patreon || "");
          if (patreonChannel && patreonChannel.includes("patreon.com") && patreonChannel !== mainUrl) candidateUrls.push(patreonChannel);

          for (const url of candidateUrls) {
            if (emailMap.has(url)) {
              pl.email = emailMap.get(url)!;
              emailsMerged++;
              break;
            }
          }
        }
        await appendAndSave(`Email scraper: merged ${emailsMerged} emails into leads (map has ${emailMap.size} entries)`);
      } catch (err: any) {
        await appendAndSave(`[WARN] Email scraper failed: ${err.message}`);
      }
    }

    await appendAndSave(`Step 2: Crawling Patreon profiles...`, 15, "Re-enrichment: Profile crawl");
    await crawlPatreonProfiles(platformLeads, appendAndSave);
    await appendAndSave(`Profile crawl complete`, 25);

    await appendAndSave(`Step 2b: Cross-platform email lookup...`, 28, "Re-enrichment: Cross-platform emails");
    await crossPlatformEmailLookup(platformLeads, appendAndSave);
    await appendAndSave(`Cross-platform lookup complete`, 32);

    await appendAndSave(`Step 3: Crawling creator websites...`, 35, "Re-enrichment: Website crawl");
    await crawlCreatorWebsites(platformLeads, appendAndSave);
    await appendAndSave(`Website crawl complete`, 55);

    await appendAndSave(`Step 4: Updating leads in database...`, 60, "Re-enrichment: Saving crawl results");
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
    await appendAndSave(`Updated ${crawlUpdated} leads from crawl data`, 65);

    const refreshedLeads = await storage.listLeadsByRun(runId);
    const leadsToEnrich = refreshedLeads.filter((l) => !l.email || l.email === "");

    await appendAndSave(`Step 5: Apollo enrichment for ${leadsToEnrich.length} leads without email...`, 70, "Re-enrichment: Apollo enrichment");

    let enrichedCount = 0;
    if (isApolloAvailable() && leadsToEnrich.length > 0) {
      let apolloSkipped = 0;
      for (const lead of leadsToEnrich) {
        try {
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

          let result = await apolloPersonMatch({
            name: leaderName,
            firstName: hasRealName ? firstName : undefined,
            lastName: hasRealName ? lastName : undefined,
            domain: enrichableDomain,
            organizationName: lead.communityName !== leaderName ? lead.communityName || undefined : undefined,
            linkedinUrl,
          });

          if (!result && hasRealName && !linkedinUrl) {
            result = await apolloPersonMatch({
              firstName,
              lastName,
              domain: enrichableDomain,
            });
            await new Promise((r) => setTimeout(r, 300));
          }

          if (!result) continue;

          const updateData: Record<string, any> = {};
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

          if (Object.keys(updateData).length > 0) {
            await storage.updateLead(lead.id, updateData);
            enrichedCount++;
          }

          await new Promise((r) => setTimeout(r, 300));
        } catch (err: any) {
          await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
        }
      }
      await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped - invalid names)`, 85);
    } else {
      await appendAndSave("Apollo enrichment skipped (no API key configured)", 85);
    }

    await appendAndSave(`Step 6: Re-scoring all leads...`, 88, "Re-enrichment: Scoring");
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

      const hasContact = !!(lead.email || lead.website || lead.phone || lead.linkedin);
      const status = determineStatus(breakdown.total, params.threshold, hasContact);

      await storage.updateLead(lead.id, {
        score: breakdown.total,
        scoreBreakdown: breakdown,
        status,
        lastSeenAt: new Date(),
      });
      reScored++;
    }

    const qualifiedCount = await storage.countLeadsByRunAndStatus(runId, "qualified");
    const watchlistCount = await storage.countLeadsByRunAndStatus(runId, "watchlist");
    const emailCount = finalLeads.filter((l) => l.email && l.email !== "").length;

    await storage.updateRun(runId, {
      qualified: qualifiedCount,
      watchlist: watchlistCount,
    });

    await appendAndSave(
      `Re-enrichment complete! ${reScored} leads re-scored, ${emailCount} have emails, ${qualifiedCount} qualified, ${watchlistCount} watchlist`,
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
    log(`Re-enrichment of run ${runId} failed: ${err.message}`, "pipeline");
    await storage.updateRun(runId, {
      status: "failed",
      step: "Re-enrichment failed",
      logs: appendLog(currentLogs, `[ERROR] Re-enrichment failed: ${err.message}`),
      finishedAt: new Date(),
    });
  } finally {
    activeRunIds.delete(runId);
  }
}
