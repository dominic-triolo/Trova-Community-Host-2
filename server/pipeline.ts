import { storage } from "./storage";
import { runActorAndGetResults } from "./apify";
import { scoreLead } from "./scoring";
import { apolloPersonMatch, isApolloAvailable, extractDomainFromUrl as apolloExtractDomain, isEnrichableDomain as apolloIsEnrichable } from "./apollo";
import { hunterDomainSearch, extractDomainFromUrl, isEnrichableDomain, isHunterAvailable } from "./hunter";
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

function extractBareDomainUrls(text: string): string[] {
  const bareDomainRegex = /(?<![/@a-zA-Z0-9])(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:com|org|net|co|io|me|info|biz|us|uk|ca|au|de|fr|es|it|nl|se|no|fi|dk|ch|at|be|nz|in|co\.uk|com\.au|co\.nz)(?:\/[^\s"'<>,)}\]]*)?)/gi;
  const matches = text.match(bareDomainRegex) || [];
  const imageExts = [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"];
  return Array.from(new Set(matches.map(m => m.trim())))
    .filter(m => !imageExts.some(ext => m.toLowerCase().endsWith(ext)))
    .filter(m => !m.includes("@"))
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

function isPatreonCdnUrl(url: string): boolean {
  if (!url) return false;
  const lower = url.toLowerCase();
  return lower.includes("patreonusercontent.com") || lower.includes("patreon-media") || lower.includes("token-hash=");
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

  const dedupedKeywords = Array.from(new Set(keywords.map(k => k.trim()).filter(Boolean)));
  if (dedupedKeywords.length === 0) {
    await appendAndSave(`Patreon: no keywords to search`);
    return leads;
  }
  const keywordPreview = dedupedKeywords.length <= 5 ? dedupedKeywords.join(", ") : `${dedupedKeywords.slice(0, 5).join(", ")} (+${dedupedKeywords.length - 5} more)`;
  await appendAndSave(`Patreon: searching ${dedupedKeywords.length} keywords: ${keywordPreview}`);

  try {
    const items = await runActorAndGetResults("louisdeconinck~patreon-scraper", {
      searchQueries: dedupedKeywords,
      maxRequestsPerCrawl: Math.min(800, Math.max(100, maxItems * 6)),
    }, 300000);

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
          } catch {}
        }

        const creatorEmail = extractEmailsFromText(`${description} ${aboutText}`)[0] || "";

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
          leaderName: creatorName,
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
    await appendAndSave(`[WARN] Patreon search failed: ${err.message}`);
  }

  return leads;
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

const GOOGLE_ENRICHMENT_MAX = 20;
const GOOGLE_ENRICHMENT_SOCIAL_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com", "discord.gg", "discord.com", "facebook.com", "tiktok.com", "twitch.tv", "linkedin.com", "patreon.com", "google.com", "apple.com", "spotify.com", "amazon.com", "reddit.com", "tumblr.com", "pinterest.com", "github.com", "medium.com", "wordpress.com", "linktr.ee", "beacons.ai", "ko-fi.com", "buymeacoffee.com", "gumroad.com", "substack.com", "bit.ly", "apify.com", "meetup.com", "eventbrite.com", "yelp.com", "tripadvisor.com", "bbb.org"];

async function googleSearchEnrichCreators(
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

  const queries = leadsToSearch.map(l => {
    const name = l.communityName || l.leaderName || "";
    return { term: `"${name}" contact website email`, leadIdx: leads.indexOf(l) };
  });

  const batchSize = 5;
  let enriched = 0;

  for (let i = 0; i < queries.length; i += batchSize) {
    const batch = queries.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(queries.length / batchSize);

    await appendAndSave(`Google enrichment: batch ${batchNum}/${totalBatches} (${batch.length} searches)...`);

    try {
      const searchQueries = batch.map(q => ({ term: q.term, countryCode: "us", languageCode: "en", maxPagesPerQuery: 1, resultsPerPage: 5 }));

      const results = await runActorAndGetResults("apify~google-search-scraper", {
        queries: searchQueries.map(q => q.term).join("\n"),
        maxPagesPerQuery: 1,
        resultsPerPage: 5,
        countryCode: "us",
        languageCode: "en",
        mobileResults: false,
      }, 120000);

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

        if (foundAnything) enriched++;
      }
    } catch (err: any) {
      await appendAndSave(`[WARN] Google enrichment batch ${batchNum} failed: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 1000));
  }

  await appendAndSave(`Google enrichment: found new data for ${enriched}/${leadsToSearch.length} creators`);
}

async function crawlCreatorWebsitesForEmails(
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

  const websiteEntries = Array.from(uniqueWebsites.entries()).slice(0, 30);
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
      const results = await runActorAndGetResults("apify~cheerio-scraper", {
        startUrls,
        globs,
        maxCrawlPages: 5 * batch.length,
        maxConcurrency: 3,
        pageFunction: `async function pageFunction(context) {
  const { $, request } = context;
  const text = $('body').text();
  return { url: request.url, text: text.substring(0, 5000) };
}`,
      }, 60000);

      for (const result of results) {
        const pageText = result.text || "";
        const pageUrl = result.url || "";
        if (!pageText || !pageUrl) continue;

        const pageDomain = extractDomain(pageUrl);
        if (!pageDomain || emailMap.has(pageDomain)) continue;

        const emails = extractEmailsFromText(pageText);
        const validEmail = emails.find((e) => !isBlockedEmail(e));
        if (validEmail) {
          emailMap.set(pageDomain, validEmail);
        }
      }

      await appendAndSave(`Website crawl batch ${batchNum}: found ${emailMap.size} total emails so far`);
    } catch (err: any) {
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

    const leadsWithoutContactInfo = allPlatformLeads.filter(l => !l.email && !l.ownedChannels?.website && !l.ownedChannels?.linkedin);
    if (leadsWithoutContactInfo.length > 0) {
      await appendAndSave(`Google enrichment: ${leadsWithoutContactInfo.length} leads need website/LinkedIn lookup`, 32, "Step 2: Google contact search");
      await googleSearchEnrichCreators(allPlatformLeads, appendAndSave);
    } else {
      await appendAndSave("Google enrichment: skipped (all leads already have contact info)");
    }

    const leadsNeedingEmail = allPlatformLeads.filter(l => !l.email && l.ownedChannels?.website && !isPatreonCdnUrl(l.ownedChannels.website));
    if (leadsNeedingEmail.length > 0) {
      await appendAndSave(`Crawling ${leadsNeedingEmail.length} creator websites for contact emails...`, 38, "Step 3: Website contact crawl");
      const websiteEmailMap = await crawlCreatorWebsitesForEmails(leadsNeedingEmail, appendAndSave);
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
    const APOLLO_MAX_CALLS_PER_RUN = 50;
    const APOLLO_MIN_SCORE = 15;
    const leadsToEnrich = runLeads
      .filter((l) => !l.email && (l.score || 0) >= APOLLO_MIN_SCORE)
      .sort((a, b) => (b.score || 0) - (a.score || 0));
    let enrichedCount = 0;

    if (params.enableApollo !== false) {
      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        const totalWithoutEmail = runLeads.filter((l) => !l.email).length;
        const skippedLowScore = totalWithoutEmail - leadsToEnrich.length;
        await appendAndSave(`Apollo.io: enriching top ${Math.min(leadsToEnrich.length, APOLLO_MAX_CALLS_PER_RUN)} of ${totalWithoutEmail} leads without email (${skippedLowScore} below score ${APOLLO_MIN_SCORE}, max ${APOLLO_MAX_CALLS_PER_RUN} API calls)...`);

        let apolloSkipped = 0;
        let apolloCalls = 0;
        let apolloDeduped = 0;
        for (const lead of leadsToEnrich) {
          if (apolloCalls >= APOLLO_MAX_CALLS_PER_RUN) {
            await appendAndSave(`Apollo.io: reached ${APOLLO_MAX_CALLS_PER_RUN} call limit, stopping`);
            break;
          }
          try {
            if (lead.apolloEnrichedAt) {
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

            if (!result && hasRealName && !linkedinUrl && apolloCalls < APOLLO_MAX_CALLS_PER_RUN) {
              apolloCalls++;
              result = await apolloPersonMatch({
                firstName,
                lastName,
                domain: enrichableDomain,
              });
              await new Promise((r) => setTimeout(r, 300));
            }

            if (!result) {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date() });
              continue;
            }

            const updateData: Record<string, any> = { apolloEnrichedAt: new Date() };

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

            if (Object.keys(updateData).length <= 1) {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date() });
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

        await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped invalid names, ${apolloDeduped} already enriched, ${apolloCalls} API calls used)`);
      } else {
        await appendAndSave("Apollo enrichment: skipped (no API key configured)");
      }
    } else {
      await appendAndSave("Apollo enrichment skipped (disabled by user)");
    }

    const HUNTER_MAX_CALLS_PER_RUN = 30;
    const refreshedLeads = await storage.listLeadsByRun(runId);
    const leadsForHunter = refreshedLeads
      .filter((l) => !l.email)
      .filter((l) => {
        const channels = (l.ownedChannels as Record<string, string>) || {};
        const websiteUrl = channels.website || l.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        return domain && isEnrichableDomain(domain);
      })
      .sort((a, b) => (b.score || 0) - (a.score || 0));

    if (isHunterAvailable() && leadsForHunter.length > 0) {
      await appendAndSave(`Hunter.io: enriching ${Math.min(leadsForHunter.length, HUNTER_MAX_CALLS_PER_RUN)} leads with website domains...`, 87, "Step 9: Hunter.io enrichment");
      const enrichedDomains = new Set<string>();
      let hunterEnriched = 0;
      let hunterCalls = 0;

      for (const lead of leadsForHunter) {
        if (hunterCalls >= HUNTER_MAX_CALLS_PER_RUN) {
          await appendAndSave(`Hunter.io: reached ${HUNTER_MAX_CALLS_PER_RUN} call limit, stopping`);
          break;
        }

        const channels = (lead.ownedChannels as Record<string, string>) || {};
        const websiteUrl = channels.website || lead.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        if (!domain || !isEnrichableDomain(domain) || enrichedDomains.has(domain)) continue;
        enrichedDomains.add(domain);

        hunterCalls++;
        const result = await hunterDomainSearch(domain);
        if (!result || result.emails.length === 0) continue;

        const bestEmail = result.emails.sort((a, b) => b.confidence - a.confidence)[0];
        if (isBlockedEmail(bestEmail.value)) continue;

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
          ownedChannels: channels,
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
        hunterEnriched++;
        await new Promise((r) => setTimeout(r, 200));
      }

      await appendAndSave(`Hunter.io: enriched ${hunterEnriched} leads from ${enrichedDomains.size} domains (${hunterCalls} API calls)`);
      enrichedCount += hunterEnriched;
    } else if (leadsForHunter.length > 0) {
      await appendAndSave("Hunter.io: skipped (no API key configured)");
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
      const RE_APOLLO_MAX_CALLS = 50;
      const RE_APOLLO_MIN_SCORE = 15;
      const leadsToEnrich = refreshedLeads
        .filter((l) => (!l.email || l.email === "") && (l.score || 0) >= RE_APOLLO_MIN_SCORE)
        .sort((a, b) => (b.score || 0) - (a.score || 0));
      const totalWithoutEmail = refreshedLeads.filter((l) => !l.email || l.email === "").length;

      await appendAndSave(`Step 2: Apollo enrichment for top ${Math.min(leadsToEnrich.length, RE_APOLLO_MAX_CALLS)} of ${totalWithoutEmail} leads without email (max ${RE_APOLLO_MAX_CALLS} calls)...`, 50, "Re-enrichment: Apollo enrichment");

      let enrichedCount = 0;
      if (isApolloAvailable() && leadsToEnrich.length > 0) {
        let apolloSkipped = 0;
        let apolloCalls = 0;
        let apolloDeduped = 0;
        for (const lead of leadsToEnrich) {
          if (apolloCalls >= RE_APOLLO_MAX_CALLS) {
            await appendAndSave(`Apollo.io: reached ${RE_APOLLO_MAX_CALLS} call limit, stopping`);
            break;
          }
          try {
            if (lead.apolloEnrichedAt) {
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

            if (!result && hasRealName && !linkedinUrl && apolloCalls < RE_APOLLO_MAX_CALLS) {
              apolloCalls++;
              result = await apolloPersonMatch({
                firstName,
                lastName,
                domain: enrichableDomain,
              });
              await new Promise((r) => setTimeout(r, 300));
            }

            if (!result) {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date() });
              continue;
            }

            const updateData: Record<string, any> = { apolloEnrichedAt: new Date() };
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

            if (Object.keys(updateData).length > 1) {
              await storage.updateLead(lead.id, updateData);
              enrichedCount++;
            } else {
              await storage.updateLead(lead.id, { apolloEnrichedAt: new Date() });
            }

            await new Promise((r) => setTimeout(r, 300));
          } catch (err: any) {
            await appendAndSave(`[WARN] Apollo enrichment failed for lead ${lead.id}: ${err.message}`);
          }
        }
        await appendAndSave(`Apollo.io: enriched ${enrichedCount} of ${leadsToEnrich.length} leads (${apolloSkipped} skipped invalid names, ${apolloDeduped} already enriched, ${apolloCalls} API calls used)`, 70);
      } else {
        await appendAndSave("Apollo enrichment skipped (no API key configured)", 70);
      }
    } else {
      await appendAndSave("Apollo enrichment skipped (disabled by user)", 70);
    }

    const RE_HUNTER_MAX_CALLS = 30;
    const hunterLeads = await storage.listLeadsByRun(runId);
    const leadsForHunterReEnrich = hunterLeads
      .filter((l) => !l.email || l.email === "")
      .filter((l) => {
        const ch = (l.ownedChannels as Record<string, string>) || {};
        const websiteUrl = ch.website || l.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        return domain && isEnrichableDomain(domain);
      })
      .sort((a, b) => (b.score || 0) - (a.score || 0));

    if (isHunterAvailable() && leadsForHunterReEnrich.length > 0) {
      await appendAndSave(`Step 4: Hunter.io enrichment for ${Math.min(leadsForHunterReEnrich.length, RE_HUNTER_MAX_CALLS)} leads...`, 72, "Re-enrichment: Hunter.io");
      const enrichedDomains = new Set<string>();
      let hunterEnriched = 0;
      let hunterCalls = 0;

      for (const lead of leadsForHunterReEnrich) {
        if (hunterCalls >= RE_HUNTER_MAX_CALLS) break;
        const ch = (lead.ownedChannels as Record<string, string>) || {};
        const websiteUrl = ch.website || lead.website || "";
        const domain = extractDomainFromUrl(websiteUrl);
        if (!domain || !isEnrichableDomain(domain) || enrichedDomains.has(domain)) continue;
        enrichedDomains.add(domain);

        hunterCalls++;
        const result = await hunterDomainSearch(domain);
        if (!result || result.emails.length === 0) continue;

        const bestEmail = result.emails.sort((a, b) => b.confidence - a.confidence)[0];
        if (isBlockedEmail(bestEmail.value)) continue;

        const updateData: Record<string, any> = { email: bestEmail.value };
        if (bestEmail.phone_number && !lead.phone) updateData.phone = bestEmail.phone_number;
        if (bestEmail.linkedin && !lead.linkedin) updateData.linkedin = bestEmail.linkedin;

        if (Object.keys(updateData).length > 0) {
          await storage.updateLead(lead.id, updateData);
          hunterEnriched++;
        }
        await new Promise((r) => setTimeout(r, 200));
      }
      await appendAndSave(`Hunter.io: enriched ${hunterEnriched} leads from ${enrichedDomains.size} domains`);
    } else if (leadsForHunterReEnrich.length > 0) {
      await appendAndSave("Hunter.io: skipped (no API key configured)");
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
