import { storage } from "./storage";
import { runActorAndGetResults } from "./apify";
import { scoreLead, determineStatus } from "./scoring";
import { log } from "./index";
import type { RunParams, InsertSourceUrl, InsertLead, InsertLeader } from "@shared/schema";

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
      for (const extra of ["newsletter", "membership", "contact"]) {
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
        maxItems: Math.min(maxItems, 30),
      }, 180000);

      for (const item of items) {
        if (leads.length >= maxItems) break;
        const memberCount = item.stats?.memberCounts?.all || 0;
        const description = item.description || "";
        const fullText = `${item.name || ""} ${description}`;
        const city = item.city || "";
        const country = item.country || "";
        const location = [city, item.state, country].filter(Boolean).join(", ");

        leads.push({
          source: "meetup",
          communityName: item.name || "",
          communityType: detectCommunityType(fullText),
          description,
          location,
          website: item.link || "",
          email: extractEmailsFromText(description)[0] || "",
          phone: "",
          leaderName: "",
          memberCount,
          subscriberCount: 0,
          ownedChannels: { meetup: item.link || "active" },
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
        searchKeywords: [kw],
        maxResults: Math.min(10, maxItems - leads.length),
        maxResultsShorts: 0,
        maxResultStreams: 0,
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
        if (item.channelDescriptionLinks) {
          for (const link of item.channelDescriptionLinks) {
            const url = (link.url || "").toLowerCase();
            if (url.includes("discord")) channels.discord = "detected";
            if (url.includes("patreon")) channels.patreon = url;
            if (url.includes("instagram")) channels.instagram = "detected";
            if (url.includes("twitter") || url.includes("x.com")) channels.twitter = "detected";
          }
        }

        const monetization: Record<string, any> = {};
        if (item.isMonetized) monetization.youtube_monetized = true;
        if (channels.patreon) monetization.patreon = true;

        leads.push({
          source: "youtube",
          communityName: channelName,
          communityType: detectCommunityType(`${channelName} ${description}`),
          description: description.substring(0, 2000),
          location,
          website: channelUrl,
          email: extractEmailsFromText(description)[0] || "",
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
        country: "US",
        city: city || "all",
        category: "custom",
        keyword: kw,
        maxItems: Math.min(20, maxItems - leads.length),
      }, 180000);

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
        if (organizer.website_url) channels.website = organizer.website_url;

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
          website: organizerUrl || item.url || "",
          email: extractEmailsFromText(description)[0] || "",
          phone: "",
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
        maxGroups: Math.min(20, maxItems - leads.length),
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
    await storage.updateRun(runId, { status: "running", startedAt: new Date() });

    await appendAndSave("Pipeline started", 2, "Step 1: Platform-specific discovery");

    const maxPerPlatform = Math.min(30, Math.floor(params.maxDiscoveredUrls / 4));
    const keywords = params.seedKeywords;
    const geos = params.seedGeos;

    const platformResults = await Promise.allSettled([
      scrapeMeetupGroups(keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)),
      scrapeYouTubeChannels(keywords, maxPerPlatform, (msg) => appendAndSave(msg)),
      scrapeRedditCommunities(keywords, maxPerPlatform, (msg) => appendAndSave(msg)),
      scrapeEventbriteEvents(keywords, geos, maxPerPlatform, (msg) => appendAndSave(msg)),
    ]);

    const allPlatformLeads: PlatformLead[] = [];
    const platformNames = ["Meetup", "YouTube", "Reddit", "Eventbrite"];
    for (let i = 0; i < platformResults.length; i++) {
      const result = platformResults[i];
      if (result.status === "fulfilled") {
        allPlatformLeads.push(...result.value);
        await appendAndSave(`${platformNames[i]}: ${result.value.length} results`);
      } else {
        await appendAndSave(`[WARN] ${platformNames[i]} failed: ${result.reason?.message || "Unknown error"}`);
      }
    }

    await appendAndSave(`Platform discovery complete: ${allPlatformLeads.length} results`, 35, "Step 2: Google Search discovery");

    const queries = buildGoogleQueries(params);
    await appendAndSave(`Generated ${queries.length} Google search queries`);

    let allDiscoveredUrls: { url: string; domain: string; source: string }[] = [];

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
            if (domain.includes("instagram.com")) continue;
            if (domain.includes("facebook.com")) continue;

            const urlType = classifyUrl(url);
            if (["meetup", "youtube", "reddit", "eventbrite"].includes(urlType)) continue;

            if (!allDiscoveredUrls.some((u) => u.url === url)) {
              allDiscoveredUrls.push({ url, domain, source: urlType });
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

    const sourceUrlsData: InsertSourceUrl[] = allDiscoveredUrls.map((u) => ({
      url: u.url,
      domain: u.domain,
      source: u.source,
      fetchStatus: "new",
      runId,
    }));
    await storage.createSourceUrls(sourceUrlsData);
    await storage.updateRun(runId, { urlsDiscovered: allDiscoveredUrls.length + allPlatformLeads.length });

    await appendAndSave(
      `Google discovery complete: ${allDiscoveredUrls.length} website URLs`,
      50,
      "Step 3: Extract website data"
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
            maxRequestsPerCrawl: batch.length,
            maxConcurrency: 10,
            pageFunction: `async function pageFunction(context) {
              const { request, $, log } = context;
              const title = $('title').text().trim();
              const description = $('meta[name="description"]').attr('content') || '';
              const bodyText = $('body').text().replace(/\\s+/g, ' ').substring(0, 5000);
              const links = [];
              $('a[href]').each(function() { links.push($(this).attr('href')); });
              return {
                url: request.url,
                title,
                description,
                bodyText,
                links: links.slice(0, 100),
              };
            }`,
          }, 120000);

          for (const item of items) {
            extractedPages.push(item);
          }

          await appendAndSave(`Extracted ${items.length} pages in batch ${batchNum}`);
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
      "Step 4: Create & score leads"
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

        const emails = extractEmailsFromText(pageText);
        const phones = extractPhonesFromText(pageText);
        const channels = detectOwnedChannels(pageText, url);
        const monetization = detectMonetization(pageText);
        const engagement = detectEngagement(pageText);
        const tripFit = detectTripFit(pageText);
        const communityType = detectCommunityType(pageText);

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
          linkedin: "",
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
        const hasContact = !!(email || url || phones[0]);
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

    await appendAndSave(`Created ${createdCount} total leads`, 85, "Step 5: Scoring & qualification");

    const qualifiedCount = await storage.countLeadsByRunAndStatus(runId, "qualified");
    const watchlistCount = await storage.countLeadsByRunAndStatus(runId, "watchlist");

    await storage.updateRun(runId, {
      qualified: qualifiedCount,
      watchlist: watchlistCount,
    });

    await appendAndSave(
      `Scoring complete: ${qualifiedCount} qualified, ${watchlistCount} watchlist`,
      95,
      "Step 6: Finalizing"
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
  }
}
