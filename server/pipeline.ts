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
  const { seedKeywords, seedGeos, communityTypes, intentTerms } = params;

  const communityLabels: Record<string, string[]> = {
    church: ["church", "ministry", "bible study", "young adults church"],
    run_club: ["run club", "running group", "running community"],
    hiking: ["hiking club", "hiking group", "outdoor club", "trail group"],
    social_club: ["social club", "social group", "meetup group"],
    book_club: ["book club", "reading group"],
    professional: ["professional association", "professional network", "networking group"],
    alumni: ["alumni chapter", "alumni group", "alumni association"],
    nonprofit: ["nonprofit community", "volunteer group", "charity group"],
    fitness: ["CrossFit community", "yoga studio community", "fitness community"],
    coworking: ["coworking community", "coworking space members"],
    other: ["community group", "local club", "social organization"],
  };

  const sitePrefixes = [
    "",
    "site:meetup.com",
    "site:eventbrite.com",
    "site:facebook.com",
    "site:youtube.com",
    "site:substack.com",
  ];

  for (const kw of seedKeywords) {
    for (const intent of intentTerms.slice(0, 3)) {
      const geoStr = seedGeos.length > 0 ? ` ${seedGeos[0]}` : "";
      queries.push(`${kw} ${intent}${geoStr}`);
    }
  }

  for (const ct of communityTypes) {
    const labels = communityLabels[ct] || [ct];
    for (const label of labels.slice(0, 2)) {
      for (const intent of intentTerms.slice(0, 2)) {
        const geoStr = seedGeos.length > 0 ? ` ${seedGeos[0]}` : "";
        queries.push(`${label} ${intent}${geoStr}`);
      }
      for (const site of sitePrefixes.slice(0, 3)) {
        const geoStr = seedGeos.length > 0 ? ` ${seedGeos[0]}` : "";
        const prefix = site ? `${site} ` : "";
        queries.push(`${prefix}${label} community${geoStr}`);
      }
    }
  }

  for (const kw of seedKeywords) {
    for (const channelTerm of ["newsletter", "membership", "subscribe", "podcast"]) {
      const geoStr = seedGeos.length > 0 ? ` ${seedGeos[0]}` : "";
      queries.push(`${kw} ${channelTerm}${geoStr}`);
    }
  }

  const unique = [...new Set(queries)];
  return unique.slice(0, 200);
}

function extractEmailsFromText(text: string): string[] {
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const matches = text.match(emailRegex) || [];
  return [...new Set(matches)].filter(
    (e) => !e.endsWith(".png") && !e.endsWith(".jpg") && !e.endsWith(".gif")
  );
}

function extractPhonesFromText(text: string): string[] {
  const phoneRegex = /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
  return [...new Set(text.match(phoneRegex) || [])];
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

    await appendAndSave("Pipeline started", 5, "Step A: Discovery (Google Search)");

    const queries = buildGoogleQueries(params);
    await appendAndSave(`Generated ${queries.length} search queries`);

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
        await appendAndSave(`Running Google Search batch ${batchIdx + 1}/${queryBatches.length} (${batch.length} queries)`);

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

            if (!allDiscoveredUrls.some((u) => u.url === url)) {
              allDiscoveredUrls.push({ url, domain, source: classifyUrl(url) });
            }
          }
        }

        await appendAndSave(`Batch ${batchIdx + 1} complete. Total URLs: ${allDiscoveredUrls.length}`);
      } catch (err: any) {
        await appendAndSave(`[ERROR] Google batch ${batchIdx + 1} failed: ${err.message}`);
      }

      const progress = 5 + Math.round((batchIdx / queryBatches.length) * 25);
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
    await storage.updateRun(runId, { urlsDiscovered: allDiscoveredUrls.length });

    await appendAndSave(`Discovery complete: ${allDiscoveredUrls.length} URLs stored`, 30, "Step B: Classify URLs");

    const classified: Record<string, string[]> = {};
    for (const u of allDiscoveredUrls) {
      const cat = u.source;
      if (!classified[cat]) classified[cat] = [];
      classified[cat].push(u.url);
    }

    for (const [cat, urls] of Object.entries(classified)) {
      await appendAndSave(`${cat}: ${urls.length} URLs`);
    }

    await appendAndSave("Classification complete", 35, "Step C: Extract data");

    const websiteUrls = [
      ...(classified.website || []),
      ...(classified.meetup || []),
      ...(classified.eventbrite || []),
      ...(classified.facebook_page || []),
    ].slice(0, Math.min(100, params.maxDiscoveredUrls));

    let extractedLeads: any[] = [];

    if (websiteUrls.length > 0) {
      const extractBatchSize = 10;
      const totalBatches = Math.ceil(websiteUrls.length / extractBatchSize);

      for (let i = 0; i < websiteUrls.length; i += extractBatchSize) {
        const batch = websiteUrls.slice(i, i + extractBatchSize);
        const batchNum = Math.floor(i / extractBatchSize) + 1;
        try {
          await appendAndSave(`Extracting data from ${batch.length} websites (batch ${batchNum}/${totalBatches})`);

          const items = await runActorAndGetResults("apify~web-scraper", {
            startUrls: batch.map((u) => ({ url: u })),
            maxRequestsPerCrawl: batch.length,
            maxCrawlingDepth: 0,
            pageFunction: `async function pageFunction(context) {
              const { request, log, jQuery } = context;
              const $ = jQuery;
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
          }, 300000);

          for (const item of items) {
            extractedLeads.push(item);
          }

          await appendAndSave(`Extracted ${items.length} pages in batch ${batchNum}`);
        } catch (err: any) {
          await appendAndSave(`[ERROR] Web extraction batch ${batchNum} failed: ${err.message}`);
        }

        const progress = 35 + Math.round((batchNum / totalBatches) * 20);
        await appendAndSave(`Progress update`, progress);
      }
    }

    if (classified.youtube && classified.youtube.length > 0 && params.sources.youtube) {
      await appendAndSave(`Processing ${classified.youtube.length} YouTube URLs`);
    }
    if (classified.substack && classified.substack.length > 0 && params.sources.substack) {
      await appendAndSave(`Processing ${classified.substack.length} Substack URLs`);
    }

    await appendAndSave(`Extraction complete: ${extractedLeads.length} pages processed`, 55, "Step D: Enrich & create leads");

    let createdCount = 0;

    for (const item of extractedLeads) {
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
        await appendAndSave(`[ERROR] Lead processing failed: ${err.message}`);
      }
    }

    await storage.updateRun(runId, { leadsExtracted: createdCount });

    await appendAndSave(`Created ${createdCount} leads`, 80, "Step E: Scoring & qualification");

    const qualifiedCount = await storage.countLeadsByRunAndStatus(runId, "qualified");
    const watchlistCount = await storage.countLeadsByRunAndStatus(runId, "watchlist");

    await storage.updateRun(runId, {
      qualified: qualifiedCount,
      watchlist: watchlistCount,
    });

    await appendAndSave(`Scoring complete: ${qualifiedCount} qualified, ${watchlistCount} watchlist`, 95, "Step F: Finalizing");

    await storage.updateRun(runId, {
      status: "succeeded",
      progress: 100,
      step: "Complete",
      finishedAt: new Date(),
      logs: appendLog(currentLogs, `Pipeline complete! ${qualifiedCount} qualified, ${watchlistCount} watchlist leads.`),
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
