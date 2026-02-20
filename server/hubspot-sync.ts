import { storage } from "./storage";
import { isHubspotConfigured } from "./hubspot";
import { log } from "./index";
import type { HostTraits, ScoringInsights, LearnedWeights, Lead } from "@shared/schema";

const HUBSPOT_API_BASE = "https://api.hubapi.com";
const CONFIRMED_STAGES = ["confirmed", "closed", "complete"];
const TRIPS_PIPELINE_NAME = "trips";

interface HubSpotDeal {
  id: string;
  properties: {
    dealname?: string;
    dealstage?: string;
    pipeline?: string;
    amount?: string;
  };
}

interface HubSpotContact {
  id: string;
  properties: {
    email?: string;
    firstname?: string;
    lastname?: string;
  };
}

async function hubspotFetch(path: string, options?: RequestInit): Promise<Response> {
  const token = process.env.HUBSPOT_ACCESS_TOKEN;
  if (!token) throw new Error("HUBSPOT_ACCESS_TOKEN not configured");

  const res = await fetch(`${HUBSPOT_API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...(options?.headers || {}),
    },
  });

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("retry-after") || "2");
    await new Promise(r => setTimeout(r, retryAfter * 1000));
    return hubspotFetch(path, options);
  }

  return res;
}

async function findTripsPipelineId(): Promise<string | null> {
  const res = await hubspotFetch("/crm/v3/pipelines/deals");
  if (!res.ok) return null;
  const data = await res.json();
  const pipeline = (data.results || []).find((p: any) =>
    p.label?.toLowerCase().includes(TRIPS_PIPELINE_NAME) ||
    p.id?.toLowerCase().includes(TRIPS_PIPELINE_NAME)
  );
  return pipeline?.id || null;
}

async function getConfirmedStageIds(pipelineId: string): Promise<Set<string>> {
  const res = await hubspotFetch(`/crm/v3/pipelines/deals/${pipelineId}/stages`);
  if (!res.ok) return new Set();
  const data = await res.json();
  const stageIds = new Set<string>();
  for (const stage of data.results || []) {
    const label = (stage.label || "").toLowerCase();
    if (CONFIRMED_STAGES.some(s => label.includes(s))) {
      stageIds.add(stage.id);
    }
  }
  return stageIds;
}

async function getAllDeals(pipelineId: string): Promise<HubSpotDeal[]> {
  const deals: HubSpotDeal[] = [];
  let after: string | undefined;

  while (true) {
    const body: any = {
      filterGroups: [{
        filters: [{
          propertyName: "pipeline",
          operator: "EQ",
          value: pipelineId,
        }],
      }],
      properties: ["dealname", "dealstage", "pipeline", "amount"],
      limit: 100,
    };
    if (after) body.after = after;

    const res = await hubspotFetch("/crm/v3/objects/deals/search", {
      method: "POST",
      body: JSON.stringify(body),
    });

    if (!res.ok) break;
    const data = await res.json();
    deals.push(...(data.results || []));

    if (data.paging?.next?.after) {
      after = data.paging.next.after;
      await new Promise(r => setTimeout(r, 110));
    } else {
      break;
    }
  }

  return deals;
}

async function getDealContactsBatch(dealIds: string[]): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>();
  const batches: string[][] = [];
  for (let i = 0; i < dealIds.length; i += 100) {
    batches.push(dealIds.slice(i, i + 100));
  }

  for (const batch of batches) {
    const body = {
      inputs: batch.map(id => ({ id })),
    };
    const res = await hubspotFetch("/crm/v4/associations/deals/contacts/batch/read", {
      method: "POST",
      body: JSON.stringify(body),
    });
    await new Promise(r => setTimeout(r, 110));

    if (!res.ok) {
      for (const id of batch) {
        result.set(id, []);
      }
      continue;
    }

    const data = await res.json();
    for (const item of data.results || []) {
      const dealId = item.from?.id;
      const contactIds = (item.to || []).map((t: any) => t.toObjectId || t.id);
      if (dealId) result.set(dealId, contactIds);
    }
    for (const id of batch) {
      if (!result.has(id)) result.set(id, []);
    }
  }

  return result;
}

async function getContactsBatch(contactIds: string[]): Promise<Map<string, HubSpotContact>> {
  const result = new Map<string, HubSpotContact>();
  const batches: string[][] = [];
  for (let i = 0; i < contactIds.length; i += 100) {
    batches.push(contactIds.slice(i, i + 100));
  }

  for (const batch of batches) {
    const body = {
      inputs: batch.map(id => ({ id })),
      properties: ["email", "firstname", "lastname"],
    };
    const res = await hubspotFetch("/crm/v3/objects/contacts/batch/read", {
      method: "POST",
      body: JSON.stringify(body),
    });
    await new Promise(r => setTimeout(r, 110));

    if (!res.ok) continue;
    const data = await res.json();
    for (const contact of data.results || []) {
      result.set(contact.id, contact);
    }
  }

  return result;
}

function extractTraitsFromLead(lead: Lead): HostTraits {
  const channels = (lead.ownedChannels as Record<string, string>) || {};
  const monetization = (lead.monetizationSignals as Record<string, any>) || {};
  const engagement = (lead.engagementSignals as Record<string, any>) || {};
  const tripFit = (lead.tripFitSignals as Record<string, any>) || {};
  const breakdown = (lead.scoreBreakdown as any) || {};

  const platforms = Object.keys(channels).filter(k => channels[k]);
  const audienceSize = Math.max(
    engagement.member_count || 0,
    engagement.subscriber_count || 0,
    engagement.patron_count || 0,
    engagement.instagram_followers || 0,
    engagement.twitter_followers || 0,
  );

  return {
    communityType: lead.communityType || undefined,
    source: lead.source || undefined,
    audienceSize,
    platformCount: platforms.length,
    platforms,
    hasWebsite: !!channels.website,
    hasNewsletter: !!(channels.newsletter || channels.substack),
    hasYoutube: !!channels.youtube,
    hasPodcast: !!(channels.podcast || channels.rss),
    hasInstagram: !!channels.instagram,
    hasLinkedin: !!lead.linkedin,
    monetizationSignalCount: Object.keys(monetization).filter(k => monetization[k]).length,
    tripFitSignalCount: Object.keys(tripFit).filter(k => tripFit[k]).length,
    engagementSignalCount: Object.keys(engagement).filter(k => engagement[k] && engagement[k] !== 0).length,
    score: lead.score || 0,
    nicheIdentity: breakdown.nicheIdentity || 0,
    trustLeadership: breakdown.trustLeadership || 0,
    engagement: breakdown.engagement || 0,
    monetization: breakdown.monetization || 0,
    ownedChannels: breakdown.ownedChannels || 0,
    tripFit: breakdown.tripFit || 0,
  };
}

export async function syncHubSpotDeals(): Promise<{
  dealsFound: number;
  profilesCreated: number;
  matched: number;
  topHosts: number;
  pipelineId: string | null;
}> {
  if (!isHubspotConfigured()) {
    throw new Error("HubSpot not configured");
  }

  log("Finding Trips pipeline...", "hubspot-learn");
  const pipelineId = await findTripsPipelineId();
  if (!pipelineId) {
    throw new Error("Could not find 'Trips' pipeline in HubSpot. Make sure a pipeline with 'Trips' in the name exists.");
  }

  const confirmedStageIds = await getConfirmedStageIds(pipelineId);
  log(`Found ${confirmedStageIds.size} confirmed stages`, "hubspot-learn");

  log("Fetching all deals...", "hubspot-learn");
  const allDeals = await getAllDeals(pipelineId);
  log(`Found ${allDeals.length} deals in Trips pipeline`, "hubspot-learn");

  log("Fetching deal-contact associations (batch)...", "hubspot-learn");
  const dealIds = allDeals.map(d => d.id);
  const dealContactsMap = await getDealContactsBatch(dealIds);

  const contactTrips = new Map<string, { contactId: string; confirmedTrips: number; totalDeals: number }>();

  for (const deal of allDeals) {
    const contactIds = dealContactsMap.get(deal.id) || [];
    const isConfirmed = confirmedStageIds.has(deal.properties.dealstage || "");

    for (const contactId of contactIds) {
      const existing = contactTrips.get(contactId) || { contactId, confirmedTrips: 0, totalDeals: 0 };
      existing.totalDeals++;
      if (isConfirmed) existing.confirmedTrips++;
      contactTrips.set(contactId, existing);
    }
  }

  log(`Found ${contactTrips.size} unique contacts across deals`, "hubspot-learn");

  log("Fetching contact details (batch)...", "hubspot-learn");
  const allContactIds = Array.from(contactTrips.keys());
  const contactsMap = await getContactsBatch(allContactIds);
  log(`Retrieved ${contactsMap.size} contact records`, "hubspot-learn");

  await storage.clearHostProfiles();

  let matched = 0;
  const contactEntries = Array.from(contactTrips.values());

  for (const entry of contactEntries) {
    const contact = contactsMap.get(entry.contactId);
    if (!contact) continue;

    const email = contact.properties.email || "";
    const name = [contact.properties.firstname, contact.properties.lastname].filter(Boolean).join(" ");

    let matchedLeadId: number | undefined;
    let traits: HostTraits = {};

    if (email) {
      const lead = await storage.findLeadByEmail(email);
      if (lead) {
        matchedLeadId = lead.id;
        traits = extractTraitsFromLead(lead);

        const runParams = lead.runId ? (await storage.getRun(lead.runId))?.params as any : null;
        if (runParams?.seedKeywords) {
          traits.keywords = runParams.seedKeywords;
        }

        matched++;
      }
    }

    await storage.createHostProfile({
      hubspotContactId: contact.id,
      email,
      name,
      confirmedTrips: entry.confirmedTrips,
      totalDeals: entry.totalDeals,
      traits,
      matchedLeadId: matchedLeadId ?? null,
    });
  }

  const topHosts = contactEntries.filter(c => c.confirmedTrips >= 2).length;
  log(`Profiles created: ${contactEntries.length}, matched to leads: ${matched}, top hosts (2+ trips): ${topHosts}`, "hubspot-learn");

  return {
    dealsFound: allDeals.length,
    profilesCreated: contactEntries.length,
    matched,
    topHosts,
    pipelineId,
  };
}

const DEFAULT_WEIGHTS: LearnedWeights = {
  nicheIdentity: 20,
  trustLeadership: 15,
  engagement: 20,
  monetization: 15,
  ownedChannels: 20,
  tripFit: 10,
};

export async function computeScoringWeights(): Promise<{
  weights: LearnedWeights;
  insights: ScoringInsights;
  sampleSize: number;
  topHostCount: number;
}> {
  const profiles = await storage.listHostProfiles();

  if (profiles.length < 5) {
    return {
      weights: DEFAULT_WEIGHTS,
      insights: {
        topTraits: [],
        topPlatforms: [],
        topCommunityTypes: [],
        avgAudienceSize: 0,
        avgPlatformCount: 0,
        avgScore: 0,
        suggestedKeywords: [],
      },
      sampleSize: profiles.length,
      topHostCount: 0,
    };
  }

  const topHosts = profiles.filter(p => (p.confirmedTrips || 0) >= 2 && p.traits);
  const allWithTraits = profiles.filter(p => p.traits && Object.keys(p.traits).length > 0);

  if (topHosts.length < 3) {
    const sortedByTrips = profiles
      .filter(p => (p.confirmedTrips || 0) >= 1 && p.traits)
      .sort((a, b) => (b.confirmedTrips || 0) - (a.confirmedTrips || 0));
    topHosts.push(...sortedByTrips.slice(0, Math.max(5, sortedByTrips.length)));
    const unique = new Map(topHosts.map(h => [h.id, h]));
    topHosts.length = 0;
    topHosts.push(...Array.from(unique.values()));
  }

  if (topHosts.length === 0) {
    return {
      weights: DEFAULT_WEIGHTS,
      insights: {
        topTraits: [],
        topPlatforms: [],
        topCommunityTypes: [],
        avgAudienceSize: 0,
        avgPlatformCount: 0,
        avgScore: 0,
        suggestedKeywords: [],
      },
      sampleSize: profiles.length,
      topHostCount: 0,
    };
  }

  function avgPillar(hosts: typeof topHosts, key: keyof HostTraits): number {
    const vals = hosts.map(h => Number((h.traits as any)?.[key]) || 0);
    return vals.reduce((s, v) => s + v, 0) / Math.max(vals.length, 1);
  }

  const topAvg = {
    nicheIdentity: avgPillar(topHosts, "nicheIdentity"),
    trustLeadership: avgPillar(topHosts, "trustLeadership"),
    engagement: avgPillar(topHosts, "engagement"),
    monetization: avgPillar(topHosts, "monetization"),
    ownedChannels: avgPillar(topHosts, "ownedChannels"),
    tripFit: avgPillar(topHosts, "tripFit"),
  };

  const allAvg = {
    nicheIdentity: avgPillar(allWithTraits, "nicheIdentity"),
    trustLeadership: avgPillar(allWithTraits, "trustLeadership"),
    engagement: avgPillar(allWithTraits, "engagement"),
    monetization: avgPillar(allWithTraits, "monetization"),
    ownedChannels: avgPillar(allWithTraits, "ownedChannels"),
    tripFit: avgPillar(allWithTraits, "tripFit"),
  };

  const rawWeights: LearnedWeights = { ...DEFAULT_WEIGHTS };
  const pillars: (keyof LearnedWeights)[] = ["nicheIdentity", "trustLeadership", "engagement", "monetization", "ownedChannels", "tripFit"];

  for (const p of pillars) {
    const topVal = topAvg[p];
    const allVal = allAvg[p] || 1;
    const lift = topVal / Math.max(allVal, 0.1);
    rawWeights[p] = DEFAULT_WEIGHTS[p] * Math.min(Math.max(lift, 0.5), 2.0);
  }

  const totalRaw = pillars.reduce((s, p) => s + rawWeights[p], 0);
  const weights: LearnedWeights = {} as LearnedWeights;
  for (const p of pillars) {
    weights[p] = Math.round((rawWeights[p] / totalRaw) * 100);
  }

  const traitCounts = new Map<string, number>();
  const platformCounts = new Map<string, number>();
  const typeCounts = new Map<string, number>();
  const keywordCounts = new Map<string, number>();
  let totalAudience = 0;
  let totalPlatforms = 0;
  let totalScore = 0;

  for (const h of topHosts) {
    const t = h.traits as HostTraits;
    if (!t) continue;

    if (t.hasWebsite) traitCounts.set("Has Website", (traitCounts.get("Has Website") || 0) + 1);
    if (t.hasNewsletter) traitCounts.set("Newsletter/Substack", (traitCounts.get("Newsletter/Substack") || 0) + 1);
    if (t.hasYoutube) traitCounts.set("YouTube Channel", (traitCounts.get("YouTube Channel") || 0) + 1);
    if (t.hasPodcast) traitCounts.set("Podcast", (traitCounts.get("Podcast") || 0) + 1);
    if (t.hasInstagram) traitCounts.set("Instagram", (traitCounts.get("Instagram") || 0) + 1);
    if (t.hasLinkedin) traitCounts.set("LinkedIn", (traitCounts.get("LinkedIn") || 0) + 1);
    if ((t.monetizationSignalCount || 0) > 0) traitCounts.set("Monetization Signals", (traitCounts.get("Monetization Signals") || 0) + 1);
    if ((t.audienceSize || 0) > 1000) traitCounts.set("1K+ Audience", (traitCounts.get("1K+ Audience") || 0) + 1);
    if ((t.platformCount || 0) >= 3) traitCounts.set("3+ Platforms", (traitCounts.get("3+ Platforms") || 0) + 1);

    if (t.source) platformCounts.set(t.source, (platformCounts.get(t.source) || 0) + 1);
    if (t.communityType) typeCounts.set(t.communityType, (typeCounts.get(t.communityType) || 0) + 1);
    totalAudience += t.audienceSize || 0;
    totalPlatforms += t.platformCount || 0;
    totalScore += t.score || 0;

    if (t.keywords) {
      for (const kw of t.keywords) {
        keywordCounts.set(kw, (keywordCounts.get(kw) || 0) + 1);
      }
    }
  }

  const topTraits = Array.from(traitCounts.entries())
    .map(([trait, count]) => ({ trait, prevalence: Math.round((count / topHosts.length) * 100) }))
    .sort((a, b) => b.prevalence - a.prevalence)
    .slice(0, 10);

  const topPlatforms = Array.from(platformCounts.entries())
    .map(([platform, count]) => ({ platform, count }))
    .sort((a, b) => b.count - a.count);

  const topCommunityTypes = Array.from(typeCounts.entries())
    .map(([type, count]) => ({ type, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const suggestedKeywords = Array.from(keywordCounts.entries())
    .map(([keyword, count]) => ({ keyword, score: Math.round((count / topHosts.length) * 100) }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 20);

  const insights: ScoringInsights = {
    topTraits,
    topPlatforms,
    topCommunityTypes,
    avgAudienceSize: Math.round(totalAudience / topHosts.length),
    avgPlatformCount: Math.round((totalPlatforms / topHosts.length) * 10) / 10,
    avgScore: Math.round(totalScore / topHosts.length),
    suggestedKeywords,
  };

  await storage.saveScoringWeights({
    weights,
    sampleSize: profiles.length,
    topHostCount: topHosts.length,
    insights,
  });

  return { weights, insights, sampleSize: profiles.length, topHostCount: topHosts.length };
}

export async function getLatestWeights(): Promise<LearnedWeights | null> {
  const row = await storage.getLatestScoringWeights();
  return row?.weights || null;
}

export async function getLatestInsights(): Promise<ScoringInsights | null> {
  const row = await storage.getLatestScoringWeights();
  return row?.insights || null;
}
