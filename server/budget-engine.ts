import type { SourceId, BudgetAllocation, PlatformAllocation } from "@shared/schema";
import { PLATFORM_COST_PER_LEAD, PLATFORM_EMAIL_YIELD, PLATFORM_VALID_EMAIL_RATE, KEYWORD_PLATFORM_MAP } from "@shared/schema";

const ENRICHMENT_COST_PER_LEAD = 0.005;
const MIN_LEADS_PER_PLATFORM = 10;
const DISCOVERY_BUDGET_RATIO = 0.65;
const ENRICHMENT_BUDGET_RATIO = 0.35;
const PODCAST_MIN_BUDGET = 3;
const ENRICHMENT_EMAIL_ADD_RATE = 0.25;
const ENRICHMENT_VALID_RATE = 0.55;

const ACTIVE_PLATFORMS: SourceId[] = ["patreon", "facebook", "podcast", "substack", "meetup"];

export interface PlatformStats {
  platform: string;
  totalLeads: number;
  withEmail: number;
  validEmails: number;
  validRate: number;
}

function getValidEmailRate(platform: string, historicalStats?: PlatformStats[]): number {
  if (historicalStats && historicalStats.length > 0) {
    const stat = historicalStats.find(s => s.platform === platform);
    if (stat && stat.withEmail >= 10) {
      return stat.validRate;
    }
  }
  return PLATFORM_VALID_EMAIL_RATE[platform] || 0.25;
}

function getEmailYield(platform: string): number {
  return PLATFORM_EMAIL_YIELD[platform] || 0.2;
}

function getEffectiveValidEmailYield(platform: string, historicalStats?: PlatformStats[]): number {
  const baseYield = getEmailYield(platform);
  const validRate = getValidEmailRate(platform, historicalStats);
  const baseValidYield = baseYield * validRate;
  const enrichmentValidYield = (1 - baseYield) * ENRICHMENT_EMAIL_ADD_RATE * ENRICHMENT_VALID_RATE;
  return baseValidYield + enrichmentValidYield;
}

export function selectPlatformsForKeywords(
  keywords: string[],
  podcastEnabled: boolean = true,
  enabledPlatforms?: SourceId[],
): SourceId[] {
  const allowedPlatforms = enabledPlatforms && enabledPlatforms.length > 0
    ? ACTIVE_PLATFORMS.filter(p => enabledPlatforms.includes(p))
    : podcastEnabled
      ? ACTIVE_PLATFORMS
      : ACTIVE_PLATFORMS.filter(p => p !== "podcast");

  if (allowedPlatforms.length === 0) return [...ACTIVE_PLATFORMS];

  const platformScores = new Map<SourceId, number>();

  for (const kw of keywords) {
    const kwLower = kw.toLowerCase().trim();
    let matched = false;

    for (const [pattern, platforms] of Object.entries(KEYWORD_PLATFORM_MAP)) {
      if (kwLower.includes(pattern) || pattern.includes(kwLower)) {
        for (const p of platforms) {
          if (allowedPlatforms.includes(p)) {
            platformScores.set(p, (platformScores.get(p) || 0) + 2);
            matched = true;
          }
        }
      }
    }

    if (!matched) {
      for (const p of allowedPlatforms) {
        platformScores.set(p, (platformScores.get(p) || 0) + 1);
      }
    }
  }

  const sorted = Array.from(platformScores.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([p]) => p);

  return sorted.length > 0 ? sorted : [...allowedPlatforms];
}

export function allocateBudget(
  keywords: string[],
  budgetUsd: number,
  podcastEnabled: boolean = true,
  historicalStats?: PlatformStats[],
  enabledPlatforms?: SourceId[],
  emailTarget?: number,
): BudgetAllocation {
  const effectivePodcast = podcastEnabled && budgetUsd >= PODCAST_MIN_BUDGET;
  const platforms = selectPlatformsForKeywords(keywords, effectivePodcast, enabledPlatforms);

  const discoveryBudget = budgetUsd * DISCOVERY_BUDGET_RATIO;
  const enrichmentBudget = budgetUsd * ENRICHMENT_BUDGET_RATIO;

  const platformAllocations: PlatformAllocation[] = [];
  let remainingDiscovery = discoveryBudget;

  const emailYieldScored = platforms.map(p => {
    const emailYield = getEmailYield(p);
    const validRate = getValidEmailRate(p, historicalStats);
    const effectiveValidYield = getEffectiveValidEmailYield(p, historicalStats);
    const cost = PLATFORM_COST_PER_LEAD[p] || 0.02;
    return {
      platform: p,
      yield: emailYield,
      validRate,
      effectiveValidYield,
      cost,
      efficiency: effectiveValidYield / cost,
    };
  }).sort((a, b) => b.efficiency - a.efficiency);

  const totalEfficiency = emailYieldScored.reduce((s, p) => s + p.efficiency, 0) || 1;

  let leadsNeededForTarget = Infinity;
  if (emailTarget && emailTarget > 0) {
    const weightedValidYield = emailYieldScored.reduce((s, p) => {
      const share = p.efficiency / totalEfficiency;
      return s + p.effectiveValidYield * share;
    }, 0);
    leadsNeededForTarget = Math.ceil(emailTarget / Math.max(0.05, weightedValidYield));
  }

  for (const p of emailYieldScored) {
    if (remainingDiscovery <= 0) break;
    const share = p.efficiency / totalEfficiency;
    const platformBudget = Math.min(remainingDiscovery, discoveryBudget * share);
    const budgetBasedLeads = Math.max(MIN_LEADS_PER_PLATFORM, Math.floor(platformBudget / p.cost));
    const maxLeads = budgetBasedLeads;
    const estimatedCost = maxLeads * p.cost;
    remainingDiscovery -= estimatedCost;

    platformAllocations.push({
      platform: p.platform,
      maxLeads,
      estimatedCostUsd: Math.round(estimatedCost * 100) / 100,
      costPerLead: p.cost,
    });
  }

  const estimatedTotalLeads = platformAllocations.reduce((s, p) => s + p.maxLeads, 0);

  const weightedEmailRate = platformAllocations.reduce((s, p) => {
    const yield_ = getEmailYield(p.platform);
    const enrichedYield = Math.min(0.85, yield_ + (1 - yield_) * ENRICHMENT_EMAIL_ADD_RATE);
    return s + enrichedYield * p.maxLeads;
  }, 0) / Math.max(1, estimatedTotalLeads);

  const estimatedEmails = Math.round(estimatedTotalLeads * weightedEmailRate);

  const weightedValidEmailYield = platformAllocations.reduce((s, p) => {
    return s + getEffectiveValidEmailYield(p.platform, historicalStats) * p.maxLeads;
  }, 0) / Math.max(1, estimatedTotalLeads);

  const estimatedValidEmails = Math.round(estimatedTotalLeads * weightedValidEmailYield);

  return {
    totalBudgetUsd: budgetUsd,
    discoveryBudgetUsd: Math.round(discoveryBudget * 100) / 100,
    enrichmentBudgetUsd: Math.round(enrichmentBudget * 100) / 100,
    platforms: platformAllocations,
    estimatedTotalLeads,
    estimatedEmailRate: Math.round(weightedEmailRate * 100) / 100,
    estimatedEmails,
    estimatedValidEmails,
    estimatedValidEmailRate: Math.round(weightedValidEmailYield * 100) / 100,
  };
}

export function estimateBudgetForEmailTarget(
  keywords: string[],
  emailTarget: number,
  podcastEnabled: boolean = true,
  historicalStats?: PlatformStats[],
  enabledPlatforms?: SourceId[],
): BudgetAllocation {
  let low = 0.5;
  let high = 30;
  let bestAllocation = allocateBudget(keywords, high, podcastEnabled, historicalStats, enabledPlatforms, emailTarget);

  for (let i = 0; i < 15; i++) {
    const mid = (low + high) / 2;
    const alloc = allocateBudget(keywords, mid, podcastEnabled, historicalStats, enabledPlatforms, emailTarget);
    if (alloc.estimatedValidEmails >= emailTarget) {
      bestAllocation = alloc;
      high = mid;
    } else {
      low = mid;
    }
  }

  const finalBudget = Math.ceil(high * 2) / 2;
  return allocateBudget(keywords, Math.min(25, finalBudget), podcastEnabled, historicalStats, enabledPlatforms, emailTarget);
}

export function canAffordStep(
  currentSpend: number,
  budgetUsd: number,
  estimatedStepCost: number,
): boolean {
  return (currentSpend + estimatedStepCost) <= budgetUsd * 1.05;
}

export function getRemainingBudget(currentSpend: number, budgetUsd: number): number {
  return Math.max(0, budgetUsd - currentSpend);
}
