import type { SourceId, BudgetAllocation, PlatformAllocation } from "@shared/schema";
import { PLATFORM_COST_PER_LEAD, PLATFORM_EMAIL_YIELD, KEYWORD_PLATFORM_MAP } from "@shared/schema";

const ENRICHMENT_COST_PER_LEAD = 0.005;
const MIN_LEADS_PER_PLATFORM = 10;
const MAX_LEADS_PER_PLATFORM = 200;
const DISCOVERY_BUDGET_RATIO = 0.65;
const ENRICHMENT_BUDGET_RATIO = 0.35;

const ACTIVE_PLATFORMS: SourceId[] = ["patreon", "facebook", "podcast", "substack"];

export function selectPlatformsForKeywords(keywords: string[]): SourceId[] {
  const platformScores = new Map<SourceId, number>();

  for (const kw of keywords) {
    const kwLower = kw.toLowerCase().trim();
    let matched = false;

    for (const [pattern, platforms] of Object.entries(KEYWORD_PLATFORM_MAP)) {
      if (kwLower.includes(pattern) || pattern.includes(kwLower)) {
        for (const p of platforms) {
          if (ACTIVE_PLATFORMS.includes(p)) {
            platformScores.set(p, (platformScores.get(p) || 0) + 2);
            matched = true;
          }
        }
      }
    }

    if (!matched) {
      for (const p of ACTIVE_PLATFORMS) {
        platformScores.set(p, (platformScores.get(p) || 0) + 1);
      }
    }
  }

  const sorted = Array.from(platformScores.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([p]) => p);

  return sorted.length > 0 ? sorted : [...ACTIVE_PLATFORMS];
}

export function allocateBudget(
  keywords: string[],
  budgetUsd: number,
): BudgetAllocation {
  const platforms = selectPlatformsForKeywords(keywords);

  const discoveryBudget = budgetUsd * DISCOVERY_BUDGET_RATIO;
  const enrichmentBudget = budgetUsd * ENRICHMENT_BUDGET_RATIO;

  const platformAllocations: PlatformAllocation[] = [];
  let remainingDiscovery = discoveryBudget;

  const emailYieldScored = platforms.map(p => ({
    platform: p,
    yield: PLATFORM_EMAIL_YIELD[p] || 0.2,
    cost: PLATFORM_COST_PER_LEAD[p] || 0.02,
    efficiency: (PLATFORM_EMAIL_YIELD[p] || 0.2) / (PLATFORM_COST_PER_LEAD[p] || 0.02),
  })).sort((a, b) => b.efficiency - a.efficiency);

  const totalEfficiency = emailYieldScored.reduce((s, p) => s + p.efficiency, 0);

  for (const p of emailYieldScored) {
    if (remainingDiscovery <= 0) break;
    const share = p.efficiency / totalEfficiency;
    const platformBudget = Math.min(remainingDiscovery, discoveryBudget * share);
    const maxLeads = Math.min(
      MAX_LEADS_PER_PLATFORM,
      Math.max(MIN_LEADS_PER_PLATFORM, Math.floor(platformBudget / p.cost))
    );
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
    const yield_ = PLATFORM_EMAIL_YIELD[p.platform] || 0.2;
    return s + yield_ * p.maxLeads;
  }, 0) / Math.max(1, estimatedTotalLeads);

  const enrichedRate = Math.min(0.85, weightedEmailRate + 0.25);

  return {
    totalBudgetUsd: budgetUsd,
    discoveryBudgetUsd: Math.round(discoveryBudget * 100) / 100,
    enrichmentBudgetUsd: Math.round(enrichmentBudget * 100) / 100,
    platforms: platformAllocations,
    estimatedTotalLeads,
    estimatedEmailRate: Math.round(enrichedRate * 100) / 100,
  };
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
