import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { runPipeline, reEnrichRun, resumeRun, restartRun, activeRunIds, cancelledRunIds } from "./pipeline";
import { verifyEmailBatch, mapResultToValidation } from "./millionverifier";
import { checkEmailsInHubspot, isHubspotConfigured } from "./hubspot";
import { syncHubSpotDeals, computeScoringWeights, getLatestInsights, getLatestWeights } from "./hubspot-sync";
import { clearWeightsCache } from "./scoring";
import { runParamsSchema, DEFAULT_RUN_PARAMS, PLATFORM_COST_PER_LEAD, PLATFORM_EMAIL_YIELD, PLATFORM_VALID_EMAIL_RATE, type SourceId } from "@shared/schema";
import { fromError } from "zod-validation-error";
import { log } from "./index";
import { allocateBudget, estimateBudgetForEmailTarget } from "./budget-engine";

async function recoverStuckRuns() {
  try {
    const runs = await storage.listRuns();
    const stuckRuns = runs.filter((r) => r.status === "running" && !activeRunIds.has(r.id));
    for (const run of stuckRuns) {
      log(`Recovering stuck run ${run.id} (was in status "running" with no active process)`, "startup");
      await storage.updateRun(run.id, {
        status: "interrupted",
        step: "Interrupted (server restart)",
        finishedAt: new Date(),
        logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Run interrupted by server restart. Use Resume to continue from where it left off, or Re-enrich to run Apollo/Leads Finder only.\n`,
      });
    }
    if (stuckRuns.length > 0) {
      log(`Recovered ${stuckRuns.length} stuck run(s) as interrupted`, "startup");
    }
  } catch (err: any) {
    log(`Failed to recover stuck runs: ${err.message}`, "startup");
  }
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  recoverStuckRuns();

  app.post("/api/auth", (req, res) => {
    const { password } = req.body || {};
    const sitePassword = process.env.SITE_PASSWORD;
    if (!sitePassword) {
      return res.status(500).json({ message: "Site password not configured" });
    }
    if (password === sitePassword) {
      return res.json({ success: true });
    }
    return res.status(401).json({ success: false, message: "Incorrect password" });
  });

  app.post("/api/runs", async (req, res) => {
    try {
      const parsed = runParamsSchema.safeParse(req.body);
      if (!parsed.success) {
        const validationError = fromError(parsed.error);
        return res.status(400).json({ message: validationError.toString() });
      }

      const params = {
        ...DEFAULT_RUN_PARAMS,
        ...parsed.data,
      };

      const run = await storage.createRun({
        status: "queued",
        progress: 0,
        step: "Queued",
        logs: "",
        params,
      });

      setTimeout(() => {
        runPipeline(run.id).catch((err) => {
          console.error(`Pipeline run ${run.id} error:`, err);
        });
      }, 100);

      res.json({ id: run.id });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/stats/platforms", async (_req, res) => {
    try {
      const historicalStats = await storage.getPlatformValidEmailStats();
      const allPlatforms: SourceId[] = ["patreon", "facebook", "podcast", "substack"];
      const stats = allPlatforms.map(p => {
        const hist = historicalStats.find(s => s.platform === p);
        const costPerLead = PLATFORM_COST_PER_LEAD[p] || 0.02;
        const emailYield = PLATFORM_EMAIL_YIELD[p] || 0.20;
        const defaultValidRate = PLATFORM_VALID_EMAIL_RATE[p] || 0.25;

        const totalLeads = hist?.totalLeads || 0;
        const withEmail = hist?.withEmail || 0;
        const validEmails = hist?.validEmails || 0;
        const hasHistory = totalLeads >= 5;

        const validRatePerLead = hasHistory && totalLeads > 0
          ? validEmails / totalLeads
          : emailYield * defaultValidRate;

        const costPerValidEmail = validRatePerLead > 0
          ? costPerLead / validRatePerLead
          : costPerLead / (emailYield * defaultValidRate);

        return {
          platform: p,
          totalLeads,
          withEmail,
          validEmails,
          validRatePerLead: Math.round(validRatePerLead * 1000) / 10,
          costPerValidEmail: Math.round(costPerValidEmail * 100) / 100,
          isHistorical: hasHistory,
        };
      });
      res.json(stats);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/autonomous/preview", async (req, res) => {
    try {
      const { keywords, budgetUsd, emailTarget, podcastEnabled, enabledPlatforms } = req.body;
      if (!keywords || !Array.isArray(keywords) || keywords.length === 0) {
        return res.status(400).json({ message: "Keywords are required" });
      }
      const podcast = podcastEnabled !== false;
      const historicalStats = await storage.getPlatformValidEmailStats();
      const platforms = Array.isArray(enabledPlatforms) && enabledPlatforms.length > 0 ? enabledPlatforms : undefined;

      if (emailTarget && Number(emailTarget) > 0) {
        const allocation = estimateBudgetForEmailTarget(keywords, Number(emailTarget), podcast, historicalStats, platforms);
        return res.json({ allocation, derivedFrom: "emailTarget" });
      }
      const budget = Number(budgetUsd) || 5;
      const allocation = allocateBudget(keywords, Math.max(1, Math.min(20, budget)), podcast, historicalStats, platforms);
      return res.json({ allocation, derivedFrom: "budget" });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/autonomous", async (req, res) => {
    try {
      const { keywords, budgetUsd, emailTarget, podcastEnabled, enabledPlatforms } = req.body;
      if (!keywords || !Array.isArray(keywords) || keywords.length === 0) {
        return res.status(400).json({ message: "Keywords are required" });
      }
      const podcast = podcastEnabled !== false;
      const historicalStats = await storage.getPlatformValidEmailStats();
      const platforms = Array.isArray(enabledPlatforms) && enabledPlatforms.length > 0 ? enabledPlatforms : undefined;

      let allocation;
      let budget: number;
      let finalEmailTarget: number;

      if (emailTarget && Number(emailTarget) > 0) {
        allocation = estimateBudgetForEmailTarget(keywords, Number(emailTarget), podcast, historicalStats, platforms);
        budget = budgetUsd && Number(budgetUsd) > 0 ? Number(budgetUsd) : allocation.totalBudgetUsd;
        finalEmailTarget = Number(emailTarget);
      } else {
        budget = Number(budgetUsd);
        if (!budget || budget < 1 || budget > 20) {
          return res.status(400).json({ message: "Budget must be between $1 and $20" });
        }
        allocation = allocateBudget(keywords, budget, podcast, historicalStats, platforms);
        finalEmailTarget = allocation.estimatedEmails;
      }

      const enabledSources = allocation.platforms.map(p => p.platform);
      const totalLeads = allocation.platforms.reduce((s, p) => s + p.maxLeads, 0);

      const params = {
        ...DEFAULT_RUN_PARAMS,
        seedKeywords: keywords,
        seedGeos: [],
        enabledSources,
        maxDiscoveredUrls: totalLeads,
        enableApollo: allocation.enrichmentBudgetUsd > 0,
        enableLeadsFinder: allocation.enrichmentBudgetUsd > 0.05,
      };

      const run = await storage.createRun({
        status: "queued",
        progress: 0,
        step: "Queued",
        logs: "",
        params,
        isAutonomous: true,
        budgetUsd: budget,
        budgetAllocation: allocation,
        emailTarget: finalEmailTarget,
        podcastEnabled: podcast,
      });

      setTimeout(() => {
        runPipeline(run.id).catch((err) => {
          console.error(`Autonomous pipeline run ${run.id} error:`, err);
        });
      }, 100);

      res.json({ id: run.id, allocation });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/runs", async (_req, res) => {
    try {
      const runs = await storage.listRuns();
      res.json(runs);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/runs/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ message: "Invalid run id" });

      const run = await storage.getRun(id);
      if (!run) return res.status(404).json({ message: "Run not found" });

      const netNewValidEmails = await storage.countLeadsByRunWithNetNewValidEmail(id);
      res.json({ ...run, netNewValidEmails });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/:id/cancel", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ message: "Invalid run id" });

      const run = await storage.getRun(id);
      if (!run) return res.status(404).json({ message: "Run not found" });

      const cancellableStatuses = ["running", "queued", "interrupted"];
      if (!cancellableStatuses.includes(run.status)) {
        return res.status(409).json({ message: `Run cannot be stopped (status: ${run.status})` });
      }

      cancelledRunIds.add(id);
      activeRunIds.delete(id);

      if (run.status === "queued" || run.status === "interrupted") {
        const emailCount = await storage.countLeadsByRunWithEmail(id);
        const validEmailCount = await storage.countLeadsByRunWithValidEmail(id);
        await storage.updateRun(id, {
          status: "stopped",
          step: "Stopped by user",
          leadsWithEmail: emailCount,
          leadsWithValidEmail: validEmailCount,
          finishedAt: new Date(),
          logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Run stopped by user. ${emailCount} leads with email preserved.\n`,
        });
      }

      res.json({ message: "Stop requested", runId: id });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/:id/re-enrich", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ message: "Invalid run id" });

      const run = await storage.getRun(id);
      if (!run) return res.status(404).json({ message: "Run not found" });
      if (run.status === "running") return res.status(409).json({ message: "Run is already in progress" });

      reEnrichRun(id).catch((err) => console.error("Re-enrich error:", err));
      res.json({ message: "Re-enrichment started", runId: id });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/:id/resume", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ message: "Invalid run id" });

      const run = await storage.getRun(id);
      if (!run) return res.status(404).json({ message: "Run not found" });
      if (run.status === "running") return res.status(409).json({ message: "Run is already in progress" });
      if (run.status !== "interrupted" && run.status !== "failed" && run.status !== "stopped") {
        return res.status(400).json({ message: "Only interrupted, failed, or stopped runs can be resumed" });
      }

      resumeRun(id).catch((err) => console.error("Resume error:", err));
      res.json({ message: "Resume started", runId: id });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/:id/restart", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ message: "Invalid run id" });

      const run = await storage.getRun(id);
      if (!run) return res.status(404).json({ message: "Run not found" });
      if (run.status === "running") return res.status(409).json({ message: "Run is already in progress" });
      if (run.status !== "interrupted" && run.status !== "failed" && run.status !== "stopped") {
        return res.status(400).json({ message: "Only interrupted, failed, or stopped runs can be restarted" });
      }

      restartRun(id).catch((err) => console.error("Restart error:", err));
      res.json({ message: "Restart started", runId: id });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/leads", async (req, res) => {
    try {
      const runId = req.query.runId ? parseInt(req.query.runId as string) : null;
      let leads;
      if (runId && !isNaN(runId)) {
        leads = await storage.listLeadsByRun(runId);
      } else {
        leads = await storage.listLeads();
      }
      res.json(leads);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/exports/csv", async (req, res) => {
    try {
      const runId = req.query.runId ? parseInt(req.query.runId as string) : null;
      let leads;

      if (runId && !isNaN(runId)) {
        leads = await storage.listLeadsByRun(runId);
      } else {
        leads = await storage.listLeads();
      }

      const headers = [
        "Source", "Community Name", "Community Type", "Leader Name", "Leader Role",
        "Location", "Website", "Email", "Email Status", "Phone", "LinkedIn",
        "Patreon URL", "Personal Website",
        "Member/Patron Count", "Subscriber Count", "Episode Count", "Post/Video Count",
        "Instagram Followers", "Twitter Followers", "Genre",
        "YouTube", "Instagram", "Twitter", "Facebook", "TikTok",
        "Discord", "Twitch", "Substack", "Linktree",
        "Podcast URL", "RSS Feed URL",
        "Has Sponsorships", "Has Merch", "Has Courses", "Has Membership", "Established Creator",
        "Score",
        "Niche Score", "Trust Score", "Engagement Score", "Monetization Score", "Channels Score", "Trip Fit Score",
        "HubSpot Status",
        "Discovered At",
      ];

      const esc = (v: any) => {
        if (v === null || v === undefined || v === "") return "";
        const s = String(v).replace(/"/g, '""');
        return `"${s}"`;
      };

      const sourceDisplayNames: Record<string, string> = {
        patreon: "Patreon",
        facebook: "Facebook Groups",
        podcast: "Podcast",
        substack: "Substack",
        meetup: "Meetup",
        youtube: "YouTube",
        reddit: "Reddit",
        eventbrite: "Eventbrite",
        google: "Google Search",
      };

      const csvRows = [headers.join(",")];
      for (const lead of leads) {
        const engagement = (lead.engagementSignals as Record<string, any>) || {};
        const channels = (lead.ownedChannels as Record<string, string>) || {};
        const breakdown = (lead.scoreBreakdown as any) || {};
        const monetization = (lead.monetizationSignals as Record<string, any>) || {};
        const raw = (lead.raw as Record<string, any>) || {};

        const row = [
          esc(sourceDisplayNames[lead.source || ""] || lead.source || ""),
          esc(lead.communityName),
          esc(lead.communityType),
          esc(lead.leaderName),
          esc(lead.leaderRole),
          esc(lead.location),
          esc(lead.website),
          esc(lead.email),
          esc(lead.emailValidation || ""),
          esc(lead.phone),
          esc(lead.linkedin),
          esc(channels.patreon || ""),
          esc(channels.website || ""),
          (engagement.member_count || engagement.patron_count) ? (engagement.member_count ?? engagement.patron_count) : "",
          engagement.subscriber_count || "",
          engagement.episode_count || "",
          (engagement.post_count || engagement.total_videos) ? (engagement.post_count ?? engagement.total_videos) : "",
          engagement.instagram_followers || "",
          engagement.twitter_followers || "",
          esc(engagement.genre || ""),
          esc(channels.youtube || ""),
          esc(channels.instagram || ""),
          esc(channels.twitter || ""),
          esc(channels.facebook || ""),
          esc(channels.tiktok || ""),
          esc(channels.discord || ""),
          esc(channels.twitch || ""),
          esc(channels.substack || ""),
          esc(channels.linktree || ""),
          esc(channels.podcast || raw.itunes_url || raw.url || ""),
          esc(channels.rss || raw.feedUrl || ""),
          monetization.sponsored ? "Yes" : "",
          monetization.merch ? "Yes" : "",
          monetization.courses ? "Yes" : "",
          monetization.membership || monetization.patreon ? "Yes" : "",
          monetization.established ? "Yes" : "",
          lead.score ?? 0,
          breakdown.nicheIdentity ?? "",
          breakdown.trustLeadership ?? "",
          breakdown.engagement ?? "",
          breakdown.monetization ?? "",
          breakdown.ownedChannels ?? "",
          breakdown.tripFit ?? "",
          esc(lead.hubspotStatus === "existing" ? "Existing" : lead.hubspotStatus === "net_new" ? "Net New" : ""),
          esc(lead.firstSeenAt ? new Date(lead.firstSeenAt).toISOString() : ""),
        ];
        csvRows.push(row.join(","));
      }

      const csv = csvRows.join("\n");
      const filename = runId ? `run${runId}_leads.csv` : `all_leads.csv`;
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename=${filename}`);
      res.send(csv);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/admin/fix-fb-names", async (req, res) => {
    try {
      const sitePassword = process.env.SITE_PASSWORD;
      if (sitePassword && req.body.password !== sitePassword) {
        return res.status(403).json({ message: "Unauthorized" });
      }
      const runId = req.body.runId;
      if (!runId) return res.status(400).json({ message: "runId required" });
      const allLeads = await storage.listLeadsByRun(runId);
      const fbLeads = allLeads.filter((l: any) => l.source === "facebook");
      let fixed = 0;
      let duped = 0;
      const seenGroupUrls = new Set<string>();

      for (const lead of fbLeads) {
        const raw = lead.raw as any;
        const rawUrl = raw?.url || raw?.link || "";
        const channels = lead.ownedChannels as Record<string, string> | null;
        const groupUrl = channels?.facebook || "";

        if (seenGroupUrls.has(groupUrl) && groupUrl) {
          duped++;
          continue;
        }
        if (groupUrl) seenGroupUrls.add(groupUrl);

        const isPostUrl = /\/groups\/[^/]+\/posts\//i.test(rawUrl);
        if (!isPostUrl) continue;

        const snippet = raw?.description || raw?.snippet || "";
        const slug = rawUrl.match(/\/groups\/([^/]+)\//)?.[1] || "";
        const isNumericSlug = /^\d+$/.test(slug);

        const fromSnippet = extractGroupNameFromSnippetStatic(snippet);
        const newName = fromSnippet || (isNumericSlug ? "" : slugToGroupNameStatic(slug)) || `FB Group ${slug}`;

        if (newName && newName !== lead.communityName) {
          await storage.updateLead(lead.id, { communityName: newName });
          fixed++;
        }
      }

      res.json({ fixed, duped, total: fbLeads.length });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/hubspot/sync", async (req, res) => {
    try {
      if (!isHubspotConfigured()) {
        return res.status(400).json({ message: "HubSpot is not configured. Set HUBSPOT_ACCESS_TOKEN." });
      }
      log("Starting HubSpot deal sync...", "hubspot-learn");
      const syncResult = await syncHubSpotDeals();
      log(`HubSpot sync complete: ${syncResult.profilesCreated} profiles from ${syncResult.dealsFound} deals`, "hubspot-learn");

      log("Computing scoring weights from host profiles...", "hubspot-learn");
      const weightsResult = await computeScoringWeights();
      clearWeightsCache();
      log(`Scoring weights computed from ${weightsResult.sampleSize} profiles (${weightsResult.topHostCount} top hosts)`, "hubspot-learn");

      res.json({
        sync: syncResult,
        weights: weightsResult.weights,
        insights: weightsResult.insights,
        sampleSize: weightsResult.sampleSize,
        topHostCount: weightsResult.topHostCount,
      });
    } catch (err: any) {
      log(`HubSpot sync error: ${err.message}`, "hubspot-learn");
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/hubspot/insights", async (_req, res) => {
    try {
      const weights = await getLatestWeights();
      const insights = await getLatestInsights();
      const row = await storage.getLatestScoringWeights();
      if (!row) {
        return res.json({ hasData: false });
      }
      res.json({
        hasData: true,
        weights,
        insights,
        sampleSize: row.sampleSize,
        topHostCount: row.topHostCount,
        computedAt: row.computedAt,
      });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/hubspot/host-profiles", async (_req, res) => {
    try {
      const profiles = await storage.listHostProfiles();
      res.json(profiles);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/backfill/verify-and-check", async (req, res) => {
    try {
      const sitePassword = process.env.SITE_PASSWORD;
      if (sitePassword && req.body.password !== sitePassword) {
        return res.status(403).json({ message: "Unauthorized" });
      }

      const allLeads = await storage.listLeads();
      const summary: any = { totalLeads: allLeads.length, emailVerified: 0, hubspotChecked: 0, errors: [] };

      const needsVerification = allLeads.filter(l => l.email && (!l.emailValidation || l.emailValidation === ""));
      summary.needsVerification = needsVerification.length;
      log(`Backfill: ${needsVerification.length} leads need email verification`, "backfill");

      if (needsVerification.length > 0) {
        const emailInputs = needsVerification.map(l => ({ email: l.email!, leadId: l.id }));
        try {
          const verifyResults = await verifyEmailBatch(emailInputs, (done, total) => {
            log(`Backfill: MillionVerifier progress ${done}/${total}`, "backfill");
          });
          const verifyEntries = Array.from(verifyResults.entries());
          for (const [leadId, result] of verifyEntries) {
            const validation = mapResultToValidation(result.result);
            await storage.updateLead(leadId, { emailValidation: validation });
            summary.emailVerified++;
          }
          log(`Backfill: Verified ${summary.emailVerified} emails`, "backfill");
        } catch (err: any) {
          log(`Backfill: MillionVerifier error: ${err.message}`, "backfill");
          summary.errors.push(`MillionVerifier: ${err.message}`);
        }
      }

      if (isHubspotConfigured()) {
        const refreshedLeads = await storage.listLeads();
        const needsHubspot = refreshedLeads.filter(l =>
          l.email && l.emailValidation === "valid" && (!(l as any).hubspotStatus || (l as any).hubspotStatus === "")
        );
        summary.needsHubspot = needsHubspot.length;
        log(`Backfill: ${needsHubspot.length} leads need HubSpot check`, "backfill");

        if (needsHubspot.length > 0) {
          try {
            const emailToLeadIds = new Map<string, number[]>();
            for (const lead of needsHubspot) {
              const email = lead.email!.toLowerCase();
              if (!emailToLeadIds.has(email)) emailToLeadIds.set(email, []);
              emailToLeadIds.get(email)!.push(lead.id);
            }
            const uniqueEmails = Array.from(emailToLeadIds.keys());
            const hubResults = await checkEmailsInHubspot(uniqueEmails);
            const hubEntries = Array.from(hubResults.entries());
            for (const [email, exists] of hubEntries) {
              const status = exists ? "existing" : "net_new";
              const leadIds = emailToLeadIds.get(email.toLowerCase()) || [];
              for (const leadId of leadIds) {
                await storage.updateLead(leadId, { hubspotStatus: status });
                summary.hubspotChecked++;
              }
            }
            log(`Backfill: HubSpot checked ${summary.hubspotChecked} leads`, "backfill");
          } catch (err: any) {
            log(`Backfill: HubSpot error: ${err.message}`, "backfill");
            summary.errors.push(`HubSpot: ${err.message}`);
          }
        }
      } else {
        summary.hubspotSkipped = "HUBSPOT_ACCESS_TOKEN not configured";
      }

      log(`Backfill complete: ${summary.emailVerified} verified, ${summary.hubspotChecked} HubSpot checked`, "backfill");
      res.json(summary);
    } catch (err: any) {
      log(`Backfill error: ${err.message}`, "backfill");
      res.status(500).json({ message: err.message });
    }
  });

  app.post("/api/runs/:id/hubspot-check", async (req, res) => {
    try {
      const runId = parseInt(req.params.id);
      if (!isHubspotConfigured()) {
        return res.status(400).json({ message: "HubSpot not configured" });
      }
      const leads = await storage.listLeadsByRun(runId);
      const needsHubspot = leads.filter(l =>
        l.email && l.emailValidation === "valid" && (!l.hubspotStatus || l.hubspotStatus === "")
      );
      if (needsHubspot.length === 0) {
        return res.json({ message: "All valid emails already checked", checked: 0 });
      }
      const emailToLeadIds = new Map<string, number[]>();
      for (const lead of needsHubspot) {
        const email = lead.email!.toLowerCase();
        if (!emailToLeadIds.has(email)) emailToLeadIds.set(email, []);
        emailToLeadIds.get(email)!.push(lead.id);
      }
      const hubResults = await checkEmailsInHubspot(Array.from(emailToLeadIds.keys()));
      let existing = 0, netNew = 0;
      for (const [email, exists] of Array.from(hubResults.entries())) {
        const status = exists ? "existing" : "net_new";
        const leadIds = emailToLeadIds.get(email.toLowerCase()) || [];
        for (const leadId of leadIds) {
          await storage.updateLead(leadId, { hubspotStatus: status });
          if (exists) existing++; else netNew++;
        }
      }
      log(`HubSpot check for run ${runId}: ${existing} existing, ${netNew} net new out of ${needsHubspot.length}`, "hubspot");
      res.json({ checked: existing + netNew, existing, netNew });
    } catch (err: any) {
      log(`HubSpot check error: ${err.message}`, "hubspot");
      res.status(500).json({ message: err.message });
    }
  });

  return httpServer;
}

function extractGroupNameFromSnippetStatic(snippet: string): string {
  const arrowMatch = snippet.match(/▻\s*(.+?)(?:\.\s|\s*$)/);
  if (arrowMatch && arrowMatch[1] && arrowMatch[1].length > 3 && arrowMatch[1].length < 100) {
    let name = arrowMatch[1].trim();
    name = name.replace(/\s*\((?:www|http).*$/i, "").trim();
    if (name.length > 3) return name;
  }
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

function slugToGroupNameStatic(slug: string): string {
  return slug
    .replace(/[-_]+/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}
