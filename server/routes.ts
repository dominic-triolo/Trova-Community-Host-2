import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { runPipeline, reEnrichRun, resumeRun, activeRunIds, cancelledRunIds } from "./pipeline";
import { runParamsSchema, DEFAULT_RUN_PARAMS } from "@shared/schema";
import { fromError } from "zod-validation-error";
import { log } from "./index";

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

      res.json(run);
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
      if (run.status !== "running" && run.status !== "queued") {
        return res.status(409).json({ message: "Run is not currently active" });
      }

      cancelledRunIds.add(id);

      if (run.status === "queued") {
        await storage.updateRun(id, {
          status: "failed",
          step: "Cancelled by user",
          finishedAt: new Date(),
          logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Run cancelled by user\n`,
        });
      }

      res.json({ message: "Cancellation requested", runId: id });
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
      if (run.status !== "interrupted" && run.status !== "failed") {
        return res.status(400).json({ message: "Only interrupted or failed runs can be resumed" });
      }

      resumeRun(id).catch((err) => console.error("Resume error:", err));
      res.json({ message: "Resume started", runId: id });
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
          engagement.member_count ?? engagement.patron_count ?? "",
          engagement.subscriber_count ?? "",
          engagement.episode_count ?? "",
          engagement.post_count ?? engagement.total_videos ?? "",
          engagement.instagram_followers ?? "",
          engagement.twitter_followers ?? "",
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

  return httpServer;
}
