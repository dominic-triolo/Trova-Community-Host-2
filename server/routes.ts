import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { runPipeline } from "./pipeline";
import { runParamsSchema, DEFAULT_RUN_PARAMS } from "@shared/schema";
import { fromError } from "zod-validation-error";

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {

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

  app.get("/api/leads", async (_req, res) => {
    try {
      const leads = await storage.listLeads();
      res.json(leads);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  app.get("/api/exports/:type", async (req, res) => {
    try {
      const type = req.params.type;
      let leads;
      if (type === "qualified") {
        leads = await storage.listLeadsByStatus("qualified");
      } else if (type === "watchlist") {
        leads = await storage.listLeadsByStatus("watchlist");
      } else {
        leads = await storage.listLeads();
      }

      const headers = [
        "communityName", "communityType", "leaderName", "leaderRole",
        "location", "website", "email", "phone", "linkedin",
        "score", "status", "ownedChannels", "monetizationSignals",
      ];

      const csvRows = [headers.join(",")];
      for (const lead of leads) {
        const row = headers.map((h) => {
          const val = (lead as any)[h];
          if (val === null || val === undefined) return "";
          if (typeof val === "object") return `"${JSON.stringify(val).replace(/"/g, '""')}"`;
          return `"${String(val).replace(/"/g, '""')}"`;
        });
        csvRows.push(row.join(","));
      }

      const csv = csvRows.join("\n");
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename=${type}_leads.csv`);
      res.send(csv);
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  return httpServer;
}
