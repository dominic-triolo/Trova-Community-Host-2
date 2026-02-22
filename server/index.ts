import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { serveStatic } from "./static";
import { createServer } from "http";
import { activeRunIds, resumeRun, restartRun, resumingRunIds } from "./pipeline";
import { storage } from "./storage";
import { abortAllActiveRuns } from "./apify";

const app = express();
const httpServer = createServer(app);

declare module "http" {
  interface IncomingMessage {
    rawBody: unknown;
  }
}

app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  }),
);

app.use(express.urlencoded({ extended: false }));

export function log(message: string, source = "express") {
  const formattedTime = new Date().toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  });

  console.log(`${formattedTime} [${source}] ${message}`);
}

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      log(logLine);
    }
  });

  next();
});

(async () => {
  await registerRoutes(httpServer, app);

  app.use((err: any, _req: Request, res: Response, next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    console.error("Internal Server Error:", err);

    if (res.headersSent) {
      return next(err);
    }

    return res.status(status).json({ message });
  });

  // importantly only setup vite in development and after
  // setting up all the other routes so the catch-all route
  // doesn't interfere with the other routes
  if (process.env.NODE_ENV === "production") {
    serveStatic(app);
  } else {
    const { setupVite } = await import("./vite");
    await setupVite(httpServer, app);
  }

  // ALWAYS serve the app on the port specified in the environment variable PORT
  // Other ports are firewalled. Default to 5000 if not specified.
  // this serves both the API and the client.
  // It is the only port that is not firewalled.
  const port = parseInt(process.env.PORT || "5000", 10);
  httpServer.listen(
    {
      port,
      host: "0.0.0.0",
      reusePort: true,
    },
    () => {
      log(`serving on port ${port}`);
    },
  );

  const gracefulShutdown = async (signal: string) => {
    log(`Received ${signal}, marking active runs as interrupted...`, "shutdown");

    await abortAllActiveRuns();

    const runIds = Array.from(activeRunIds);
    for (const id of runIds) {
      try {
        const run = await storage.getRun(id);
        if (run && run.status === "running") {
          await storage.updateRun(id, {
            status: "interrupted",
            step: "Interrupted (server restart)",
            finishedAt: new Date(),
            logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Run interrupted by server shutdown (${signal}). Use Resume to continue from where it left off, or Restart to re-run from scratch.\n`,
          });
          log(`Marked run ${id} as interrupted`, "shutdown");
        }
      } catch (err: any) {
        log(`Failed to mark run ${id} as interrupted: ${err.message}`, "shutdown");
      }
    }
    process.exit(0);
  };

  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
  process.on("SIGINT", () => gracefulShutdown("SIGINT"));

  const autoResumeOnStartup = async () => {
    try {
      const allRuns = await storage.listRuns();

      const staleRuns = allRuns.filter(r => r.status === "running" && !activeRunIds.has(r.id) && !resumingRunIds.has(r.id));
      const MAX_INTERRUPTED_AGE_MS = 30 * 60 * 1000;
      const interruptedRuns = allRuns.filter(r => {
        if (r.status !== "interrupted" || activeRunIds.has(r.id) || resumingRunIds.has(r.id)) return false;
        const finishedAt = r.finishedAt ? new Date(r.finishedAt).getTime() : 0;
        const heartbeat = r.lastHeartbeat ? new Date(r.lastHeartbeat).getTime() : 0;
        const lastActivity = Math.max(finishedAt, heartbeat);
        return lastActivity > 0 && (Date.now() - lastActivity) < MAX_INTERRUPTED_AGE_MS;
      });

      const toResume = [...staleRuns, ...interruptedRuns];
      if (toResume.length === 0) return;

      const staleCount = staleRuns.length;
      const interruptedCount = interruptedRuns.length;
      log(`Found ${staleCount} stale + ${interruptedCount} interrupted run(s) — auto-resuming all...`, "auto-resume");

      for (const run of toResume) {
        try {
          if (run.status === "running") {
            await storage.updateRun(run.id, {
              status: "interrupted",
              step: "Interrupted (server restart detected)",
              logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Run detected as stale after server restart. Auto-resuming from checkpoint...\n`,
            });
          } else {
            await storage.updateRun(run.id, {
              logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Server restarted. Auto-resuming interrupted run from checkpoint...\n`,
            });
          }

          log(`Auto-resuming run ${run.id} (was ${run.status}) from checkpoint...`, "auto-resume");
          resumeRun(run.id).then(async () => {
            const updated = await storage.getRun(run.id);
            if (updated && updated.step?.includes("Resume aborted")) {
              log(`Resume of run ${run.id} aborted (no checkpoint). Falling back to restart...`, "auto-resume");
              restartRun(run.id).catch((err2) => {
                log(`Auto-restart fallback for run ${run.id} also failed: ${err2.message}`, "auto-resume");
              });
            }
          }).catch((err) => {
            log(`Auto-resume of run ${run.id} failed: ${err.message}. Attempting restart...`, "auto-resume");
            restartRun(run.id).catch((err2) => {
              log(`Auto-restart fallback for run ${run.id} also failed: ${err2.message}`, "auto-resume");
            });
          });
        } catch (err: any) {
          log(`Failed to auto-resume run ${run.id}: ${err.message}`, "auto-resume");
        }
      }
    } catch (err: any) {
      log(`Auto-resume check failed: ${err.message}`, "auto-resume");
    }
  };

  setTimeout(autoResumeOnStartup, 5000);

  const WATCHDOG_INTERVAL_MS = 3 * 60 * 1000;
  const HEARTBEAT_STALE_MS = 5 * 60 * 1000;

  setInterval(async () => {
    try {
      const allRuns = await storage.listRuns();
      const runningRuns = allRuns.filter(r => r.status === "running");

      for (const run of runningRuns) {
        if (activeRunIds.has(run.id) || resumingRunIds.has(run.id)) continue;

        const heartbeat = run.lastHeartbeat ? new Date(run.lastHeartbeat).getTime() : 0;
        const age = Date.now() - heartbeat;

        if (heartbeat === 0 || age > HEARTBEAT_STALE_MS) {
          log(`Watchdog: run ${run.id} has stale heartbeat (${Math.round(age / 1000)}s old). Auto-resuming...`, "watchdog");

          await storage.updateRun(run.id, {
            status: "interrupted",
            step: "Interrupted (stale heartbeat)",
            logs: (run.logs || "") + `\n[${new Date().toLocaleTimeString("en-US", { hour12: false })}] Heartbeat stale for ${Math.round(age / 1000)}s. Auto-resuming from checkpoint...\n`,
          });

          resumeRun(run.id).then(async () => {
            const updated = await storage.getRun(run.id);
            if (updated && updated.step?.includes("Resume aborted")) {
              log(`Watchdog: resume of run ${run.id} aborted (no checkpoint). Falling back to restart...`, "watchdog");
              restartRun(run.id).catch((err2) => {
                log(`Watchdog restart fallback for run ${run.id} failed: ${err2.message}`, "watchdog");
              });
            }
          }).catch((err) => {
            log(`Watchdog auto-resume of run ${run.id} failed: ${err.message}. Attempting restart...`, "watchdog");
            restartRun(run.id).catch((err2) => {
              log(`Watchdog restart fallback for run ${run.id} failed: ${err2.message}`, "watchdog");
            });
          });
        }
      }
    } catch (err: any) {
      log(`Watchdog check failed: ${err.message}`, "watchdog");
    }
  }, WATCHDOG_INTERVAL_MS);
})();
