import { log } from "./index";

const APIFY_BASE = "https://api.apify.com/v2";

export const activeApifyRunIds = new Set<string>();

function getToken(): string {
  const token = process.env.APIFY_TOKEN;
  if (!token) throw new Error("APIFY_TOKEN is not set");
  return token;
}

export async function abortApifyRun(runId: string): Promise<void> {
  try {
    const token = getToken();
    const res = await fetch(`${APIFY_BASE}/actor-runs/${runId}/abort?token=${token}`, {
      method: "POST",
    });
    if (res.ok) {
      log(`[APIFY] Aborted run ${runId}`, "apify");
    } else {
      log(`[APIFY] Failed to abort run ${runId}: ${res.status}`, "apify");
    }
  } catch (err: any) {
    log(`[APIFY] Error aborting run ${runId}: ${err.message}`, "apify");
  }
}

export async function abortAllActiveRuns(): Promise<void> {
  const runIds = Array.from(activeApifyRunIds);
  if (runIds.length === 0) return;
  log(`[APIFY] Aborting ${runIds.length} active Apify runs...`, "apify");
  await Promise.allSettled(runIds.map(id => abortApifyRun(id)));
  activeApifyRunIds.clear();
}

export async function startActorRun(actorId: string, input: Record<string, any>): Promise<string> {
  const token = getToken();
  const url = `${APIFY_BASE}/acts/${actorId}/runs?token=${token}`;
  const timeouts = [30000, 60000, 90000];

  for (let attempt = 0; attempt < timeouts.length; attempt++) {
    const timeoutMs = timeouts[attempt];
    const controller = new AbortController();
    const fetchTimeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(input),
        signal: controller.signal,
      });

      if (!res.ok) {
        const text = await res.text();
        const is429or5xx = res.status === 429 || res.status >= 500;
        if (is429or5xx && attempt < timeouts.length - 1) {
          log(`[APIFY] Start failed (${res.status}) for ${actorId}, retrying in ${(attempt + 1) * 5}s (attempt ${attempt + 1}/${timeouts.length})...`, "apify");
          clearTimeout(fetchTimeout);
          await sleep((attempt + 1) * 5000);
          continue;
        }
        throw new Error(`Apify start failed (${res.status}): ${text}`);
      }

      const data = await res.json();
      return data.data.id;
    } catch (err: any) {
      clearTimeout(fetchTimeout);
      if (err.name === "AbortError") {
        if (attempt < timeouts.length - 1) {
          log(`[APIFY] Start timed out after ${timeoutMs / 1000}s for ${actorId}, retrying with ${timeouts[attempt + 1] / 1000}s timeout (attempt ${attempt + 1}/${timeouts.length})...`, "apify");
          continue;
        }
        throw new Error(`Apify start timed out after ${timeoutMs / 1000}s for actor ${actorId} (${timeouts.length} attempts)`);
      }
      const isTransient = err.code === "ECONNRESET" || err.code === "ETIMEDOUT" || err.code === "ENOTFOUND" || err.message?.includes("fetch failed");
      if (isTransient && attempt < timeouts.length - 1) {
        log(`[APIFY] Transient error starting ${actorId}: ${err.message}, retrying in ${(attempt + 1) * 5}s (attempt ${attempt + 1}/${timeouts.length})...`, "apify");
        await sleep((attempt + 1) * 5000);
        continue;
      }
      throw err;
    } finally {
      clearTimeout(fetchTimeout);
    }
  }

  throw new Error(`Apify start failed after ${timeouts.length} attempts for actor ${actorId}`);
}

export interface ApifyRunResult {
  status: "SUCCEEDED" | "FAILED" | "TIMED_OUT";
  usageTotalUsd: number;
}

export async function waitForRun(runId: string, timeoutMs = 300000): Promise<ApifyRunResult> {
  const token = getToken();
  const start = Date.now();
  let consecutiveErrors = 0;

  while (Date.now() - start < timeoutMs) {
    try {
      const controller = new AbortController();
      const fetchTimeout = setTimeout(() => controller.abort(), 15000);

      const res = await fetch(`${APIFY_BASE}/actor-runs/${runId}?token=${token}`, {
        signal: controller.signal,
      });
      clearTimeout(fetchTimeout);

      if (!res.ok) {
        consecutiveErrors++;
        log(`[APIFY] Failed to check run status: ${res.status} (attempt ${consecutiveErrors})`, "apify");
        if (consecutiveErrors >= 10) {
          log(`[APIFY] Too many consecutive polling errors, giving up`, "apify");
          return { status: "FAILED", usageTotalUsd: 0 };
        }
        await sleep(5000);
        continue;
      }

      consecutiveErrors = 0;
      const data = await res.json();
      const status = data.data.status;
      const usageTotalUsd = data.data.usageTotalUsd ?? data.data.usageUsd ?? 0;

      if (status === "SUCCEEDED") return { status: "SUCCEEDED", usageTotalUsd };
      if (status === "FAILED" || status === "ABORTED") return { status: "FAILED", usageTotalUsd };
      if (status === "TIMED-OUT") return { status: "TIMED_OUT", usageTotalUsd };

      await sleep(3000);
    } catch (err: any) {
      consecutiveErrors++;
      log(`[APIFY] Polling error: ${err.message} (attempt ${consecutiveErrors})`, "apify");
      if (consecutiveErrors >= 10) {
        log(`[APIFY] Too many consecutive polling errors, giving up`, "apify");
        return { status: "FAILED", usageTotalUsd: 0 };
      }
      await sleep(5000);
    }
  }

  log(`[APIFY] Run ${runId} timed out after ${timeoutMs}ms`, "apify");
  return { status: "TIMED_OUT", usageTotalUsd: 0 };
}

export async function getDatasetItems(runId: string): Promise<any[]> {
  const token = getToken();
  const res = await fetch(`${APIFY_BASE}/actor-runs/${runId}/dataset/items?token=${token}&format=json`);

  if (!res.ok) {
    log(`[APIFY] Failed to fetch dataset: ${res.status}`, "apify");
    return [];
  }

  return res.json();
}

export interface ActorRunOutput {
  items: any[];
  costUsd: number;
}

export async function runActorAndGetResults(actorId: string, input: Record<string, any>, timeoutMs = 300000, perResultCostUsd?: number): Promise<ActorRunOutput> {
  log(`[APIFY] Starting actor ${actorId}`, "apify");
  const runId = await startActorRun(actorId, input);
  activeApifyRunIds.add(runId);
  log(`[APIFY] Run ${runId} started, waiting...`, "apify");

  try {
    const result = await waitForRun(runId, timeoutMs);
    log(`[APIFY] Run ${runId} finished: ${result.status} (cost: $${result.usageTotalUsd.toFixed(4)})`, "apify");

    if (result.status !== "SUCCEEDED") {
      try {
        const partial = await collectPartialResults(runId, actorId, result.status, result.usageTotalUsd, perResultCostUsd);
        if (partial.items.length > 0) {
          log(`[APIFY] Salvaged ${partial.items.length} partial results from failed run ${runId}`, "apify");
          return partial;
        }
      } catch (partialErr) {
        log(`[APIFY] Failed to collect partial results from ${runId}: ${(partialErr as Error).message}`, "apify");
      }
      const err: any = new Error(`Actor run ${runId} ended with status: ${result.status}`);
      err.costUsd = result.usageTotalUsd;
      throw err;
    }

    const items = await getDatasetItems(runId);
    const costUsd = perResultCostUsd ? items.length * perResultCostUsd : result.usageTotalUsd;
    log(`[APIFY] Got ${items.length} items from run ${runId}${perResultCostUsd ? ` (pay-per-result: $${costUsd.toFixed(2)})` : ''}`, "apify");
    return { items, costUsd };
  } finally {
    activeApifyRunIds.delete(runId);
  }
}

export class ApifyWallClockTimeoutError extends Error {
  costUsd: number;
  constructor(actorId: string, timeoutMs: number, apifyRunId?: string) {
    super(`Wall-clock timeout (${Math.round(timeoutMs / 1000)}s) exceeded for actor ${actorId}${apifyRunId ? ` (run ${apifyRunId} aborted)` : ""}`);
    this.name = "ApifyWallClockTimeoutError";
    this.costUsd = 0;
  }
}

export async function runActorWithWallClockTimeout(
  actorId: string,
  input: Record<string, any>,
  wallClockMs: number,
  perResultCostUsd?: number,
  collectPartialOnTimeout: boolean = false,
): Promise<ActorRunOutput> {
  log(`[APIFY] Starting actor ${actorId} (wall-clock limit: ${Math.round(wallClockMs / 1000)}s${collectPartialOnTimeout ? ", partial-collect enabled" : ""})`, "apify");
  const apifyRunId = await startActorRun(actorId, input);
  activeApifyRunIds.add(apifyRunId);
  log(`[APIFY] Run ${apifyRunId} started, waiting...`, "apify");

  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  let lastKnownCostUsd = 0;

  try {
    const waitPromise = waitForRun(apifyRunId, wallClockMs);
    waitPromise.catch(() => {});

    const result = await Promise.race([
      waitPromise,
      new Promise<never>((_, reject) => {
        timeoutHandle = setTimeout(() => {
          log(`[APIFY] Wall-clock timeout (${Math.round(wallClockMs / 1000)}s) hit for run ${apifyRunId}, aborting...`, "apify");
          abortApifyRun(apifyRunId).catch(() => {});
          reject(new ApifyWallClockTimeoutError(actorId, wallClockMs, apifyRunId));
        }, wallClockMs);
      }),
    ]);

    if (timeoutHandle) clearTimeout(timeoutHandle);

    log(`[APIFY] Run ${apifyRunId} finished: ${result.status} (cost: $${result.usageTotalUsd.toFixed(4)})`, "apify");
    lastKnownCostUsd = result.usageTotalUsd;

    if (result.status !== "SUCCEEDED") {
      if (collectPartialOnTimeout) {
        return await collectPartialResults(apifyRunId, actorId, result.status, result.usageTotalUsd, perResultCostUsd);
      }
      const err: any = new Error(`Actor run ${apifyRunId} ended with status: ${result.status}`);
      err.costUsd = result.usageTotalUsd;
      throw err;
    }

    const items = await getDatasetItems(apifyRunId);
    const costUsd = perResultCostUsd ? items.length * perResultCostUsd : result.usageTotalUsd;
    log(`[APIFY] Got ${items.length} items from run ${apifyRunId}${perResultCostUsd ? ` (pay-per-result: $${costUsd.toFixed(2)})` : ""}`, "apify");
    return { items, costUsd };
  } catch (err) {
    if (timeoutHandle) clearTimeout(timeoutHandle);

    if (collectPartialOnTimeout) {
      try {
        const partial = await collectPartialResults(apifyRunId, actorId, "WALL_CLOCK_TIMEOUT", lastKnownCostUsd, perResultCostUsd);
        return partial;
      } catch (fetchErr) {
        log(`[APIFY] Failed to fetch partial results from ${apifyRunId}: ${(fetchErr as Error).message}`, "apify");
      }
    }

    throw err;
  } finally {
    activeApifyRunIds.delete(apifyRunId);
  }
}

async function fetchRunCost(runId: string): Promise<number> {
  try {
    const token = getToken();
    const res = await fetch(`${APIFY_BASE}/actor-runs/${runId}?token=${token}`);
    if (res.ok) {
      const data = await res.json();
      return data.data?.usageTotalUsd ?? data.data?.usageUsd ?? 0;
    }
  } catch {}
  return 0;
}

async function collectPartialResults(
  runId: string,
  actorId: string,
  status: string,
  knownCostUsd: number,
  perResultCostUsd?: number,
): Promise<ActorRunOutput> {
  await sleep(3000);

  let costUsd = knownCostUsd;
  if (costUsd === 0) {
    costUsd = await fetchRunCost(runId);
  }

  for (let attempt = 0; attempt < 3; attempt++) {
    const partialItems = await getDatasetItems(runId);
    if (partialItems.length > 0) {
      const finalCost = perResultCostUsd ? partialItems.length * perResultCostUsd : costUsd;
      log(`[APIFY] Collected ${partialItems.length} partial results from ${status} run ${runId} (cost: $${finalCost.toFixed(4)}, attempt ${attempt + 1})`, "apify");
      return { items: partialItems, costUsd: finalCost };
    }
    if (attempt < 2) {
      log(`[APIFY] No partial results yet from ${runId}, retrying in 3s (attempt ${attempt + 1}/3)...`, "apify");
      await sleep(3000);
    }
  }

  log(`[APIFY] No partial results available from ${status} run ${runId} after 3 attempts`, "apify");
  const err: any = new Error(`Actor ${actorId} ${status} with no results (run ${runId})`);
  err.costUsd = costUsd;
  throw err;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
