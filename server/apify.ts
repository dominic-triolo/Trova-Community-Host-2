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

  const controller = new AbortController();
  const fetchTimeout = setTimeout(() => controller.abort(), 30000);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input),
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Apify start failed (${res.status}): ${text}`);
    }

    const data = await res.json();
    return data.data.id;
  } catch (err: any) {
    if (err.name === "AbortError") {
      throw new Error(`Apify start timed out after 30s for actor ${actorId}`);
    }
    throw err;
  } finally {
    clearTimeout(fetchTimeout);
  }
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

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
