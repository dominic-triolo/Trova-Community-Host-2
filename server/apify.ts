import { log } from "./index";

const APIFY_BASE = "https://api.apify.com/v2";

function getToken(): string {
  const token = process.env.APIFY_TOKEN;
  if (!token) throw new Error("APIFY_TOKEN is not set");
  return token;
}

export async function startActorRun(actorId: string, input: Record<string, any>): Promise<string> {
  const token = getToken();
  const url = `${APIFY_BASE}/acts/${actorId}/runs?token=${token}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Apify start failed (${res.status}): ${text}`);
  }

  const data = await res.json();
  return data.data.id;
}

export async function waitForRun(runId: string, timeoutMs = 300000): Promise<"SUCCEEDED" | "FAILED" | "TIMED_OUT"> {
  const token = getToken();
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const res = await fetch(`${APIFY_BASE}/actor-runs/${runId}?token=${token}`);
    if (!res.ok) {
      log(`[APIFY] Failed to check run status: ${res.status}`, "apify");
      await sleep(5000);
      continue;
    }

    const data = await res.json();
    const status = data.data.status;

    if (status === "SUCCEEDED") return "SUCCEEDED";
    if (status === "FAILED" || status === "ABORTED") return "FAILED";

    await sleep(3000);
  }

  return "TIMED_OUT";
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

export async function runActorAndGetResults(actorId: string, input: Record<string, any>, timeoutMs = 300000): Promise<any[]> {
  log(`[APIFY] Starting actor ${actorId}`, "apify");
  const runId = await startActorRun(actorId, input);
  log(`[APIFY] Run ${runId} started, waiting...`, "apify");

  const status = await waitForRun(runId, timeoutMs);
  log(`[APIFY] Run ${runId} finished: ${status}`, "apify");

  if (status !== "SUCCEEDED") {
    throw new Error(`Actor run ${runId} ended with status: ${status}`);
  }

  const items = await getDatasetItems(runId);
  log(`[APIFY] Got ${items.length} items from run ${runId}`, "apify");
  return items;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
