const MV_API_BASE = "https://api.millionverifier.com/api/v3/";

export type EmailValidationResult = "ok" | "catch_all" | "invalid" | "disposable" | "unknown" | "error";

export interface VerifyResult {
  email: string;
  result: EmailValidationResult;
  quality: string;
  subresult: string;
  free: boolean;
  role: boolean;
  didyoumean: string;
  credits: number;
}

function getApiKey(): string {
  const key = process.env.MILLIONVERIFIER_API_KEY;
  if (!key) throw new Error("MILLIONVERIFIER_API_KEY not set");
  return key;
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function verifyEmail(email: string): Promise<VerifyResult> {
  const apiKey = getApiKey();
  const url = `${MV_API_BASE}?api=${encodeURIComponent(apiKey)}&email=${encodeURIComponent(email)}&timeout=15`;

  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`MillionVerifier API error: ${res.status} ${res.statusText}`);
  }

  const data = await res.json() as any;

  if (data.error && data.error !== "") {
    throw new Error(`MillionVerifier error: ${data.error}`);
  }

  return {
    email: data.email || email,
    result: data.result || "unknown",
    quality: data.quality || "unknown",
    subresult: data.subresult || "",
    free: !!data.free,
    role: !!data.role,
    didyoumean: data.didyoumean || "",
    credits: data.credits ?? 0,
  };
}

export async function verifyEmailBatch(
  emails: { email: string; leadId: number }[],
  onProgress?: (verified: number, total: number) => void,
): Promise<Map<number, VerifyResult>> {
  const results = new Map<number, VerifyResult>();
  const batchSize = 10;

  for (let i = 0; i < emails.length; i += batchSize) {
    const batch = emails.slice(i, i + batchSize);

    const promises = batch.map(async ({ email, leadId }) => {
      try {
        const result = await verifyEmail(email);
        results.set(leadId, result);
      } catch (err: any) {
        results.set(leadId, {
          email,
          result: "error",
          quality: "unknown",
          subresult: err.message || "verification failed",
          free: false,
          role: false,
          didyoumean: "",
          credits: 0,
        });
      }
    });

    await Promise.all(promises);
    onProgress?.(Math.min(i + batchSize, emails.length), emails.length);

    if (i + batchSize < emails.length) {
      await delay(100);
    }
  }

  return results;
}

export async function checkCredits(): Promise<number> {
  const apiKey = getApiKey();
  const res = await fetch(`${MV_API_BASE}credits?api=${encodeURIComponent(apiKey)}`);
  if (!res.ok) throw new Error(`MillionVerifier credits check failed: ${res.status}`);
  const data = await res.json() as any;
  return data.credits ?? 0;
}

export function mapResultToValidation(result: EmailValidationResult): string {
  switch (result) {
    case "ok": return "valid";
    case "catch_all": return "catch-all";
    case "invalid": return "invalid";
    case "disposable": return "invalid";
    case "unknown": return "unknown";
    case "error": return "unknown";
    default: return "unknown";
  }
}
