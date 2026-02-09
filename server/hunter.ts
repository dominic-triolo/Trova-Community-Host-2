import { log } from "./index";

const HUNTER_BASE = "https://api.hunter.io/v2";

function getApiKey(): string | null {
  return process.env.HUNTER_API_KEY || null;
}

interface HunterEmail {
  value: string;
  type: string;
  confidence: number;
  first_name: string;
  last_name: string;
  position: string;
  seniority: string;
  department: string;
  linkedin: string | null;
  twitter: string | null;
  phone_number: string | null;
}

interface HunterDomainResult {
  emails: HunterEmail[];
  organization: string;
  pattern: string | null;
}

export async function hunterDomainSearch(domain: string): Promise<HunterDomainResult | null> {
  const apiKey = getApiKey();
  if (!apiKey) return null;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    const res = await fetch(
      `${HUNTER_BASE}/domain-search?domain=${encodeURIComponent(domain)}&limit=10&api_key=${apiKey}`,
      { signal: controller.signal }
    );
    clearTimeout(timeout);

    if (!res.ok) {
      if (res.status === 429) {
        log(`[HUNTER] Rate limited, skipping ${domain}`, "hunter");
        return null;
      }
      if (res.status === 401 || res.status === 403) {
        log(`[HUNTER] Auth error (${res.status}), check API key`, "hunter");
        return null;
      }
      log(`[HUNTER] Error ${res.status} for ${domain}`, "hunter");
      return null;
    }

    const json = await res.json();
    const data = json.data;

    if (!data || !data.emails || data.emails.length === 0) {
      return null;
    }

    return {
      emails: data.emails,
      organization: data.organization || "",
      pattern: data.pattern || null,
    };
  } catch (err: any) {
    if (err.name === "AbortError") {
      log(`[HUNTER] Timeout for ${domain}`, "hunter");
    } else {
      log(`[HUNTER] Error for ${domain}: ${err.message}`, "hunter");
    }
    return null;
  }
}

export function extractDomainFromUrl(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export function isEnrichableDomain(domain: string): boolean {
  const skipDomains = [
    "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
    "reddit.com", "facebook.com", "instagram.com", "twitter.com",
    "x.com", "linkedin.com", "patreon.com", "tiktok.com",
    "google.com", "yelp.com", "tripadvisor.com", "wikipedia.org",
    "amazon.com", "substack.com", "discord.com", "discord.gg",
    "github.com", "medium.com",
  ];
  return !skipDomains.some((d) => domain.includes(d));
}

export function isHunterAvailable(): boolean {
  return !!getApiKey();
}
