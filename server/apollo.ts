import { log } from "./index";

const APOLLO_BASE = "https://api.apollo.io/api/v1";

function getApiKey(): string | null {
  return process.env.APOLLO_API_KEY || null;
}

export function isApolloAvailable(): boolean {
  return !!getApiKey();
}

interface ApolloPersonMatch {
  email: string | null;
  first_name: string | null;
  last_name: string | null;
  name: string | null;
  title: string | null;
  linkedin_url: string | null;
  twitter_url: string | null;
  facebook_url: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
  headline: string | null;
  email_status: string | null;
  photo_url: string | null;
  organization: {
    name: string | null;
    website_url: string | null;
    linkedin_url: string | null;
    phone: string | null;
    primary_domain: string | null;
  } | null;
  phone_numbers?: Array<{
    raw_number: string;
    sanitized_number: string;
    type: string | null;
  }>;
}

export interface ApolloEnrichResult {
  email: string;
  firstName: string;
  lastName: string;
  fullName: string;
  title: string;
  linkedin: string;
  twitter: string;
  facebook: string;
  phone: string;
  location: string;
  headline: string;
  orgName: string;
  orgPhone: string;
  emailStatus: string;
}

async function apolloFetch(endpoint: string, body: Record<string, any>): Promise<any> {
  const apiKey = getApiKey();
  if (!apiKey) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    const res = await fetch(`${APOLLO_BASE}${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": apiKey,
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!res.ok) {
      if (res.status === 429) {
        log(`[APOLLO] Rate limited`, "apollo");
        return null;
      }
      if (res.status === 401 || res.status === 403) {
        log(`[APOLLO] Auth error (${res.status}), check API key`, "apollo");
        return null;
      }
      log(`[APOLLO] Error ${res.status}`, "apollo");
      return null;
    }

    return await res.json();
  } catch (err: any) {
    clearTimeout(timeout);
    if (err.name === "AbortError") {
      log(`[APOLLO] Timeout`, "apollo");
    } else {
      log(`[APOLLO] Error: ${err.message}`, "apollo");
    }
    return null;
  }
}

export async function apolloPersonMatch(opts: {
  name?: string;
  firstName?: string;
  lastName?: string;
  domain?: string;
  organizationName?: string;
  linkedinUrl?: string;
  email?: string;
}): Promise<ApolloEnrichResult | null> {
  const body: Record<string, any> = {
    reveal_personal_emails: true,
  };

  if (opts.name) body.name = opts.name;
  if (opts.firstName) body.first_name = opts.firstName;
  if (opts.lastName) body.last_name = opts.lastName;
  if (opts.domain) body.domain = opts.domain;
  if (opts.organizationName) body.organization_name = opts.organizationName;
  if (opts.linkedinUrl) body.linkedin_url = opts.linkedinUrl;
  if (opts.email) body.email = opts.email;

  const json = await apolloFetch("/people/match", body);
  if (!json || !json.person) return null;

  const p: ApolloPersonMatch = json.person;

  if (!p.email && !p.linkedin_url && !p.twitter_url) {
    return null;
  }

  const phoneNum = (p.phone_numbers && p.phone_numbers.length > 0)
    ? p.phone_numbers[0].raw_number || p.phone_numbers[0].sanitized_number || ""
    : "";

  const locationParts = [p.city, p.state, p.country].filter(Boolean);

  return {
    email: p.email || "",
    firstName: p.first_name || "",
    lastName: p.last_name || "",
    fullName: p.name || "",
    title: p.title || "",
    linkedin: p.linkedin_url || "",
    twitter: p.twitter_url || "",
    facebook: p.facebook_url || "",
    phone: phoneNum,
    location: locationParts.join(", "),
    headline: p.headline || "",
    orgName: p.organization?.name || "",
    orgPhone: p.organization?.phone || "",
    emailStatus: p.email_status || "",
  };
}

export async function apolloBulkMatch(
  people: Array<{
    name?: string;
    firstName?: string;
    lastName?: string;
    domain?: string;
    organizationName?: string;
  }>
): Promise<Array<ApolloEnrichResult | null>> {
  const details = people.map((p) => {
    const d: Record<string, any> = { reveal_personal_emails: true };
    if (p.name) d.name = p.name;
    if (p.firstName) d.first_name = p.firstName;
    if (p.lastName) d.last_name = p.lastName;
    if (p.domain) d.domain = p.domain;
    if (p.organizationName) d.organization_name = p.organizationName;
    return d;
  });

  const json = await apolloFetch("/people/bulk_match", { details, reveal_personal_emails: true });
  if (!json || !json.matches) return people.map(() => null);

  return (json.matches as any[]).map((match: any) => {
    if (!match) return null;
    const p = match;

    if (!p.email && !p.linkedin_url && !p.twitter_url) {
      return null;
    }

    const phoneNum = (p.phone_numbers && p.phone_numbers.length > 0)
      ? p.phone_numbers[0].raw_number || p.phone_numbers[0].sanitized_number || ""
      : "";

    const locationParts = [p.city, p.state, p.country].filter(Boolean);

    return {
      email: p.email || "",
      firstName: p.first_name || "",
      lastName: p.last_name || "",
      fullName: p.name || "",
      title: p.title || "",
      linkedin: p.linkedin_url || "",
      twitter: p.twitter_url || "",
      facebook: p.facebook_url || "",
      phone: phoneNum,
      location: locationParts.join(", "),
      headline: p.headline || "",
      orgName: p.organization?.name || "",
      orgPhone: p.organization?.phone || "",
      emailStatus: p.email_status || "",
    } as ApolloEnrichResult;
  });
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
    "patreon.com", "meetup.com", "eventbrite.com", "youtube.com", "youtu.be",
    "reddit.com", "facebook.com", "instagram.com", "twitter.com",
    "x.com", "linkedin.com", "tiktok.com", "google.com", "yelp.com",
    "tripadvisor.com", "wikipedia.org", "amazon.com", "substack.com",
    "discord.com", "discord.gg", "github.com", "medium.com",
  ];
  return !skipDomains.some((d) => domain.includes(d));
}
