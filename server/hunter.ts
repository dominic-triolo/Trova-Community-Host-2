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
