import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { useQuery } from "@tanstack/react-query";
import {
  Search,
  Podcast,
  Mail,
  Link2,
  ShieldCheck,
  BarChart3,
  RefreshCw,
  Database,
  Key,
  DollarSign,
  Layers,
  CheckCircle,
  ArrowRight,
  Globe,
  Users,
} from "lucide-react";
import { SiFacebook, SiPatreon, SiSubstack, SiMeetup } from "react-icons/si";

interface PlatformStat {
  platform: string;
  totalLeads: number;
  withEmail: number;
  validEmails: number;
  validRatePerLead: number;
  costPerValidEmail: number;
  isHistorical: boolean;
}

function YieldBadge({ platform, stats }: { platform: string; stats?: PlatformStat[] }) {
  const stat = stats?.find(s => s.platform === platform);
  if (!stat) return null;
  return (
    <div className="flex items-center gap-2" data-testid={`yield-${platform}`}>
      <CheckCircle className="w-3.5 h-3.5 text-green-500" />
      <span className="text-sm">
        {stat.isHistorical ? (
          <>Email yield rate: <strong>{stat.validRatePerLead}%</strong> of leads end up with a valid email <Badge variant="outline" className="ml-1 text-[10px]">based on {stat.totalLeads} leads</Badge></>
        ) : (
          <>Email yield rate: ~{stat.validRatePerLead}% <Badge variant="secondary" className="ml-1 text-[10px]">estimated — not enough data yet</Badge></>
        )}
      </span>
    </div>
  );
}

function SectionHeading({ icon: Icon, title, id }: { icon: any; title: string; id?: string }) {
  return (
    <div id={id} className="flex items-center gap-2 scroll-mt-20" data-testid={id ? `section-${id}` : undefined}>
      <div className="flex items-center justify-center w-8 h-8 rounded-md bg-primary/10">
        <Icon className="w-4 h-4 text-primary" />
      </div>
      <h2 className="text-lg font-semibold">{title}</h2>
    </div>
  );
}

function ActorBadge({ name, cost }: { name: string; cost?: string }) {
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <Badge variant="secondary" className="font-mono text-xs">{name}</Badge>
      {cost && <Badge variant="outline" className="text-xs">{cost}</Badge>}
    </div>
  );
}

function StepCard({ step, title, description, actor, cost, details }: {
  step: string;
  title: string;
  description: string;
  actor?: string;
  cost?: string;
  details?: string[];
}) {
  return (
    <div className="border rounded-md p-4 space-y-2">
      <div className="flex items-center gap-2 flex-wrap">
        <Badge variant="default" className="text-xs">{step}</Badge>
        <span className="font-medium text-sm">{title}</span>
      </div>
      <p className="text-sm text-muted-foreground">{description}</p>
      {actor && <ActorBadge name={actor} cost={cost} />}
      {details && details.length > 0 && (
        <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
          {details.map((d, i) => <li key={i}>{d}</li>)}
        </ul>
      )}
    </div>
  );
}

export default function Documentation() {
  const { data: platformStats } = useQuery<PlatformStat[]>({
    queryKey: ["/api/stats/platforms"],
  });

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto p-6 space-y-8">
        <div className="space-y-2">
          <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-docs-title">
            Documentation
          </h1>
          <p className="text-muted-foreground text-sm">
            Complete reference for how Trova Community Host Finder discovers, enriches, and scores leads.
          </p>
        </div>

        <nav className="border rounded-md p-4 space-y-1.5">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Table of Contents</p>
          {[
            { label: "How It Works", href: "#overview" },
            { label: "Platform Discovery Sources", href: "#platforms" },
            { label: "Enrichment Chain", href: "#enrichment" },
            { label: "Email Validation & CRM", href: "#validation" },
            { label: "Scoring System", href: "#scoring" },
            { label: "Autonomous Mode", href: "#autonomous" },
            { label: "Apify Actors Reference", href: "#actors" },
            { label: "Environment Variables", href: "#env" },
          ].map((item) => (
            <a key={item.href} href={item.href} data-testid={`link-toc-${item.href.slice(1)}`} className="block text-sm text-primary hover:underline underline-offset-2">
              {item.label}
            </a>
          ))}
        </nav>

        <Separator />

        {/* OVERVIEW */}
        <section className="space-y-4">
          <SectionHeading icon={Layers} title="How It Works" id="overview" />
          <Card>
            <CardContent className="pt-6 space-y-3">
              <p className="text-sm text-muted-foreground">
                The app finds people who lead online or offline communities and determines if they'd be a good fit to host group trips through TrovaTrip. Here's the high-level flow:
              </p>
              <div className="grid gap-2">
                {[
                  { num: "1", text: "Finds community leaders by searching Patreon, Facebook Groups, Apple Podcasts, Substack, Meetup, Mighty Networks, and Google Community Search using your keywords" },
                  { num: "2", text: "Builds a social graph by following linked profiles across YouTube, Instagram, Twitter, personal websites, and link aggregator pages (Linktree, Beacons)" },
                  { num: "3", text: "Finds emails by crawling personal websites, using Apollo.io for professional contact lookup, and falling back to Leads Finder" },
                  { num: "4", text: "Verifies emails through MillionVerifier to confirm which ones are valid and deliverable" },
                  { num: "5", text: "Scores each lead 0-100 across six categories to surface the most promising hosts" },
                  { num: "6", text: "Checks your CRM by cross-referencing leads against HubSpot contacts to flag net new vs. existing" },
                  { num: "7", text: "Exports results to CSV for outreach, with filtering by score, source, email status, and CRM status" },
                ].map((item) => (
                  <div key={item.num} className="flex items-start gap-3">
                    <div className="flex items-center justify-center w-6 h-6 rounded-full bg-primary/10 text-primary text-xs font-semibold flex-shrink-0 mt-0.5">
                      {item.num}
                    </div>
                    <p className="text-sm">{item.text}</p>
                  </div>
                ))}
              </div>
              <p className="text-sm text-muted-foreground">
                Two modes available: <strong>Manual</strong> (you pick platforms and settings) and <strong>Autonomous</strong> (set an email target and budget, the system optimizes automatically).
              </p>
            </CardContent>
          </Card>
        </section>

        <Separator />

        {/* PLATFORM DISCOVERY */}
        <section className="space-y-4">
          <SectionHeading icon={Search} title="Platform Discovery Sources" id="platforms" />

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <SiPatreon className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Patreon</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.03/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="louisdeconinck~patreon-scraper" />
              <p className="text-sm text-muted-foreground">
                Searches Patreon by your keywords to find creator pages. Extracts the creator's name, about text, social links (YouTube, Instagram, Twitter, Facebook, LinkedIn, TikTok, Discord, personal website), Patreon tiers, estimated earnings, and subscriber counts.
              </p>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Special Extraction</p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li>Parses "about" text for real names using patterns like "I'm [Name]", "my name is [Name]", "we are [Name] and [Name]"</li>
                  <li>Extracts obfuscated emails (e.g., "name [at] gmail [dot] com")</li>
                  <li>Discovers bare domains and link aggregator URLs (Linktree, Beacons, bio.link)</li>
                  <li>Parses brand names to extract real names (e.g., "Jenne Sluder Yoga" becomes "Jenne Sluder")</li>
                </ul>
              </div>
              <YieldBadge platform="patreon" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <SiFacebook className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Facebook Groups</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.01/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="apify~google-search-scraper" cost="via Google site: search" />
              <p className="text-sm text-muted-foreground">
                Instead of scraping Facebook directly (which is blocked), the app searches Google for <code className="bg-muted px-1 rounded text-xs">site:facebook.com/groups "[keyword]"</code>. Returns public Facebook Group pages with group names, descriptions, member counts, admin names, and URLs found in descriptions.
              </p>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Google Bridge Step</p>
                <p className="text-sm text-muted-foreground">
                  After finding groups, runs a second Google search for each group to find the group leader/organizer's personal website, LinkedIn profile, or org contact page. Searches patterns like "[group name] [admin name] website" or "[group name] organizer founder leader".
                </p>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Default filter: minimum 100 members</span>
              </div>
              <YieldBadge platform="facebook" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <Podcast className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Podcasts (Apple Podcasts)</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.03/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="benthepythondev/podcast-intelligence-aggregator" cost="$30/1K results, pay-per-use" />
              <p className="text-sm text-muted-foreground">
                Searches Apple Podcasts by your keywords. Returns structured data including the podcast feed URL, website URL, artist name, episode count, genres, and any social links.
              </p>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">RSS Feed Email Extraction</p>
                <p className="text-sm text-muted-foreground">
                  After finding podcasts, fetches each podcast's RSS feed using a Cheerio scraper and parses it for <code className="bg-muted px-1 rounded text-xs">&lt;itunes:email&gt;</code> (the host's email), <code className="bg-muted px-1 rounded text-xs">&lt;itunes:name&gt;</code>, website links, and social URLs from show notes. This step alone has a ~40-60% direct email hit rate.
                </p>
              </div>
              <p className="text-sm text-muted-foreground">
                <strong>Budget note:</strong> Requires at least $3 budget in autonomous mode due to higher per-lead cost.
              </p>
              <YieldBadge platform="podcast" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <SiSubstack className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Substack</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.01/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="apify~google-search-scraper + apify~cheerio-scraper" />
              <p className="text-sm text-muted-foreground">
                Searches Google for <code className="bg-muted px-1 rounded text-xs">site:substack.com "[keyword]"</code> to find Substack publications. Then uses a Cheerio scraper to visit each Substack's about page and extract the author's email, social links, and bio information.
              </p>
              <YieldBadge platform="substack" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <SiMeetup className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Meetup Groups</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.01/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="apify~google-search-scraper + apify~cheerio-scraper" />
              <p className="text-sm text-muted-foreground">
                Searches Google for <code className="bg-muted px-1 rounded text-xs">site:meetup.com "[keyword]"</code> to find Meetup groups. Then scrapes each group page for organizer names, member counts, about text, and social links. Also crawls the <code className="bg-muted px-1 rounded text-xs">/members/?op=leaders</code> page for additional organizer information.
              </p>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Organizer Extraction</p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li>Uses 12+ CSS selectors targeting organizer info, host cards, and group leader elements</li>
                  <li>Text-based fallback patterns: "Organized by", "Hosted by", "Led by", "Founded by"</li>
                  <li>Extracts member counts, about text, and social links from group pages</li>
                  <li>Mailto link extraction from group descriptions</li>
                </ul>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Default filter: minimum 50 members</span>
              </div>
              <YieldBadge platform="meetup" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <Users className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Mighty Networks</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.01/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="apify~google-search-scraper + apify~cheerio-scraper" />
              <p className="text-sm text-muted-foreground">
                Searches Google for <code className="bg-muted px-1 rounded text-xs">site:mightynetworks.com "[keyword]"</code> to find Mighty Networks community pages. Scrapes each community landing page for host names, emails, websites, social links, and community descriptions.
              </p>
              <YieldBadge platform="mighty" stats={platformStats} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
              <Globe className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-base">Google Community Search</CardTitle>
              <Badge variant="outline" className="ml-auto text-xs">~$0.01/lead</Badge>
            </CardHeader>
            <CardContent className="space-y-3">
              <ActorBadge name="apify~google-search-scraper + apify~cheerio-scraper" />
              <p className="text-sm text-muted-foreground">
                Broad Google search for community, club, and group websites across the open web. Uses 4 query templates per keyword (club/group/community, organization/association, leader/founder/organizer, join us/become member) combined with geographic locations.
              </p>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Website Crawling</p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li>Crawls up to 12 pages per discovered site across 23 URL patterns (contact, about, team, board, officers, etc.)</li>
                  <li>Extracts emails from mailto links and page content, prioritizing footer and header sections</li>
                  <li>Accepts domain-matching emails first, then personal email addresses (gmail, yahoo, etc.) as fallback</li>
                  <li>Discovers leader names, social links, and organization descriptions</li>
                  <li>Filters out social media domains (facebook, twitter, instagram, etc.) to focus on org websites</li>
                </ul>
              </div>
              <p className="text-sm text-muted-foreground">
                <strong>Geographic targeting:</strong> Combine keywords with locations (e.g., "running club" + "Denver, CO") for hyper-local community discovery.
              </p>
              <YieldBadge platform="google" stats={platformStats} />
            </CardContent>
          </Card>
        </section>

        <Separator />

        {/* ENRICHMENT CHAIN */}
        <section className="space-y-4">
          <SectionHeading icon={Link2} title="Enrichment Chain" id="enrichment" />
          <p className="text-sm text-muted-foreground">
            After finding leads on the platforms above, the app runs this enrichment chain sequentially to maximize email and contact discovery:
          </p>

          <div className="space-y-3">
            <StepCard
              step="1a"
              title="Link Aggregator Scrape (Pass 1)"
              description="Visits any Linktree, Beacons, or bio.link URLs found in Patreon about text. Extracts emails, LinkedIn profiles, and personal websites from those pages."
              actor="apify~cheerio-scraper"
            />

            <div className="flex justify-center">
              <ArrowRight className="w-4 h-4 text-muted-foreground rotate-90" />
            </div>

            <StepCard
              step="2a"
              title="YouTube About Page Scrape"
              description="Scrapes ALL YouTube channels found in social links (no filtering). Extracts business emails, websites, LinkedIn URLs, and any new link aggregator URLs from the channel's about page."
              actor="apify~cheerio-scraper"
            />

            <StepCard
              step="2b"
              title="Instagram Bio Scrape"
              description="Scrapes Instagram profile bios for emails, website links, Linktree links, real names, and follower counts."
              actor="apify~instagram-profile-scraper"
              cost="$1.60/1K profiles"
            />

            <StepCard
              step="2c"
              title="Twitter/X Bio Scrape"
              description="Scrapes Twitter profiles for emails, website links, Linktree links, real names, locations, and follower counts."
              actor="apidojo~twitter-user-scraper"
              cost="$0.40/1K users"
            />

            <StepCard
              step="2d"
              title="Link Aggregator Scrape (Pass 2)"
              description="Scrapes any NEW aggregator URLs discovered from YouTube, Instagram, or Twitter in the steps above. Catches Linktree links that weren't in the original Patreon data."
              actor="apify~cheerio-scraper"
            />

            <StepCard
              step="2e"
              title="Google Contact Search"
              description="Smart Google search for each lead to find contact info. Uses real names for name-bearing creators and Patreon slugs for pseudonymous creators. Skips LinkedIn search for pseudonyms."
              actor="apify~google-search-scraper"
              details={["Batched 5 at a time", "Uses real names when available for better results"]}
            />

            <StepCard
              step="2f"
              title="Slug Domain Probe"
              description="Tries the Patreon slug as a .com domain (e.g., patreon.com/britchida checks if britchida.com exists). For creators without known websites, this sometimes discovers their personal site."
            />

            <div className="flex justify-center">
              <ArrowRight className="w-4 h-4 text-muted-foreground rotate-90" />
            </div>

            <StepCard
              step="3"
              title="Website Contact Crawl"
              description="Crawls personal websites and slug-probed domains across 23 subpage patterns (contact, about, team, board, officers, organizers, hosts, founders, etc.) to find email addresses. Extracts mailto links and prioritizes footer/header content. Accepts domain-matching emails first, then personal emails (gmail, yahoo, etc.) as fallback."
              actor="apify~cheerio-scraper"
              details={["Crawls up to 12 pages per batch", "8000 char text capture with footer/header prioritization", "Supports 15+ URL glob patterns"]}
            />

            <div className="flex justify-center">
              <ArrowRight className="w-4 h-4 text-muted-foreground rotate-90" />
            </div>

            <StepCard
              step="4"
              title="Lead Creation & Initial Scoring"
              description="Creates lead records in the database from all enriched data. Scores each lead 0-100 across 6 pillars (Niche & Identity, Trust & Leadership, Engagement, Monetization, Owned Channels, Trip Fit)."
            />

            <div className="flex justify-center">
              <ArrowRight className="w-4 h-4 text-muted-foreground rotate-90" />
            </div>

            <StepCard
              step="5"
              title="Apollo.io Enrichment"
              description="Looks up contacts by name + domain + LinkedIn URL. Only runs on leads with a minimum score of 15. Skips pseudonymous creators. Deduplicates across runs so the same lead isn't looked up twice."
              actor="Apollo.io People Match API"
              cost="Free 10K credits/month"
              details={["Can be enabled/disabled per run", "Accepts LinkedIn URLs from social graph for better match rates"]}
            />

            <div className="flex justify-center">
              <ArrowRight className="w-4 h-4 text-muted-foreground rotate-90" />
            </div>

            <StepCard
              step="6"
              title="Leads Finder Fallback"
              description="For leads that still have no email after Apollo, this service looks up verified emails by domain. Batched by domain for efficiency."
              actor="code_crafter~leads-finder"
              cost="$1.50/1K leads"
            />
          </div>
        </section>

        <Separator />

        {/* VALIDATION & CRM */}
        <section className="space-y-4">
          <SectionHeading icon={ShieldCheck} title="Email Validation & CRM Check" id="validation" />

          <div className="space-y-3">
            <StepCard
              step="7"
              title="Email Validation (MillionVerifier)"
              description="Validates every discovered email as valid, invalid, catch-all, or unknown. Critical step — raw email discovery produces many bad addresses. Processes in batches of 10 with rate limiting."
              actor="MillionVerifier Real-Time API"
              cost="~$0.50/1K emails"
              details={[
                "Results: valid, invalid, catch-all, unknown",
                "Requires MILLIONVERIFIER_API_KEY secret",
              ]}
            />

            <StepCard
              step="7a"
              title="HubSpot CRM Check"
              description="Batch checks all valid emails against your HubSpot contacts. Marks each lead as 'existing' (already in CRM) or 'net_new' (not in CRM). Read-only — uses HubSpot Search API, never writes to your CRM."
              actor="HubSpot Search API (Private App)"
              details={[
                "Scope: crm.objects.contacts.read only",
                "Rate-limited with 110ms delay between batches",
                "Chunks emails into groups of 3 per API call",
                "Retries up to 3 times on rate limits or errors",
                "Requires HUBSPOT_ACCESS_TOKEN secret",
              ]}
            />
          </div>
        </section>

        <Separator />

        {/* SCORING */}
        <section className="space-y-4">
          <SectionHeading icon={BarChart3} title="Scoring System" id="scoring" />
          <Card>
            <CardContent className="pt-6 space-y-4">
              <p className="text-sm text-muted-foreground">
                Each lead is scored 0-100 across six pillars. After enrichment, a final scoring pass recalculates with bonus points for contact info and audience size.
              </p>
              <div className="space-y-3">
                {[
                  { name: "Niche & Identity", points: "0-20 pts", desc: "How well does this community match travel-friendly niches? Hiking clubs, yoga groups, photography communities score high. Generic or unclear niches score low." },
                  { name: "Trust & Leadership", points: "0-15 pts", desc: "Is there a visible, identifiable leader? Having a real name, personal website, LinkedIn profile, or clear 'founder/organizer' role boosts this score." },
                  { name: "Engagement", points: "0-20 pts", desc: "How active and engaged is the community? Member counts, follower counts, posting frequency, and social media presence factor in. Larger, more active communities score higher." },
                  { name: "Monetization", points: "0-15 pts", desc: "Is the leader already monetizing? Patreon tiers, paid memberships, course offerings, or existing paid trips signal someone comfortable charging their audience." },
                  { name: "Owned Channels", points: "0-20 pts", desc: "How many direct communication channels does the leader control? Email list, personal website, YouTube, Instagram, podcast, newsletter — each adds points." },
                  { name: "Trip Fit", points: "0-10 pts", desc: "Does the community explicitly mention travel, trips, retreats, or group outings? Direct mentions of travel-related activities get the highest marks." },
                ].map((pillar) => (
                  <div key={pillar.name} className="border rounded-md p-3 space-y-1">
                    <div className="flex items-center justify-between gap-2 flex-wrap">
                      <span className="font-medium text-sm">{pillar.name}</span>
                      <Badge variant="secondary" className="text-xs">{pillar.points}</Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">{pillar.desc}</p>
                  </div>
                ))}
              </div>
              <div className="border-t pt-3 space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Bonuses</p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li><strong>Audience size bonus</strong> — Extra points for large follower/member counts</li>
                  <li><strong>Contact info bonus</strong> — Extra points for having a verified email</li>
                </ul>
              </div>
            </CardContent>
          </Card>
        </section>

        <Separator />

        {/* AUTONOMOUS MODE */}
        <section className="space-y-4">
          <SectionHeading icon={RefreshCw} title="Autonomous Mode" id="autonomous" />
          <Card>
            <CardContent className="pt-6 space-y-3">
              <p className="text-sm text-muted-foreground">
                In autonomous mode, you set two constraints and the system optimizes everything else:
              </p>
              <div className="grid gap-2 sm:grid-cols-2">
                <div className="border rounded-md p-3 space-y-1">
                  <div className="flex items-center gap-2">
                    <Mail className="w-4 h-4 text-primary" />
                    <span className="font-medium text-sm">Email Target</span>
                  </div>
                  <p className="text-sm text-muted-foreground">1-500 valid emails (verified by MillionVerifier, not just raw emails)</p>
                </div>
                <div className="border rounded-md p-3 space-y-1">
                  <div className="flex items-center gap-2">
                    <DollarSign className="w-4 h-4 text-primary" />
                    <span className="font-medium text-sm">Max Budget</span>
                  </div>
                  <p className="text-sm text-muted-foreground">$1-$25 spending cap across all Apify actors</p>
                </div>
              </div>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">How It Works</p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li>Budget engine maps your keywords to platforms and allocates ~65% to discovery, ~35% to enrichment</li>
                  <li>Uses historical valid-email yield rates per platform (requires 5+ leads from past runs for accuracy)</li>
                  <li>Over-allocates discovery to compensate for validation loss (raw emails that turn out invalid)</li>
                  <li>Bidirectional estimation: fill budget to see estimated emails, or fill email target to auto-calculate cost</li>
                  <li>All platforms enabled by default except Podcast (toggle individually as needed)</li>
                  <li>Podcast toggle requires $3+ budget (high yield but expensive at $0.03/lead)</li>
                  <li>Google Community Search included for broad website-based discovery across the open web</li>
                </ul>
              </div>
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Expansion Loop</p>
                <p className="text-sm text-muted-foreground">
                  If valid emails are still below target and budget remains, runs up to 2 additional rounds:
                </p>
                <ul className="text-sm text-muted-foreground space-y-1 pl-4 list-disc">
                  <li>Re-runs platform scrapers with expanded keywords (e.g., "yoga community leader", "yoga group organizer")</li>
                  <li>Creates new leads and runs the full enrichment chain</li>
                  <li>Stops when email target is hit OR budget is exhausted, whichever comes first</li>
                  <li>Only social scraping (Instagram/Twitter/YouTube) is budget-gated; enrichment always runs</li>
                </ul>
              </div>
            </CardContent>
          </Card>
        </section>

        <Separator />

        {/* ACTORS REFERENCE */}
        <section className="space-y-4">
          <SectionHeading icon={Database} title="Apify Actors Reference" id="actors" />
          <Card>
            <CardContent className="pt-6">
              <div className="overflow-x-auto">
                <table className="w-full text-sm" data-testid="table-actors-reference">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2 pr-4 font-medium">Actor</th>
                      <th className="text-left py-2 pr-4 font-medium">Purpose</th>
                      <th className="text-left py-2 font-medium">Cost</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {[
                      { actor: "louisdeconinck~patreon-scraper", purpose: "Patreon creator search", cost: "~$0.03/lead" },
                      { actor: "apify~google-search-scraper", purpose: "Google Search (Facebook, Substack, Meetup, Mighty Networks, Google Community, contact search)", cost: "Variable" },
                      { actor: "apify~cheerio-scraper", purpose: "Website/RSS/aggregator scraping", cost: "Low" },
                      { actor: "benthepythondev/podcast-intelligence-aggregator", purpose: "Apple Podcasts search", cost: "$30/1K results" },
                      { actor: "apify/instagram-profile-scraper", purpose: "Instagram bio scraping", cost: "$1.60/1K" },
                      { actor: "apidojo/twitter-user-scraper", purpose: "Twitter bio scraping", cost: "$0.40/1K" },
                      { actor: "code_crafter~leads-finder", purpose: "Email enrichment fallback", cost: "$1.50/1K" },
                    ].map((row) => (
                      <tr key={row.actor}>
                        <td className="py-2 pr-4"><code className="bg-muted px-1.5 py-0.5 rounded text-xs">{row.actor}</code></td>
                        <td className="py-2 pr-4 text-muted-foreground">{row.purpose}</td>
                        <td className="py-2 text-muted-foreground">{row.cost}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </section>

        <Separator />

        {/* ENV VARS */}
        <section className="space-y-4">
          <SectionHeading icon={Key} title="Environment Variables" id="env" />
          <Card>
            <CardContent className="pt-6">
              <div className="overflow-x-auto">
                <table className="w-full text-sm" data-testid="table-env-vars">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2 pr-4 font-medium">Variable</th>
                      <th className="text-left py-2 pr-4 font-medium">Purpose</th>
                      <th className="text-left py-2 font-medium">Required</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {[
                      { key: "APIFY_TOKEN", purpose: "Authenticates all Apify actor calls", required: "Yes" },
                      { key: "APOLLO_API_KEY", purpose: "Apollo.io contact enrichment", required: "Yes" },
                      { key: "MILLIONVERIFIER_API_KEY", purpose: "Email validation", required: "Optional" },
                      { key: "HUBSPOT_ACCESS_TOKEN", purpose: "HubSpot CRM contact check (read-only)", required: "Optional" },
                      { key: "HUNTER_API_KEY", purpose: "Hunter.io email lookup (legacy)", required: "Optional" },
                      { key: "SITE_PASSWORD", purpose: "Protects the app with a login screen", required: "Yes" },
                      { key: "SESSION_SECRET", purpose: "Session management", required: "Yes" },
                    ].map((row) => (
                      <tr key={row.key}>
                        <td className="py-2 pr-4"><code className="bg-muted px-1.5 py-0.5 rounded text-xs">{row.key}</code></td>
                        <td className="py-2 pr-4 text-muted-foreground">{row.purpose}</td>
                        <td className="py-2">
                          <Badge variant={row.required === "Yes" ? "default" : "secondary"} className="text-xs">
                            {row.required}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </section>

        <div className="h-8" />
      </div>
    </div>
  );
}
