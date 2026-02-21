# Trova Community Host Finder

## Overview
A web application that discovers, enriches, and scores high-potential TrovaTrip Host leads from community sources. Uses Apify Store actors for data collection (excludes Instagram). Supports communities like churches, run clubs, hiking clubs, social clubs, alumni groups, etc.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js
- **Database**: PostgreSQL with Drizzle ORM
- **Data Collection**: Apify API (platform-specific scrapers + Google Search + Cheerio)
- **Routing**: wouter (frontend), Express (backend)
- **State**: TanStack React Query

## Project Structure
```
client/src/
  App.tsx - Main app with sidebar layout and routing
  pages/
    home.tsx - Discovery configuration form (Run Finder)
    run-status.tsx - Live pipeline progress tracking
    hubspot-learning.tsx - HubSpot Learning insights dashboard
    runs-list.tsx - List of all pipeline runs
    results.tsx - Results table with filters and lead details drawer
  components/
    app-sidebar.tsx - Navigation sidebar
    theme-toggle.tsx - Dark/light mode toggle
    ui/ - Shadcn component library

server/
  index.ts - Express server setup
  routes.ts - API endpoints
  storage.ts - Database access layer (IStorage interface)
  db.ts - Database connection
  apify.ts - Apify API helper (start/wait/fetch)
  pipeline.ts - Full pipeline runner (discover/classify/extract/enrich/score)
  scoring.ts - ICP scoring engine (6 pillars + penalties, learned weights view-only)
  hubspot-sync.ts - HubSpot deal sync + trait analysis + scoring weight computation

shared/
  schema.ts - Drizzle schema, types, constants
```

## Data Model
- **runs** - Pipeline execution tracking (status, progress, logs, apifySpendUsd)
- **source_urls** - Discovered URLs with classification
- **communities** - Extracted community organizations
- **leaders** - Community leader contacts
- **host_profiles** - HubSpot-synced Host profiles with trip counts and trait data
- **scoring_weights** - Learned scoring weights computed from top Host analysis
- **leads** - Flattened, scored, export-ready lead records

## Pipeline Steps (Social Graph Enrichment Chain)
1. **Platform Discovery** - Patreon creator search (social links, about text, tiers, earnings) + Facebook Groups search (group name, description, member count, admin name, URLs from descriptions) + Meetup Groups search (via Google `site:meetup.com` + Cheerio group page scraping for organizer name, member count, emails, social links) + Apple Podcasts search via `benthepythondev/podcast-intelligence-aggregator` ($30/1K results, pay-per-use) extracting feedUrl, websiteUrl, artistName, episodeCount, genres, social links + bare domain/email extraction from about text + obfuscated email extraction ("[at]", "(at)" patterns) + real name extraction from about text (regex patterns for "I'm X", "my name is X", etc.) + brand name parsing ("Jenne Sluder Yoga" → "Jenne Sluder") + link aggregator URL extraction (Linktree, Beacons, etc.)
1a. **Google Bridge for Facebook/Meetup Groups** - Smart Google search for each Facebook or Meetup group to find leader/organizer websites, LinkedIn profiles, and org contact pages. Searches by group name + admin name (if available) or group name + "organizer/founder/leader". Extracts websites, LinkedIn, Instagram, Twitter from results.
1b. **RSS Feed Email Extraction (Podcasts)** - Cheerio scraper parses podcast RSS feeds to extract `<itunes:email>` (host email), `<itunes:name>`, website links, and social URLs from show notes. Expected 40-60% direct email hit rate. Runs after platform discovery, before link aggregator scrape.
1c. **Link Aggregator Scrape (Pass 1)** - Cheerio scraper on Linktree/Beacons/bio.link pages from Patreon about text (uncapped)
2a. **YouTube About Page Scrape** - Scrape ALL YouTube channels (no email/website filter) for business emails, websites, LinkedIn, and link aggregator URLs (uncapped)
2b. **Instagram Bio Scrape** - Scrape Instagram profiles (`apify~instagram-profile-scraper`, $1.60/1K) for emails, websites, Linktree links, real names, and follower counts (uncapped)
2c. **Twitter/X Bio Scrape** - Scrape Twitter profiles (`apidojo~twitter-user-scraper`, $0.40/1K) for emails, website links, Linktree links, real names, locations, and follower counts (uncapped)
2d. **Link Aggregator Scrape (Pass 2)** - Scrape any NEW aggregator URLs discovered via YouTube/Instagram/Twitter
2e. **Google Contact Search** - Smart Google search: uses real names for name-bearing creators, Patreon slug for pseudonymous creators, skips LinkedIn search for pseudonyms. Batched 5 at a time (uncapped).
2f. **Slug Domain Probe** - Try Patreon slug as .com domain (e.g., patreon.com/britchida → britchida.com) for creators without websites
3. **Website Contact Crawl** - Crawl personal websites + slug-probed domains for emails (Cheerio scraper on contact/about pages, uncapped, email-domain validation)
4. **Create & Score** - ICP scoring (0-100) with 6 pillars + audience size bonus + contact info bonus
5. **Apollo.io Enrichment** - Contact lookup by name + domain + LinkedIn URL (toggleable, uncapped, no score minimum, deduped across runs via apolloEnrichedAt, skips pseudonyms via isValidApolloCandidate)
6. **Leads Finder Enrichment** - Apify `code_crafter~leads-finder` actor as fallback for leads still missing email after Apollo (uncapped, batched by domain)
7. **Email Validation** - MillionVerifier validates all discovered emails as valid/invalid/catch-all/unknown
7a. **HubSpot CRM Check** - Read-only contact search via Private App token (crm.objects.contacts.read scope). Batch checks valid emails against HubSpot contacts, marks each lead as "existing" or "net_new". Runs after email validation. Results shown in UI (filter + badges) and CSV export. Uses Search API (`POST /crm/v3/objects/contacts/search`), rate-limited with 110ms delay between batches.
8. **Scoring** - Final scoring pass (no qualification threshold; scores only)
9. **Expansion Loop** (autonomous mode only) - If valid emails < target and budget remains, up to 2 rounds of deeper discovery: re-runs platform scrapers with expanded keywords (e.g. "yoga community leader", "yoga group organizer"), creates new leads, enriches via batched Leads Finder (correct API: company_domain + email_status + fetch_count), validates via MillionVerifier. Budget-gated via isBudgetExhausted at each step. Exits when target reached, budget exhausted, or lead pool exhausted.
10. **Export** - CSV download for all leads with scores (global or per-run)

## Enrichment Methods (User-Toggleable)
Users can enable/disable enrichment methods per run via the "Enrichment Methods" card:
- **Apollo.io** - Contact lookup by name, domain & LinkedIn URL using API credits (on by default)

## Social Graph Approach
The pipeline collects cross-platform profile links (YouTube, Instagram, Twitter, Facebook, LinkedIn, TikTok, Discord, Twitch, Substack, personal website) from Patreon results. These are used to:
- Extract real names from Patreon "about" text using regex patterns ("I'm X", "my name is X", "we are X and Y", etc.)
- Scrape Linktree/Beacons/bio.link aggregator pages for emails, LinkedIn, and personal websites
- Scrape Instagram bios for emails, external URLs, Linktree links, real names, and follower counts
- Scrape Twitter/X bios for emails, website links, Linktree links, real names, locations, and follower counts
- Crawl personal websites for contact emails
- Pass LinkedIn URLs to Apollo for better match rates
- Use real names (when available) instead of brand names for Google/Apollo searches
- Use website domains for Leads Finder fallback enrichment
- Display linked platforms in results UI with platform-specific icons

## Platform Tabs
The discovery form uses platform-specific tabs. Patreon, Facebook Groups, Podcasters, Substack, and Meetup are active; LinkedIn tab is visible but disabled (coming soon). Each platform tab has its own keyword/filter configuration. Facebook Groups tab includes a Google Bridge enrichment step that searches Google for group leader/organizer websites, LinkedIn profiles, and org contact pages. Facebook tab defaults to min 100 members filter. Meetup tab uses Google Search (`site:meetup.com`) + Cheerio group page scraping for organizer name, member count, emails, and social links. Meetup tab defaults to min 50 members filter. Substack tab uses Google Search (`site:substack.com`) + Cheerio about-page scraping for email/social extraction.

## Apify Actors Used
- `apify~google-search-scraper` - Google Search discovery
- `apify~cheerio-scraper` - Generic website extraction
- `apify~google-search-scraper` + `apify~cheerio-scraper` - Meetup group discovery (Google Search `site:meetup.com` + Cheerio page scraping, no subscription)
- `streamers~youtube-scraper` - YouTube channel/video search (structured data)
- `trudax~reddit-scraper-lite` - Reddit community search (structured data)
- `aitorsm~eventbrite` - Eventbrite event/organizer search (structured data)
- `apify~google-search-scraper` - Also used for Facebook group discovery via `site:facebook.com/groups` queries (no dedicated Facebook scraper needed)
- `louisdeconinck~patreon-scraper` - Patreon creator search (social links, about text, tiers, earnings)
- `apify/instagram-profile-scraper` - Instagram profile bio/email/website scraping ($1.60/1K profiles)
- `apidojo/twitter-user-scraper` - Twitter/X profile bio/website/location scraping ($0.40/1K users)
- `benthepythondev/podcast-intelligence-aggregator` - Apple Podcasts search ($30/1K results, pay-per-use, structured podcast data)
- `code_crafter~leads-finder` - Email enrichment fallback ($1.50/1k leads, verified emails by domain)

## Autonomous Mode
- Users provide keywords + email target (1-500) as primary goal + max budget ($1-$25) as spending cap
- Email target = valid emails (verified by MillionVerifier), not raw emails
- Historical platform stats: queries actual valid-email yield rates per platform across completed runs (min 5 leads), falls back to hardcoded defaults
- Budget engine over-allocates discovery to compensate for validation loss (raw → valid conversion)
- Runs table tracks `leadsWithValidEmail` separately from `leadsWithEmail` (raw)
- Bidirectional estimation: fill budget → estimate valid emails, fill email target → auto-calculate cost
- Podcast toggle: on/off switch, requires $3+ budget to include (high yield 55% but expensive $0.03/lead)
- Budget engine (`server/budget-engine.ts`) maps keywords to platforms, allocates budget 65/35 discovery/enrichment
- Platform cost estimates: Patreon $0.03, Facebook $0.01, Podcast $0.03, Substack $0.01 per lead
- Email yield rates: Podcast 55%, Substack 40%, Patreon 35%, Facebook 15%
- Dual stopping condition: discovery stops when either email target OR budget is exhausted, whichever first
- Only social scraping (Instagram/Twitter/YouTube actors) is budget-gated; enrichment (website crawl, Google search, Leads Finder) always runs
- Schema fields: `isAutonomous`, `budgetUsd`, `budgetAllocation`, `emailTarget` on runs table
- Frontend: Mode toggle (Autonomous/Manual) on home page, budget + email target display on run status page
- Preview endpoint: `POST /api/runs/autonomous/preview` returns estimated allocation for given budget/emailTarget/podcastEnabled

## API Endpoints
- `POST /api/runs` - Start a new manual pipeline run
- `POST /api/runs/autonomous` - Start autonomous run (keywords + budgetUsd)
- `GET /api/runs` - List all runs
- `GET /api/runs/:id` - Get run status
- `GET /api/leads` - List all leads (optional `?runId=` filter)
- `GET /api/exports/csv` - Download CSV with all leads, scores, and discovery datetime (optional `?runId=` filter)

## Contact Enrichment (Multi-Pass)
- **Step 1 - Website Crawl**: Cheerio scraper crawls personal websites from social graph for emails (contact/about pages)
- **Step 2 - Apollo.io**: People Match API (free 10k credits/mo) - searches by name + domain + LinkedIn URL (uncapped, min score 15)
- **Step 3 - Leads Finder**: Apify actor `code_crafter~leads-finder` ($1.50/1k leads) - fallback for leads still missing email after Apollo (uncapped, batched by domain, verified emails)
- **Step 4 - Email Validation**: MillionVerifier real-time API (~$0.50/1K emails) - validates all discovered emails as valid/invalid/catch-all/unknown (optional, requires MILLIONVERIFIER_API_KEY)
- All passes run sequentially after lead creation, before final scoring
- Apollo already accepts linkedinUrl from social graph data for better match rates

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection
- `APIFY_TOKEN` - Apify API token (secret)
- `APOLLO_API_KEY` - Apollo.io API key (secret, primary enrichment)
- `MILLIONVERIFIER_API_KEY` - MillionVerifier API key (secret, optional, email validation)
- `HUBSPOT_ACCESS_TOKEN` - HubSpot Private App token (secret, optional, CRM contact check)

## Pipeline Resilience
- **Heartbeat**: Runs update `lastHeartbeat` column every 60s while active
- **Auto-resume on startup**: Server detects runs stuck in 'running' status on boot, auto-resumes from checkpoint (5s delay)
- **Watchdog timer**: Every 3 min checks for runs with stale heartbeat (5+ min old), auto-resumes from checkpoint
- **Race guard**: `resumingRunIds` set prevents duplicate resume attempts from watchdog + startup overlap
- **Circuit breaker**: Google Bridge & Google enrichment batch loops skip remaining batches after 5 consecutive failures
- **Retry with backoff**: Apify actor starts retry up to 3 times (30s → 60s → 90s timeout), also retries on 429/5xx and transient network errors
- **Graceful shutdown**: SIGTERM/SIGINT marks active runs as 'interrupted' with checkpoint preserved for resume

## Running
- `npm run dev` - Start development server
- `npm run db:push` - Push schema to database
