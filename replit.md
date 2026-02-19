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
  scoring.ts - ICP scoring engine (6 pillars + penalties)

shared/
  schema.ts - Drizzle schema, types, constants
```

## Data Model
- **runs** - Pipeline execution tracking (status, progress, logs, apifySpendUsd)
- **source_urls** - Discovered URLs with classification
- **communities** - Extracted community organizations
- **leaders** - Community leader contacts
- **leads** - Flattened, scored, export-ready lead records

## Pipeline Steps (Social Graph Enrichment Chain)
1. **Platform Discovery** - Patreon creator search (social links, about text, tiers, earnings) + Facebook Groups search (group name, description, member count, admin name, URLs from descriptions) + Apple Podcasts search via `benthepythondev/podcast-intelligence-aggregator` ($30/1K results, pay-per-use) extracting feedUrl, websiteUrl, artistName, episodeCount, genres, social links + bare domain/email extraction from about text + obfuscated email extraction ("[at]", "(at)" patterns) + real name extraction from about text (regex patterns for "I'm X", "my name is X", etc.) + brand name parsing ("Jenne Sluder Yoga" → "Jenne Sluder") + link aggregator URL extraction (Linktree, Beacons, etc.)
1a. **Google Bridge for Facebook Groups** - Smart Google search for each Facebook group to find leader/organizer websites, LinkedIn profiles, and org contact pages. Searches by group name + admin name (if available) or group name + "organizer/founder/leader". Extracts websites, LinkedIn, Instagram, Twitter from results.
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
5. **Apollo.io Enrichment** - Contact lookup by name + domain + LinkedIn URL (toggleable, uncapped, min score 15, deduped across runs via apolloEnrichedAt, skips pseudonyms via isValidApolloCandidate)
6. **Leads Finder Enrichment** - Apify `code_crafter~leads-finder` actor as fallback for leads still missing email after Apollo (uncapped, batched by domain)
7. **Scoring** - Final scoring pass (no qualification threshold; scores only)
8. **Export** - CSV download for all leads with scores (global or per-run)

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
The discovery form uses platform-specific tabs. Patreon, Facebook Groups, Podcasters, and Substack are active; LinkedIn tab is visible but disabled (coming soon). Each platform tab has its own keyword/filter configuration. Facebook Groups tab includes a Google Bridge enrichment step that searches Google for group leader/organizer websites, LinkedIn profiles, and org contact pages. Facebook tab defaults to min 100 members filter. Substack tab uses Google Search (`site:substack.com`) + Cheerio about-page scraping for email/social extraction.

## Apify Actors Used
- `apify~google-search-scraper` - Google Search discovery
- `apify~cheerio-scraper` - Generic website extraction
- `easyapi~meetup-groups-scraper` - Meetup group search (structured data)
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
- Users provide keywords + dollar budget ($1-$20) OR email target (1-500), system auto-selects platforms and optimizes enrichment
- Bidirectional estimation: fill budget → estimate emails, fill email target → auto-calculate cost
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

## Running
- `npm run dev` - Start development server
- `npm run db:push` - Push schema to database
