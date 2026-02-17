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
- **runs** - Pipeline execution tracking (status, progress, logs)
- **source_urls** - Discovered URLs with classification
- **communities** - Extracted community organizations
- **leaders** - Community leader contacts
- **leads** - Flattened, scored, export-ready lead records

## Pipeline Steps (Social Graph Enrichment Chain)
1. **Platform Discovery** - Patreon creator search (social links, about text, tiers, earnings) + bare domain/email extraction from about text
2. **Google Contact Search** - Google search for creators missing website/LinkedIn to find personal sites, social profiles, and emails (max 20 creators/run, batched 5 at a time)
3. **Website Contact Crawl** - Crawl personal websites from social profiles for emails (Cheerio scraper on contact/about pages, max 30 sites/run)
4. **Create & Score** - ICP scoring (0-100) with 6 pillars + audience size bonus + contact info bonus
5. **Apollo.io Enrichment** - Contact lookup by name + domain + LinkedIn URL (toggleable, 50 calls/run, min score 15, deduped across runs via apolloEnrichedAt)
6. **Leads Finder Enrichment** - Apify `code_crafter~leads-finder` actor as fallback for leads still missing email after Apollo (30 leads/run, batched by domain)
7. **Scoring** - Final scoring pass (no qualification threshold; scores only)
8. **Export** - CSV download for all leads with scores (global or per-run)

## Enrichment Methods (User-Toggleable)
Users can enable/disable enrichment methods per run via the "Enrichment Methods" card:
- **Apollo.io** - Contact lookup by name, domain & LinkedIn URL using API credits (on by default)

## Social Graph Approach
The pipeline collects cross-platform profile links (YouTube, Instagram, Twitter, Facebook, LinkedIn, TikTok, Discord, Twitch, Substack, personal website) from Patreon results. These are used to:
- Crawl personal websites for contact emails
- Pass LinkedIn URLs to Apollo for better match rates
- Use website domains for Leads Finder fallback enrichment
- Display linked platforms in results UI with platform-specific icons

## Platform Tabs
The discovery form uses platform-specific tabs. Currently Patreon is active; Facebook Groups and LinkedIn tabs are visible but disabled (coming soon). Each platform tab has its own keyword/filter configuration.

## Apify Actors Used
- `apify~google-search-scraper` - Google Search discovery
- `apify~cheerio-scraper` - Generic website extraction
- `easyapi~meetup-groups-scraper` - Meetup group search (structured data)
- `streamers~youtube-scraper` - YouTube channel/video search (structured data)
- `trudax~reddit-scraper-lite` - Reddit community search (structured data)
- `aitorsm~eventbrite` - Eventbrite event/organizer search (structured data)
- `apify/facebook-groups-scraper` - Facebook public group search (structured data)
- `louisdeconinck~patreon-scraper` - Patreon creator search (social links, about text, tiers, earnings)
- `code_crafter~leads-finder` - Email enrichment fallback ($1.50/1k leads, verified emails by domain)

## API Endpoints
- `POST /api/runs` - Start a new pipeline run
- `GET /api/runs` - List all runs
- `GET /api/runs/:id` - Get run status
- `GET /api/leads` - List all leads (optional `?runId=` filter)
- `GET /api/exports/csv` - Download CSV with all leads, scores, and discovery datetime (optional `?runId=` filter)

## Contact Enrichment (Multi-Pass)
- **Step 1 - Website Crawl**: Cheerio scraper crawls personal websites from social graph for emails (contact/about pages)
- **Step 2 - Apollo.io**: People Match API (free 10k credits/mo) - searches by name + domain + LinkedIn URL (50 calls/run, min score 15)
- **Step 3 - Leads Finder**: Apify actor `code_crafter~leads-finder` ($1.50/1k leads) - fallback for leads still missing email after Apollo (30 leads/run, batched by domain, verified emails)
- All three passes run sequentially after lead creation, before final scoring
- Apollo already accepts linkedinUrl from social graph data for better match rates

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection
- `APIFY_TOKEN` - Apify API token (secret)
- `APOLLO_API_KEY` - Apollo.io API key (secret, primary enrichment)

## Running
- `npm run dev` - Start development server
- `npm run db:push` - Push schema to database
