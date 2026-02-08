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

## Pipeline Steps
1. **Platform Discovery** - Run user-selected Apify scrapers in parallel:
   - Meetup Groups (easyapi~meetup-groups-scraper) - member counts, descriptions, locations
   - YouTube Channels (streamers~youtube-scraper) - subscribers, monetization, social links
   - Reddit Communities (trudax~reddit-scraper-lite) - member counts, descriptions
   - Eventbrite Events (aitorsm~eventbrite) - organizer data, followers, venues
   - Facebook Groups (apify/facebook-groups-scraper) - member counts, public groups
2. **Google Search** - Discover generic website URLs via Google Search Scraper (optional)
3. **Extract** - Crawl generic websites with Cheerio Scraper (follows contact/about/team subpages to find organizer info)
4. **Score** - ICP scoring (0-100) with 6 pillars + audience size bonus + contact info bonus
5. **Export** - CSV download for qualified/watchlist leads (global or per-run)

## Source Selection
Users can toggle which platforms to search per run via the "Data Sources" card on the discovery form. Available sources: Meetup, YouTube, Reddit, Eventbrite, Facebook Groups, Google Search + Websites. The `enabledSources` array is stored in RunParams.

## Apify Actors Used
- `apify~google-search-scraper` - Google Search discovery
- `apify~cheerio-scraper` - Generic website extraction
- `easyapi~meetup-groups-scraper` - Meetup group search (structured data)
- `streamers~youtube-scraper` - YouTube channel/video search (structured data)
- `trudax~reddit-scraper-lite` - Reddit community search (structured data)
- `aitorsm~eventbrite` - Eventbrite event/organizer search (structured data)
- `apify/facebook-groups-scraper` - Facebook public group search (structured data)

## API Endpoints
- `POST /api/runs` - Start a new pipeline run
- `GET /api/runs` - List all runs
- `GET /api/runs/:id` - Get run status
- `GET /api/leads` - List all leads (optional `?runId=` filter)
- `GET /api/exports/:type` - Download CSV (qualified/watchlist, optional `?runId=` filter)

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection
- `APIFY_TOKEN` - Apify API token (secret)

## Running
- `npm run dev` - Start development server
- `npm run db:push` - Push schema to database
