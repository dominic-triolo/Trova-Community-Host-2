# Trova Community Host Finder

## Overview
A web application that discovers, enriches, and scores high-potential TrovaTrip Host leads from community sources. Uses Apify Store actors for data collection (excludes Instagram). Supports communities like churches, run clubs, hiking clubs, social clubs, alumni groups, etc.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js
- **Database**: PostgreSQL with Drizzle ORM
- **Data Collection**: Apify API (Google Search Scraper, Web Scraper)
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
1. **Discover** - Google Search via Apify to find community URLs
2. **Classify** - Route URLs to connectors (meetup/eventbrite/website/youtube/etc.)
3. **Extract** - Crawl and extract data using Apify web-scraper
4. **Enrich** - Extract emails, phones, owned channels from page content
5. **Score** - ICP scoring (0-100) with 6 pillars
6. **Export** - CSV download for qualified/watchlist leads

## API Endpoints
- `POST /api/runs` - Start a new pipeline run
- `GET /api/runs` - List all runs
- `GET /api/runs/:id` - Get run status
- `GET /api/leads` - List all leads
- `GET /api/exports/:type` - Download CSV (qualified/watchlist)

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection
- `APIFY_TOKEN` - Apify API token (secret)

## Running
- `npm run dev` - Start development server
- `npm run db:push` - Push schema to database
