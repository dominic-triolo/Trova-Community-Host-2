# Trova Community Host Finder

## Overview
The Trova Community Host Finder is a web application designed to identify, enrich, and score high-potential TrovaTrip Host leads from various community sources. Its primary purpose is to automate and streamline the lead generation process for finding hosts within communities such as churches, run clubs, hiking clubs, social clubs, and alumni groups. The project aims to significantly enhance TrovaTrip's ability to discover new hosts, expand its reach, and optimize its lead qualification process.

## User Preferences
I prefer clear and concise communication. When making changes, please explain the reasoning behind them in a straightforward manner. I value an iterative development approach, where changes are proposed and discussed before implementation, especially for significant architectural decisions. I want to be informed about the progress and any potential roadblocks.

## System Architecture
The application is built with a modern web stack. The frontend utilizes **React, TypeScript, Vite, TailwindCSS, and Shadcn UI** for a responsive and visually appealing user interface. The backend is powered by **Express.js**, providing a robust API layer. **PostgreSQL** with **Drizzle ORM** serves as the primary database for persistent data storage. Data collection relies heavily on the **Apify API**, leveraging various actors for platform-specific scraping, Google Search integration, and Cheerio for web parsing.

Key architectural patterns include a well-defined separation of concerns between frontend and backend, a modular pipeline design for lead discovery and enrichment, and a robust data model to track pipeline executions, source URLs, communities, leaders, and host profiles. The system incorporates an **ICP scoring engine** to evaluate lead potential based on predefined pillars and learned weights.

The lead discovery and enrichment pipeline is a multi-step process involving:
- **Platform Discovery:** Searching platforms like Patreon, Facebook Groups, Meetup, Apple Podcasts, Substack, Mighty Networks, and Google Community Search for potential leads. This includes sophisticated extraction of social links, about text, and contact information.
- **Social Graph Enrichment:** Utilizing discovered cross-platform profile links (YouTube, Instagram, Twitter, LinkedIn) to extract real names, scrape bios for emails/websites, and gather follower counts.
- **Contact Enrichment (Multi-Pass):** A sequential process involving website crawls (23 subpage patterns, mailto extraction, personal email fallback), Apollo.io API integration for contact lookup, and Leads Finder as a fallback for email discovery.
- **Email Validation:** Integration with MillionVerifier for real-time email validation.
- **HubSpot CRM Check:** Read-only integration with HubSpot to identify existing contacts and prevent duplicate lead creation.
- **Scoring:** Application of the ICP scoring engine to all leads.
- **Autonomous Mode:** An intelligent system that allows users to set a target for valid emails and a maximum budget, dynamically allocating resources across platforms and enrichment methods based on historical performance and estimated yield rates. All platforms enabled by default except Podcast.

The application design emphasizes resilience with features like heartbeats, auto-resume for interrupted runs, watchdog timers, circuit breakers, and retry mechanisms with backoff for external API calls.

## Discovery Platforms
- **Patreon** (~$0.03/lead): Creator search with social link and name extraction
- **Facebook Groups** (~$0.01/lead): Google site: search with admin name parsing from snippets, Google Bridge for contact discovery
- **Apple Podcasts** (~$0.03/lead): RSS feed email extraction, requires $3+ budget in autonomous mode
- **Substack** (~$0.01/lead): Google site: search with about page scraping
- **Meetup** (~$0.01/lead): Google site: search with 12+ CSS selectors for organizer extraction, /members/?op=leaders crawling, text-based fallback patterns
- **Mighty Networks** (~$0.01/lead): Google site: search for community landing pages
- **Google Community Search** (~$0.01/lead): Broad web search with 4 query templates per keyword, crawls 23 subpage patterns per site, geographic targeting support
- **LinkedIn Groups**: Removed (poor results — hard-capped at 1 result per keyword). LinkedIn profile enrichment preserved in social graph.

## Recent Changes (Feb 2026)
- **Google Community Search added**: New discovery source using broad Google searches for community/club/group websites with geographic targeting
- **LinkedIn Groups removed**: Removed as discovery source due to poor results (1 result per keyword cap, 10 groups → 1 valid email yield)
- **Website crawl enhanced**: Expanded to 23 subpages, mailto link extraction, personal email domain fallback (gmail, yahoo, etc.), 12 pages per batch, 8000 char text capture
- **Meetup organizer extraction improved**: 12+ CSS selectors, text fallback patterns, /members/?op=leaders crawling
- **Facebook admin name parsing**: Extracts names from Google search snippets during discovery
- **Autonomous mode defaults**: All platforms enabled except Podcast by default
- **Platform stats**: All 7 platforms (including Google) show yield rates and cost per email in autonomous mode

## External Dependencies
- **Apify API:** Used extensively for data collection through various actors (`apify~google-search-scraper`, `apify~cheerio-scraper`, `streamers~youtube-scraper`, `louisdeconinck~patreon-scraper`, `apify/instagram-profile-scraper`, `apidojo/twitter-user-scraper`, `benthepythondev/podcast-intelligence-aggregator`, `code_crafter~leads-finder`).
- **PostgreSQL:** Primary database for all application data.
- **Apollo.io API:** For contact enrichment and lookup by name, domain, and LinkedIn URL.
- **MillionVerifier API:** For real-time email validation.
- **HubSpot API:** For read-only CRM contact checks to identify existing leads.
