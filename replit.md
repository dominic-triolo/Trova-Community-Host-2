# Trova Community Host Finder

## Overview
The Trova Community Host Finder is a web application designed to identify, enrich, and score high-potential TrovaTrip Host leads from various community sources. Its primary purpose is to automate and streamline the lead generation process for finding hosts within communities such as churches, run clubs, hiking clubs, social clubs, and alumni groups. The project aims to significantly enhance TrovaTrip's ability to discover new hosts, expand its reach, and optimize its lead qualification process.

## User Preferences
I prefer clear and concise communication. When making changes, please explain the reasoning behind them in a straightforward manner. I value an iterative development approach, where changes are proposed and discussed before implementation, especially for significant architectural decisions. I want to be informed about the progress and any potential roadblocks.

## System Architecture
The application is built with a modern web stack. The frontend utilizes **React, TypeScript, Vite, TailwindCSS, and Shadcn UI** for a responsive and visually appealing user interface. The backend is powered by **Express.js**, providing a robust API layer. **PostgreSQL** with **Drizzle ORM** serves as the primary database for persistent data storage. Data collection relies heavily on the **Apify API**, leveraging various actors for platform-specific scraping, Google Search integration, and Cheerio for web parsing.

Key architectural patterns include a well-defined separation of concerns between frontend and backend, a modular pipeline design for lead discovery and enrichment, and a robust data model to track pipeline executions, source URLs, communities, leaders, and host profiles. The system incorporates an **ICP scoring engine** to evaluate lead potential based on predefined pillars and learned weights.

The lead discovery and enrichment pipeline is a multi-step process involving:
- **Platform Discovery:** Searching platforms like Patreon, Facebook Groups, Meetup, Apple Podcasts, and Substack for potential leads. This includes sophisticated extraction of social links, about text, and contact information.
- **Social Graph Enrichment:** Utilizing discovered cross-platform profile links (YouTube, Instagram, Twitter, LinkedIn) to extract real names, scrape bios for emails/websites, and gather follower counts.
- **Contact Enrichment (Multi-Pass):** A sequential process involving website crawls, Apollo.io API integration for contact lookup, and Leads Finder as a fallback for email discovery.
- **Email Validation:** Integration with MillionVerifier for real-time email validation.
- **HubSpot CRM Check:** Read-only integration with HubSpot to identify existing contacts and prevent duplicate lead creation.
- **Scoring:** Application of the ICP scoring engine to all leads.
- **Autonomous Mode:** An intelligent system that allows users to set a target for valid emails and a maximum budget, dynamically allocating resources across platforms and enrichment methods based on historical performance and estimated yield rates.

The application design emphasizes resilience with features like heartbeats, auto-resume for interrupted runs, watchdog timers, circuit breakers, and retry mechanisms with backoff for external API calls.

## External Dependencies
- **Apify API:** Used extensively for data collection through various actors (`apify~google-search-scraper`, `apify~cheerio-scraper`, `streamers~youtube-scraper`, `louisdeconinck~patreon-scraper`, `apify/instagram-profile-scraper`, `apidojo/twitter-user-scraper`, `benthepythondev/podcast-intelligence-aggregator`, `code_crafter~leads-finder`).
- **PostgreSQL:** Primary database for all application data.
- **Apollo.io API:** For contact enrichment and lookup by name, domain, and LinkedIn URL.
- **MillionVerifier API:** For real-time email validation.
- **HubSpot API:** For read-only CRM contact checks to identify existing leads.