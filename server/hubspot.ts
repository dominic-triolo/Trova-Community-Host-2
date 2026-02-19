const HUBSPOT_API_BASE = "https://api.hubapi.com";

export async function checkEmailsInHubspot(
  emails: string[]
): Promise<Map<string, boolean>> {
  const token = process.env.HUBSPOT_ACCESS_TOKEN;
  if (!token) {
    throw new Error("HUBSPOT_ACCESS_TOKEN not configured");
  }

  const results = new Map<string, boolean>();
  const BATCH_SIZE = 100;

  for (let i = 0; i < emails.length; i += BATCH_SIZE) {
    const batch = emails.slice(i, i + BATCH_SIZE);

    const filterGroups = batch.map((email) => ({
      filters: [
        {
          propertyName: "email",
          operator: "EQ",
          value: email,
        },
      ],
    }));

    const chunks: string[][] = [];
    for (let j = 0; j < batch.length; j += 3) {
      chunks.push(batch.slice(j, j + 3));
    }

    for (let ci = 0; ci < chunks.length; ci++) {
      const chunk = chunks[ci];
      let retries = 0;
      const MAX_RETRIES = 3;
      let success = false;

      while (!success && retries < MAX_RETRIES) {
        try {
          const chunkFilterGroups = chunk.map((email) => ({
            filters: [
              {
                propertyName: "email",
                operator: "EQ",
                value: email,
              },
            ],
          }));

          const response = await fetch(
            `${HUBSPOT_API_BASE}/crm/v3/objects/contacts/search`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${token}`,
              },
              body: JSON.stringify({
                filterGroups: chunkFilterGroups,
                properties: ["email"],
                limit: chunk.length,
              }),
            }
          );

          if (response.status === 429) {
            retries++;
            const retryAfter = parseInt(
              response.headers.get("retry-after") || "2"
            );
            await new Promise((r) => setTimeout(r, retryAfter * 1000));
            continue;
          }

          if (!response.ok) {
            const errText = await response.text();
            console.error(
              `HubSpot search error (${response.status}): ${errText}`
            );
            for (const email of chunk) {
              results.set(email, false);
            }
            success = true;
            break;
          }

          const data = await response.json();
          const foundEmails = new Set(
            (data.results || []).map((r: any) =>
              (r.properties?.email || "").toLowerCase()
            )
          );

          for (const email of chunk) {
            results.set(email, foundEmails.has(email.toLowerCase()));
          }
          success = true;
        } catch (err: any) {
          console.error(`HubSpot batch check error: ${err.message}`);
          retries++;
          if (retries >= MAX_RETRIES) {
            for (const email of chunk) {
              results.set(email, false);
            }
          } else {
            await new Promise((r) => setTimeout(r, 1000));
          }
        }
      }

      await new Promise((r) => setTimeout(r, 110));
    }
  }

  return results;
}

export function isHubspotConfigured(): boolean {
  return !!process.env.HUBSPOT_ACCESS_TOKEN;
}
