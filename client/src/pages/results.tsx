import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation, Link } from "wouter";
import type { Lead, ScoreBreakdown } from "@shared/schema";

const TYPE_LABELS: Record<string, string> = {
  church: "Churches / Ministries",
  run_club: "Run Clubs",
  hiking: "Hiking / Outdoors",
  social_club: "Social Clubs",
  book_club: "Book Clubs",
  professional: "Professional Orgs",
  alumni: "Alumni Chapters",
  nonprofit: "Nonprofits",
  fitness: "Fitness Studios",
  coworking: "Coworking",
  other: "Other",
};

const SOURCE_LABELS: Record<string, string> = {
  patreon: "Patreon",
  facebook: "Facebook Groups",
  podcast: "Podcast",
  substack: "Substack",
  meetup: "Meetup",
  mighty: "Mighty Networks",
  youtube: "YouTube",
  reddit: "Reddit",
  eventbrite: "Eventbrite",
  google: "Google Search",
};
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Progress } from "@/components/ui/progress";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Search,
  Download,
  ExternalLink,
  Mail,
  Phone,
  Globe,
  Youtube,
  BookOpen,
  Users,
  ChevronRight,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
  Link2,
  ShieldCheck,
  ShieldAlert,
  ShieldQuestion,
} from "lucide-react";
import { SiPatreon, SiInstagram, SiX, SiFacebook, SiLinkedin, SiTiktok, SiDiscord, SiTwitch, SiSubstack, SiMeetup } from "react-icons/si";

const PLATFORM_ICONS: Record<string, { icon: any; label: string; color: string }> = {
  patreon: { icon: SiPatreon, label: "Patreon", color: "text-[#FF424D]" },
  youtube: { icon: Youtube, label: "YouTube", color: "text-[#FF0000]" },
  instagram: { icon: SiInstagram, label: "Instagram", color: "text-[#E4405F]" },
  twitter: { icon: SiX, label: "X / Twitter", color: "text-foreground" },
  facebook: { icon: SiFacebook, label: "Facebook", color: "text-[#1877F2]" },
  linkedin: { icon: SiLinkedin, label: "LinkedIn", color: "text-[#0A66C2]" },
  tiktok: { icon: SiTiktok, label: "TikTok", color: "text-foreground" },
  discord: { icon: SiDiscord, label: "Discord", color: "text-[#5865F2]" },
  twitch: { icon: SiTwitch, label: "Twitch", color: "text-[#9146FF]" },
  substack: { icon: SiSubstack, label: "Substack", color: "text-[#FF6719]" },
  meetup: { icon: SiMeetup, label: "Meetup", color: "text-[#ED1C40]" },
  mighty: { icon: Users, label: "Mighty Networks", color: "text-[#5469D4]" },
  linktree: { icon: Link2, label: "Linktree", color: "text-[#43E55E]" },
  website: { icon: Globe, label: "Website", color: "text-muted-foreground" },
  newsletter: { icon: BookOpen, label: "Newsletter", color: "text-muted-foreground" },
};

function EmailValidationBadge({ validation }: { validation: string }) {
  if (!validation) return null;
  if (validation === "valid") {
    return (
      <Badge variant="outline" className="text-green-600 border-green-300 dark:text-green-400 dark:border-green-700 gap-1" data-testid="badge-email-valid">
        <ShieldCheck className="w-3 h-3" />
        Verified
      </Badge>
    );
  }
  if (validation === "invalid") {
    return (
      <Badge variant="outline" className="text-destructive border-destructive/30 gap-1" data-testid="badge-email-invalid">
        <ShieldAlert className="w-3 h-3" />
        Invalid
      </Badge>
    );
  }
  if (validation === "catch-all") {
    return (
      <Badge variant="outline" className="text-yellow-600 border-yellow-300 dark:text-yellow-400 dark:border-yellow-700 gap-1" data-testid="badge-email-catchall">
        <ShieldQuestion className="w-3 h-3" />
        Catch-all
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-muted-foreground gap-1" data-testid="badge-email-unknown">
      <ShieldQuestion className="w-3 h-3" />
      Unknown
    </Badge>
  );
}

function ScoreBar({ label, value, max }: { label: string; value: number; max: number }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs">{label}</span>
        <span className="text-xs font-medium">{value}/{max}</span>
      </div>
      <Progress value={pct} className="h-1.5" />
    </div>
  );
}

function LeadDetail({ lead }: { lead: Lead }) {
  const sb = lead.scoreBreakdown as ScoreBreakdown | null;
  const channels = lead.ownedChannels as Record<string, string> | null;
  const monetization = lead.monetizationSignals as Record<string, any> | null;
  const engagement = lead.engagementSignals as Record<string, any> | null;

  return (
    <div className="space-y-5">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center justify-center w-10 h-10 rounded-md bg-primary/10">
          <Users className="w-5 h-5 text-primary" />
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold truncate" data-testid="text-detail-name">
            {lead.communityName || lead.leaderName || "Unknown"}
          </p>
          <div className="flex items-center gap-2 flex-wrap">
            <p className="text-xs text-muted-foreground">
              {TYPE_LABELS[lead.communityType || ""] || lead.communityType || "Other"}
            </p>
            {lead.source && (
              <Badge variant="secondary" className="text-[10px]" data-testid="badge-detail-source">
                {SOURCE_LABELS[lead.source] || lead.source}
              </Badge>
            )}
          </div>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <div className="text-2xl font-bold" data-testid="text-detail-score">{lead.score}</div>
        <span className="text-xs text-muted-foreground">/ 100</span>
      </div>

      {sb && (
        <Card className="p-3 space-y-2.5">
          <p className="text-xs font-medium text-muted-foreground">Score Breakdown</p>
          <ScoreBar label="Niche & Identity" value={sb.nicheIdentity} max={20} />
          <ScoreBar label="Trust & Leadership" value={sb.trustLeadership} max={15} />
          <ScoreBar label="Engagement" value={sb.engagement} max={20} />
          <ScoreBar label="Monetization" value={sb.monetization} max={15} />
          <ScoreBar label="Owned Channels" value={sb.ownedChannels} max={20} />
          <ScoreBar label="Trip Fit" value={sb.tripFit} max={10} />
          {sb.penalties < 0 && (
            <div className="flex items-center justify-between text-xs text-destructive">
              <span>Penalties</span>
              <span>{sb.penalties}</span>
            </div>
          )}
        </Card>
      )}

      <Separator />

      <div className="space-y-2">
        <p className="text-xs font-medium text-muted-foreground">Contact Info</p>
        <div className="space-y-1.5">
          {lead.email && (
            <div className="flex items-center gap-2 flex-wrap text-sm">
              <Mail className="w-3.5 h-3.5 text-muted-foreground" />
              <a href={`mailto:${lead.email}`} className="text-primary underline-offset-2 hover:underline">{lead.email}</a>
              <EmailValidationBadge validation={(lead as any).emailValidation || ""} />
              {(lead as any).hubspotStatus === "net_new" && (
                <Badge variant="outline" className="text-[10px] text-blue-600 border-blue-300 dark:text-blue-400 dark:border-blue-700 gap-1" data-testid="badge-hubspot-detail-new">Net New</Badge>
              )}
              {(lead as any).hubspotStatus === "existing" && (
                <Badge variant="outline" className="text-[10px] text-muted-foreground border-muted gap-1" data-testid="badge-hubspot-detail-existing">In CRM</Badge>
              )}
            </div>
          )}
          {lead.phone && (
            <div className="flex items-center gap-2 text-sm">
              <Phone className="w-3.5 h-3.5 text-muted-foreground" />
              <span>{lead.phone}</span>
            </div>
          )}
          {lead.website && (
            <div className="flex items-center gap-2 text-sm">
              <Globe className="w-3.5 h-3.5 text-muted-foreground" />
              <a href={lead.website} target="_blank" rel="noopener noreferrer" className="text-primary underline-offset-2 hover:underline truncate">{lead.website}</a>
            </div>
          )}
          {lead.linkedin && (
            <div className="flex items-center gap-2 text-sm">
              <ExternalLink className="w-3.5 h-3.5 text-muted-foreground" />
              <a href={lead.linkedin} target="_blank" rel="noopener noreferrer" className="text-primary underline-offset-2 hover:underline truncate">LinkedIn</a>
            </div>
          )}
        </div>
      </div>

      {channels && Object.keys(channels).length > 0 && (
        <>
          <Separator />
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <p className="text-xs font-medium text-muted-foreground">Social Graph</p>
              <Badge variant="secondary" className="text-[10px]">
                <Link2 className="w-3 h-3 mr-1" />
                {Object.keys(channels).length} platforms
              </Badge>
            </div>
            <div className="space-y-1.5">
              {Object.entries(channels).map(([k, v]) => {
                const platform = PLATFORM_ICONS[k];
                const PlatformIcon = platform?.icon || Globe;
                const isLink = typeof v === "string" && v.startsWith("http");
                return (
                  <div key={k} className="flex items-center gap-2 text-sm">
                    <PlatformIcon className={`w-3.5 h-3.5 flex-shrink-0 ${platform?.color || "text-muted-foreground"}`} />
                    {isLink ? (
                      <a href={v} target="_blank" rel="noopener noreferrer" className="text-primary underline-offset-2 hover:underline truncate">
                        {platform?.label || k}
                      </a>
                    ) : (
                      <span className="text-muted-foreground">{platform?.label || k}{v && v !== "detected" && v !== "active" ? `: ${v}` : ""}</span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </>
      )}

      {monetization && Object.keys(monetization).length > 0 && (
        <>
          <Separator />
          <div className="space-y-2">
            <p className="text-xs font-medium text-muted-foreground">Monetization Signals</p>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(monetization).map(([k, v]) => (
                <Badge key={k} variant="outline" className="text-[10px]">{k}: {String(v)}</Badge>
              ))}
            </div>
          </div>
        </>
      )}

      {engagement && Object.keys(engagement).length > 0 && (
        <>
          <Separator />
          <div className="space-y-2">
            <p className="text-xs font-medium text-muted-foreground">Engagement Signals</p>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(engagement).map(([k, v]) => (
                <Badge key={k} variant="outline" className="text-[10px]">{k}: {String(v)}</Badge>
              ))}
            </div>
          </div>
        </>
      )}

      {lead.location && (
        <>
          <Separator />
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted-foreground">Location</p>
            <p className="text-sm">{lead.location}</p>
          </div>
        </>
      )}
    </div>
  );
}

const PAGE_SIZE_OPTIONS = [25, 50, 100] as const;

export default function Results() {
  const [location] = useLocation();
  const params = new URLSearchParams(location.split("?")[1] || "");
  const runId = params.get("runId");

  const [searchQ, setSearchQ] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [hubspotFilter, setHubspotFilter] = useState("all");
  const [validEmailFilter, setValidEmailFilter] = useState("all");
  const [emailOnly, setEmailOnly] = useState(false);
  const [selectedLead, setSelectedLead] = useState<Lead | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<number>(50);

  const { data: leads, isLoading } = useQuery<Lead[]>({
    queryKey: ["/api/leads", runId],
    queryFn: async () => {
      const url = runId ? `/api/leads?runId=${runId}` : "/api/leads";
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to fetch leads");
      return res.json();
    },
  });

  const availableSources = Array.from(
    new Set((leads || []).map((l) => l.source || "").filter(Boolean))
  ).sort();

  const filtered = useMemo(() => (leads || []).filter((lead) => {
    if (typeFilter !== "all" && lead.communityType !== typeFilter) return false;
    if (sourceFilter !== "all" && (lead.source || "") !== sourceFilter) return false;
    if (hubspotFilter !== "all") {
      const hs = (lead as any).hubspotStatus || "";
      if (hubspotFilter === "net_new" && hs !== "net_new") return false;
      if (hubspotFilter === "existing" && hs !== "existing") return false;
    }
    if (validEmailFilter !== "all") {
      const ev = (lead as any).emailValidation || "";
      if (validEmailFilter === "valid" && ev !== "valid") return false;
      if (validEmailFilter === "no_valid" && ev === "valid") return false;
    }
    if (emailOnly && !lead.email) return false;
    if (searchQ) {
      const q = searchQ.toLowerCase();
      return (
        (lead.communityName || "").toLowerCase().includes(q) ||
        (lead.leaderName || "").toLowerCase().includes(q) ||
        (lead.email || "").toLowerCase().includes(q) ||
        (lead.location || "").toLowerCase().includes(q)
      );
    }
    return true;
  }), [leads, typeFilter, sourceFilter, hubspotFilter, validEmailFilter, emailOnly, searchQ]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const paginatedLeads = filtered.slice((safePage - 1) * pageSize, safePage * pageSize);

  const handleFilterChange = (setter: (v: any) => void) => (v: any) => {
    setter(v);
    setPage(1);
  };

  const downloadCsv = async () => {
    const exportUrl = runId ? `/api/exports/csv?runId=${runId}` : `/api/exports/csv`;
    const res = await fetch(exportUrl);
    if (!res.ok) return;
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = runId ? `run${runId}_leads.csv` : `all_leads.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex flex-col flex-1 overflow-hidden">
      <div className="flex-shrink-0 p-4 pb-0 space-y-4 max-w-[1400px] w-full mx-auto">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="space-y-1">
            <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-results-title">
              {runId ? `Run #${runId} Results` : "Results"}
            </h1>
            <div className="flex items-center gap-2 flex-wrap">
              <p className="text-sm text-muted-foreground">
                {filtered.length} of {(leads || []).length} leads
                {leads && leads.filter((l) => l.email).length > 0 && (
                  <span className="ml-1">({leads.filter((l) => l.email).length} with email)</span>
                )}
                {leads && leads.filter((l: any) => l.emailValidation === "valid").length > 0 && (
                  <span className="ml-1 text-green-600 dark:text-green-400">
                    ({leads.filter((l: any) => l.emailValidation === "valid").length} valid)
                  </span>
                )}
                {leads && leads.filter((l: any) => l.hubspotStatus === "net_new").length > 0 && (
                  <span className="ml-1 text-blue-600 dark:text-blue-400">
                    ({leads.filter((l: any) => l.hubspotStatus === "net_new").length} net new)
                  </span>
                )}
              </p>
              {runId && (
                <Link href="/results" data-testid="link-show-all-results" className="text-xs text-primary underline-offset-2 hover:underline">
                  Show all runs
                </Link>
              )}
            </div>
          </div>
          <Button variant="outline" size="sm" onClick={downloadCsv} data-testid="button-export-csv">
            <Download className="w-4 h-4 mr-1" /> Export CSV
          </Button>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <div className="relative flex-1 min-w-[200px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input
              placeholder="Search leads..."
              value={searchQ}
              onChange={(e) => { setSearchQ(e.target.value); setPage(1); }}
              className="pl-9"
              data-testid="input-search-leads"
            />
          </div>
          <Select value={sourceFilter} onValueChange={handleFilterChange(setSourceFilter)}>
            <SelectTrigger className="w-[180px]" data-testid="select-source-filter">
              <SelectValue placeholder="Source" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Sources</SelectItem>
              {availableSources.map((src) => (
                <SelectItem key={src} value={src}>{SOURCE_LABELS[src] || src}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          {(leads || []).some((l: any) => l.emailValidation) && (
            <Select value={validEmailFilter} onValueChange={handleFilterChange(setValidEmailFilter)}>
              <SelectTrigger className="w-[180px]" data-testid="select-valid-email-filter">
                <SelectValue placeholder="Valid Email" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Emails</SelectItem>
                <SelectItem value="valid">Has Valid Email</SelectItem>
                <SelectItem value="no_valid">No Valid Email</SelectItem>
              </SelectContent>
            </Select>
          )}
          {(leads || []).some((l: any) => l.hubspotStatus) && (
            <Select value={hubspotFilter} onValueChange={handleFilterChange(setHubspotFilter)}>
              <SelectTrigger className="w-[180px]" data-testid="select-hubspot-filter">
                <SelectValue placeholder="Net New Lead" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Leads</SelectItem>
                <SelectItem value="net_new">Net New Lead</SelectItem>
                <SelectItem value="existing">Existing in CRM</SelectItem>
              </SelectContent>
            </Select>
          )}
          <Select value={typeFilter} onValueChange={handleFilterChange(setTypeFilter)}>
            <SelectTrigger className="w-[180px]" data-testid="select-type-filter">
              <SelectValue placeholder="Community type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              {Object.entries(TYPE_LABELS).map(([value, label]) => (
                <SelectItem key={value} value={value}>{label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <label className="flex items-center gap-2 cursor-pointer text-sm text-muted-foreground whitespace-nowrap">
            <Checkbox
              checked={emailOnly}
              onCheckedChange={handleFilterChange(setEmailOnly)}
              data-testid="checkbox-email-only"
            />
            Has email
          </label>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-4 pt-3">
        <div className="max-w-[1400px] w-full mx-auto">
          {isLoading ? (
            <div className="space-y-2">
              {[1, 2, 3, 4, 5].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <Card className="p-8 text-center space-y-3">
              <Users className="w-10 h-10 text-muted-foreground mx-auto" />
              <p className="text-sm font-medium">No leads found</p>
              <p className="text-xs text-muted-foreground">
                Run the finder to discover leads.
              </p>
            </Card>
          ) : (
            <Card className="overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[200px] sticky top-0 bg-card z-10">Community / Leader</TableHead>
                    <TableHead className="sticky top-0 bg-card z-10">Type</TableHead>
                    <TableHead className="sticky top-0 bg-card z-10">Location</TableHead>
                    <TableHead className="text-center sticky top-0 bg-card z-10">Score</TableHead>
                    <TableHead className="sticky top-0 bg-card z-10">Contact</TableHead>
                    <TableHead className="sticky top-0 bg-card z-10">Platforms</TableHead>
                    <TableHead className="w-8 sticky top-0 bg-card z-10"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {paginatedLeads.map((lead) => {
                    const channels = lead.ownedChannels as Record<string, string> | null;
                    const channelKeys = channels ? Object.keys(channels) : [];
                    return (
                      <Sheet key={lead.id} open={selectedLead?.id === lead.id} onOpenChange={(open) => setSelectedLead(open ? lead : null)}>
                        <SheetTrigger asChild>
                          <TableRow className="cursor-pointer hover-elevate" data-testid={`row-lead-${lead.id}`}>
                            <TableCell>
                              <div className="min-w-0">
                                <p className="text-sm font-medium truncate">{lead.communityName || lead.leaderName || "—"}</p>
                                {lead.leaderName && lead.communityName && (
                                  <p className="text-[11px] text-muted-foreground truncate">{lead.leaderName}</p>
                                )}
                              </div>
                            </TableCell>
                            <TableCell>
                              <Badge variant="secondary" className="text-[10px]">
                                {TYPE_LABELS[lead.communityType || ""] || lead.communityType || "—"}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground truncate max-w-[150px]">
                              {lead.location || "—"}
                            </TableCell>
                            <TableCell className="text-center">
                              <span className={`text-sm font-semibold ${(lead.score || 0) >= 65 ? "text-chart-3" : (lead.score || 0) >= 50 ? "text-chart-4" : "text-muted-foreground"}`}>
                                {lead.score}
                              </span>
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center gap-1.5">
                                {lead.email && (
                                  <Mail className={`w-3.5 h-3.5 ${(lead as any).emailValidation === "valid" ? "text-green-600 dark:text-green-400" : (lead as any).emailValidation === "invalid" ? "text-destructive" : "text-muted-foreground"}`} />
                                )}
                                {(lead as any).hubspotStatus === "net_new" && (
                                  <Badge variant="outline" className="text-[9px] px-1 py-0 text-blue-600 border-blue-300 dark:text-blue-400 dark:border-blue-700" data-testid={`badge-hubspot-new-${lead.id}`}>NEW</Badge>
                                )}
                                {(lead as any).hubspotStatus === "existing" && (
                                  <Badge variant="outline" className="text-[9px] px-1 py-0 text-muted-foreground border-muted" data-testid={`badge-hubspot-existing-${lead.id}`}>CRM</Badge>
                                )}
                                {lead.phone && <Phone className="w-3.5 h-3.5 text-muted-foreground" />}
                                {lead.website && <Globe className="w-3.5 h-3.5 text-muted-foreground" />}
                                {!lead.email && !lead.phone && !lead.website && <span className="text-xs text-muted-foreground">—</span>}
                              </div>
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center gap-1">
                                {channelKeys.slice(0, 5).map((ch) => {
                                  const p = PLATFORM_ICONS[ch];
                                  const Icon = p?.icon || Globe;
                                  return <Icon key={ch} className={`w-3.5 h-3.5 ${p?.color || "text-muted-foreground"}`} title={p?.label || ch} />;
                                })}
                                {channelKeys.length > 5 && <span className="text-[10px] text-muted-foreground">+{channelKeys.length - 5}</span>}
                              </div>
                            </TableCell>
                            <TableCell>
                              <ChevronRight className="w-4 h-4 text-muted-foreground" />
                            </TableCell>
                          </TableRow>
                        </SheetTrigger>
                        <SheetContent className="overflow-y-auto">
                          <SheetHeader>
                            <SheetTitle>Lead Details</SheetTitle>
                          </SheetHeader>
                          <div className="mt-4">
                            <LeadDetail lead={lead} />
                          </div>
                        </SheetContent>
                      </Sheet>
                    );
                  })}
                </TableBody>
              </Table>
            </Card>
          )}
        </div>
      </div>

      {!isLoading && filtered.length > 0 && (
        <div className="flex-shrink-0 border-t bg-background px-4 py-2">
          <div className="max-w-[1400px] w-full mx-auto flex items-center justify-between gap-4 flex-wrap">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <span>Rows per page</span>
              <Select value={String(pageSize)} onValueChange={(v) => { setPageSize(Number(v)); setPage(1); }}>
                <SelectTrigger className="w-[70px]" data-testid="select-page-size">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PAGE_SIZE_OPTIONS.map((s) => (
                    <SelectItem key={s} value={String(s)}>{s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-1">
              <span className="text-sm text-muted-foreground mr-2" data-testid="text-page-info">
                {(safePage - 1) * pageSize + 1}–{Math.min(safePage * pageSize, filtered.length)} of {filtered.length}
              </span>
              <Button
                variant="outline"
                size="icon"
                onClick={() => setPage(1)}
                disabled={safePage <= 1}
                data-testid="button-first-page"
              >
                <ChevronsLeft className="w-4 h-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                onClick={() => setPage(safePage - 1)}
                disabled={safePage <= 1}
                data-testid="button-prev-page"
              >
                <ChevronLeft className="w-4 h-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                onClick={() => setPage(safePage + 1)}
                disabled={safePage >= totalPages}
                data-testid="button-next-page"
              >
                <ChevronRight className="w-4 h-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                onClick={() => setPage(totalPages)}
                disabled={safePage >= totalPages}
                data-testid="button-last-page"
              >
                <ChevronsRight className="w-4 h-4" />
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
