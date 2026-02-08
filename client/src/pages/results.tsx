import { useState } from "react";
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
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
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
  Award,
  Eye,
  XCircle,
  Users,
  BarChart3,
  ChevronRight,
} from "lucide-react";

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
          <p className="text-xs text-muted-foreground">
            {TYPE_LABELS[lead.communityType || ""] || lead.communityType || "Other"}
          </p>
        </div>
        <Badge variant={lead.status === "qualified" ? "default" : lead.status === "watchlist" ? "secondary" : "outline"}>
          {lead.status}
        </Badge>
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
            <div className="flex items-center gap-2 text-sm">
              <Mail className="w-3.5 h-3.5 text-muted-foreground" />
              <a href={`mailto:${lead.email}`} className="text-primary underline-offset-2 hover:underline">{lead.email}</a>
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
            <p className="text-xs font-medium text-muted-foreground">Owned Channels</p>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(channels).map(([k, v]) => (
                <Badge key={k} variant="secondary" className="text-[10px]">
                  {k === "youtube" && <Youtube className="w-3 h-3 mr-1" />}
                  {k === "newsletter" && <BookOpen className="w-3 h-3 mr-1" />}
                  {k}
                </Badge>
              ))}
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

export default function Results() {
  const [location] = useLocation();
  const params = new URLSearchParams(location.split("?")[1] || "");
  const runId = params.get("runId");

  const [tab, setTab] = useState("qualified");
  const [searchQ, setSearchQ] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [selectedLead, setSelectedLead] = useState<Lead | null>(null);

  const { data: leads, isLoading } = useQuery<Lead[]>({
    queryKey: ["/api/leads", runId],
    queryFn: async () => {
      const url = runId ? `/api/leads?runId=${runId}` : "/api/leads";
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to fetch leads");
      return res.json();
    },
  });

  const filtered = (leads || []).filter((lead) => {
    if (tab === "qualified" && lead.status !== "qualified") return false;
    if (tab === "watchlist" && lead.status !== "watchlist") return false;
    if (typeFilter !== "all" && lead.communityType !== typeFilter) return false;
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
  });

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
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="space-y-1">
            <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-results-title">
              {runId ? `Run #${runId} Results` : "Results"}
            </h1>
            <div className="flex items-center gap-2 flex-wrap">
              <p className="text-sm text-muted-foreground">
                {(leads || []).length} leads found
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
              onChange={(e) => setSearchQ(e.target.value)}
              className="pl-9"
              data-testid="input-search-leads"
            />
          </div>
          <Select value={typeFilter} onValueChange={setTypeFilter}>
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
        </div>

        <Tabs value={tab} onValueChange={setTab}>
          <TabsList data-testid="tabs-status">
            <TabsTrigger value="qualified" className="gap-1">
              <Award className="w-3.5 h-3.5" /> Qualified
            </TabsTrigger>
            <TabsTrigger value="watchlist" className="gap-1">
              <Eye className="w-3.5 h-3.5" /> Watchlist
            </TabsTrigger>
            <TabsTrigger value="all" className="gap-1">
              <BarChart3 className="w-3.5 h-3.5" /> All
            </TabsTrigger>
          </TabsList>

          <TabsContent value={tab} className="mt-4">
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
                  {tab === "all" ? "Run the finder to discover leads." : `No ${tab} leads match your filters.`}
                </p>
              </Card>
            ) : (
              <Card className="overflow-hidden">
                <ScrollArea className="max-h-[600px]">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[200px]">Community / Leader</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Location</TableHead>
                        <TableHead className="text-center">Score</TableHead>
                        <TableHead>Contact</TableHead>
                        <TableHead>Channels</TableHead>
                        <TableHead className="w-8"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filtered.map((lead) => {
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
                                    {lead.email && <Mail className="w-3.5 h-3.5 text-muted-foreground" />}
                                    {lead.phone && <Phone className="w-3.5 h-3.5 text-muted-foreground" />}
                                    {lead.website && <Globe className="w-3.5 h-3.5 text-muted-foreground" />}
                                    {!lead.email && !lead.phone && !lead.website && <span className="text-xs text-muted-foreground">—</span>}
                                  </div>
                                </TableCell>
                                <TableCell>
                                  <div className="flex items-center gap-1">
                                    {channelKeys.slice(0, 3).map((ch) => (
                                      <Badge key={ch} variant="outline" className="text-[10px]">{ch}</Badge>
                                    ))}
                                    {channelKeys.length > 3 && <Badge variant="outline" className="text-[10px]">+{channelKeys.length - 3}</Badge>}
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
                </ScrollArea>
              </Card>
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
