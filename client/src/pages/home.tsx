import { useState, useMemo, useEffect, useCallback } from "react";
import { useLocation } from "wouter";
import { useMutation, useQuery } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  RECOMMENDED_KEYWORDS,
  FB_RECOMMENDED_KEYWORDS,
  PODCAST_RECOMMENDED_KEYWORDS,
  SUBSTACK_RECOMMENDED_KEYWORDS,
  MEETUP_RECOMMENDED_KEYWORDS,
  DEFAULT_RUN_PARAMS,
  AVAILABLE_ENRICHMENTS,
  type RunParams,
  type Run,
  type BudgetAllocation,
} from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  Rocket,
  Search,
  MapPin,
  Settings2,
  Loader2,
  ArrowRight,
  X,
  Plus,
  Filter,
  Mail,
  Lock,
  Lightbulb,
  Zap,
  DollarSign,
  Wrench,
  Target,
  Mic,
  Globe,
  Users,
} from "lucide-react";
import { Switch } from "@/components/ui/switch";
import { SiPatreon, SiFacebook, SiLinkedin, SiApplepodcasts, SiSubstack, SiMeetup } from "react-icons/si";

function UsedKeywordsSuggestions({
  usedKeywords,
  currentKeywords,
  onAddGroup,
}: {
  usedKeywords: string[];
  currentKeywords: string[];
  onAddGroup: (keywords: readonly string[]) => void;
}) {
  const suggestions = useMemo(() => {
    if (usedKeywords.length === 0) return [];

    const unused = RECOMMENDED_KEYWORDS.filter((rec) => {
      const anyUsed = rec.keywords.some((kw) => usedKeywords.includes(kw) || currentKeywords.includes(kw));
      return !anyUsed;
    });

    return unused.slice(0, 5);
  }, [usedKeywords, currentKeywords]);

  if (suggestions.length === 0) return null;

  return (
    <div className="space-y-2 p-3 rounded-md bg-muted/50">
      <div className="flex items-center gap-1.5">
        <Lightbulb className="w-3.5 h-3.5 text-muted-foreground" />
        <p className="text-xs font-medium text-muted-foreground">Try these next</p>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {suggestions.map((rec) => (
          <Badge
            key={rec.label}
            variant="outline"
            className="cursor-pointer select-none"
            onClick={() => onAddGroup(rec.keywords)}
            data-testid={`badge-suggest-${rec.label.replace(/\s+/g, "-").slice(0, 30)}`}
          >
            <Plus className="w-3 h-3 mr-0.5" />
            {rec.label}
          </Badge>
        ))}
      </div>
    </div>
  );
}

function ComingSoonTab({ platform, icon: Icon }: { platform: string; icon: any }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 space-y-4">
      <div className="flex items-center justify-center w-16 h-16 rounded-md bg-muted">
        <Icon className="w-8 h-8 text-muted-foreground" />
      </div>
      <div className="text-center space-y-1.5">
        <div className="flex items-center justify-center gap-1.5">
          <Lock className="w-4 h-4 text-muted-foreground" />
          <p className="text-sm font-medium text-muted-foreground">Coming Soon</p>
        </div>
        <p className="text-xs text-muted-foreground max-w-xs">
          {platform} discovery is under development. Stay tuned for updates.
        </p>
      </div>
    </div>
  );
}

export default function Home() {
  const [, navigate] = useLocation();
  const { toast } = useToast();

  const [mode, setMode] = useState<"manual" | "autonomous">("manual");
  const [autoBudget, setAutoBudget] = useState<number | "">(10);
  const [autoEmailTarget, setAutoEmailTarget] = useState<number | "">(50);
  const [autoEnabledPlatforms, setAutoEnabledPlatforms] = useState<string[]>(["patreon", "facebook", "podcast", "substack", "meetup"]);
  const autoPodcastEnabled = autoEnabledPlatforms.includes("podcast");
  const [autoKeywords, setAutoKeywords] = useState<string[]>([]);
  const [autoCustomKeyword, setAutoCustomKeyword] = useState("");
  const [previewAllocation, setPreviewAllocation] = useState<BudgetAllocation | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const [params, setParams] = useState<RunParams>({
    ...DEFAULT_RUN_PARAMS,
    enabledSources: ["patreon"],
  });
  const [customKeyword, setCustomKeyword] = useState("");
  const [platformTab, setPlatformTab] = useState("patreon");

  const { data: runs } = useQuery<Run[]>({
    queryKey: ["/api/runs"],
  });

  const usedKeywords = useMemo(() => {
    if (!runs) return [];
    const all = new Set<string>();
    for (const run of runs) {
      const p = run.params as RunParams | null;
      if (p?.seedKeywords) {
        for (const kw of p.seedKeywords) all.add(kw);
      }
    }
    return Array.from(all);
  }, [runs]);

  interface PlatformStat {
    platform: string;
    totalLeads: number;
    withEmail: number;
    validEmails: number;
    validRatePerLead: number;
    costPerValidEmail: number;
    isHistorical: boolean;
  }

  const { data: platformStats } = useQuery<PlatformStat[]>({
    queryKey: ["/api/stats/platforms"],
  });

  const runMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/runs", params);
      return res.json();
    },
    onSuccess: (data: { id: number }) => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs"] });
      toast({ title: "Pipeline started", description: "Navigating to run status..." });
      navigate(`/runs/${data.id}`);
    },
    onError: (err: Error) => {
      toast({ title: "Failed to start", description: err.message, variant: "destructive" });
    },
  });

  const fetchPreview = useCallback(async () => {
    if (autoKeywords.length === 0) {
      setPreviewAllocation(null);
      return;
    }
    setPreviewLoading(true);
    try {
      const body: any = { keywords: autoKeywords, podcastEnabled: autoPodcastEnabled, enabledPlatforms: autoEnabledPlatforms };
      if (autoEmailTarget && Number(autoEmailTarget) > 0) {
        body.emailTarget = Number(autoEmailTarget);
      } else if (autoBudget && Number(autoBudget) > 0) {
        body.budgetUsd = Number(autoBudget);
      }
      const res = await apiRequest("POST", "/api/runs/autonomous/preview", body);
      const data = await res.json();
      setPreviewAllocation(data.allocation);
    } catch {
      setPreviewAllocation(null);
    }
    setPreviewLoading(false);
  }, [autoKeywords, autoBudget, autoEmailTarget, autoPodcastEnabled, autoEnabledPlatforms]);

  useEffect(() => {
    const timer = setTimeout(fetchPreview, 400);
    return () => clearTimeout(timer);
  }, [fetchPreview]);

  const autoRunMutation = useMutation({
    mutationFn: async () => {
      const body: any = { keywords: autoKeywords, podcastEnabled: autoPodcastEnabled, enabledPlatforms: autoEnabledPlatforms };
      if (autoEmailTarget && Number(autoEmailTarget) > 0) {
        body.emailTarget = Number(autoEmailTarget);
      }
      if (autoBudget && Number(autoBudget) > 0) {
        body.budgetUsd = Number(autoBudget);
      }
      const res = await apiRequest("POST", "/api/runs/autonomous", body);
      return res.json();
    },
    onSuccess: (data: { id: number }) => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs"] });
      const desc = autoEmailTarget ? `Target: ${autoEmailTarget} emails` : `Budget: $${(Number(autoBudget) || 0).toFixed(2)}`;
      toast({ title: "Autonomous run started", description: desc });
      navigate(`/runs/${data.id}`);
    },
    onError: (err: Error) => {
      toast({ title: "Failed to start", description: err.message, variant: "destructive" });
    },
  });

  const addKeyword = (keyword: string) => {
    const trimmed = keyword.trim();
    if (!trimmed) return;
    if (params.seedKeywords.includes(trimmed)) return;
    setParams((p) => ({ ...p, seedKeywords: [...p.seedKeywords, trimmed] }));
  };

  const addKeywordGroup = (keywords: readonly string[]) => {
    setParams((p) => {
      const existing = new Set(p.seedKeywords);
      const newKws = keywords.filter((kw) => !existing.has(kw));
      if (newKws.length === 0) return p;
      return { ...p, seedKeywords: [...p.seedKeywords, ...newKws] };
    });
  };

  const removeKeywordGroup = (keywords: readonly string[]) => {
    const toRemove = new Set(keywords);
    setParams((p) => ({ ...p, seedKeywords: p.seedKeywords.filter((k) => !toRemove.has(k)) }));
  };

  const removeKeyword = (keyword: string) => {
    setParams((p) => ({ ...p, seedKeywords: p.seedKeywords.filter((k) => k !== keyword) }));
  };

  const handleCustomKeywordSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    addKeyword(customKeyword);
    setCustomKeyword("");
  };

  const updateGeos = (val: string) => {
    setParams((p) => ({ ...p, seedGeos: val.split("\n").filter(Boolean) }));
  };

  const canRun = (platformTab === "patreon" || platformTab === "facebook" || platformTab === "podcast" || platformTab === "substack" || platformTab === "meetup") && params.seedKeywords.length > 0;

  const handlePlatformTabChange = (tab: string) => {
    setPlatformTab(tab);
    if (tab === "patreon") {
      setParams((p) => ({ ...p, enabledSources: ["patreon"], seedKeywords: [], minMemberCount: 0, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    } else if (tab === "facebook") {
      setParams((p) => ({ ...p, enabledSources: ["facebook"], seedKeywords: [], minMemberCount: 100, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    } else if (tab === "podcast") {
      setParams((p) => ({ ...p, enabledSources: ["podcast"], seedKeywords: [], minMemberCount: 0, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 10, podcastCountry: "US" }));
    } else if (tab === "substack") {
      setParams((p) => ({ ...p, enabledSources: ["substack"], seedKeywords: [], minMemberCount: 0, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    } else if (tab === "meetup") {
      setParams((p) => ({ ...p, enabledSources: ["meetup"], seedKeywords: [], minMemberCount: 50, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    }
  };

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-3xl mx-auto p-6 space-y-6">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-page-title">
            Community Host Finder
          </h1>
          <p className="text-sm text-muted-foreground">
            Discover high-potential community hosts from multiple platforms.
          </p>
        </div>

        <div className="flex gap-2" data-testid="mode-toggle">
          <Button
            variant={mode === "autonomous" ? "default" : "outline"}
            onClick={() => setMode("autonomous")}
            className="gap-1.5"
            data-testid="button-mode-autonomous"
          >
            <Zap className="w-4 h-4" />
            Autonomous
          </Button>
          <Button
            variant={mode === "manual" ? "default" : "outline"}
            onClick={() => setMode("manual")}
            className="gap-1.5"
            data-testid="button-mode-manual"
          >
            <Wrench className="w-4 h-4" />
            Manual
          </Button>
        </div>

        {mode === "autonomous" && (
          <div className="space-y-4">
            <Card className="p-4 space-y-4">
              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <Target className="w-4 h-4 text-muted-foreground" />
                  <Label className="text-sm font-medium">Email Target</Label>
                </div>
                <div className="flex items-center gap-2">
                  <Input
                    type="number"
                    min={1}
                    max={500}
                    step={1}
                    value={autoEmailTarget}
                    onChange={(e) => {
                      const val = e.target.value;
                      setAutoEmailTarget(val === "" ? "" : Math.min(500, Math.max(1, parseInt(val) || 1)));
                    }}
                    placeholder="How many emails do you want?"
                    className="w-full"
                    data-testid="input-auto-email-target"
                  />
                  <span className="text-xs text-muted-foreground whitespace-nowrap">emails</span>
                </div>
                <div className="flex gap-1.5 flex-wrap">
                  {[25, 50, 100, 200, 500].map((v) => (
                    <Badge
                      key={v}
                      variant={autoEmailTarget === v ? "default" : "outline"}
                      className={`cursor-pointer select-none toggle-elevate ${autoEmailTarget === v ? "toggle-elevated" : ""}`}
                      onClick={() => setAutoEmailTarget(v)}
                      data-testid={`badge-email-target-${v}`}
                    >
                      {v}
                    </Badge>
                  ))}
                </div>
              </div>

              <Separator />

              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <DollarSign className="w-4 h-4 text-muted-foreground" />
                  <Label className="text-sm font-medium">Max Budget</Label>
                  <span className="text-[11px] text-muted-foreground">(spending cap)</span>
                </div>
                <div className="flex items-center gap-2">
                  <Input
                    type="number"
                    min={1}
                    max={20}
                    step={0.5}
                    value={autoBudget}
                    onChange={(e) => {
                      const val = e.target.value;
                      setAutoBudget(val === "" ? "" : Math.min(20, Math.max(0.5, parseFloat(val) || 1)));
                    }}
                    placeholder="Max spend"
                    className="w-full"
                    data-testid="input-auto-budget"
                  />
                  <span className="text-xs text-muted-foreground whitespace-nowrap">USD</span>
                </div>
                <div className="flex gap-1.5 flex-wrap">
                  {[3, 5, 10, 15, 20].map((v) => (
                    <Badge
                      key={v}
                      variant={autoBudget === v ? "default" : "outline"}
                      className={`cursor-pointer select-none toggle-elevate ${autoBudget === v ? "toggle-elevated" : ""}`}
                      onClick={() => setAutoBudget(v)}
                      data-testid={`badge-budget-${v}`}
                    >
                      ${v}
                    </Badge>
                  ))}
                </div>
              </div>

              {previewAllocation && autoKeywords.length > 0 && (
                <>
                  <Separator />
                  <div className="flex items-center gap-2 flex-wrap">
                    <Settings2 className="w-3.5 h-3.5 text-muted-foreground" />
                    {previewLoading && <Loader2 className="w-3 h-3 animate-spin text-muted-foreground" />}
                    <span className="text-xs text-muted-foreground">
                      Estimated cost: <span className="font-semibold text-foreground">${previewAllocation.totalBudgetUsd.toFixed(2)}</span>
                      {autoBudget ? (
                        Number(previewAllocation.totalBudgetUsd) > Number(autoBudget)
                          ? <span className="text-destructive"> (over ${Number(autoBudget).toFixed(2)} cap — will stop at cap)</span>
                          : <span> of ${Number(autoBudget).toFixed(2)} max</span>
                      ) : null}
                      {" / "}
                      ~{previewAllocation.estimatedValidEmails ?? previewAllocation.estimatedEmails} valid emails
                      {previewAllocation.estimatedEmails > 0 && (
                        <span className="text-muted-foreground"> ({previewAllocation.estimatedEmails} raw, {Math.round((previewAllocation.estimatedValidEmailRate ?? previewAllocation.estimatedEmailRate) * 100)}% valid rate)</span>
                      )}
                    </span>
                  </div>
                </>
              )}

              <p className="text-[11px] text-muted-foreground">
                Set your email goal, then choose a max budget as a spending cap. Discovery stops when either target is reached or budget runs out.
              </p>
            </Card>

            <Card className="p-4 space-y-3">
              <div className="flex items-center gap-2">
                <Globe className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Platform Sources</Label>
              </div>
              <div className="space-y-1">
                {([
                  { id: "patreon", label: "Patreon", icon: SiPatreon },
                  { id: "facebook", label: "Facebook Groups", icon: SiFacebook },
                  { id: "podcast", label: "Podcasts", icon: SiApplepodcasts },
                  { id: "substack", label: "Substack", icon: SiSubstack },
                  { id: "meetup", label: "Meetup Groups", icon: SiMeetup },
                ] as const).map(({ id, label, icon: Icon }) => {
                  const isEnabled = autoEnabledPlatforms.includes(id);
                  const isLastEnabled = isEnabled && autoEnabledPlatforms.length === 1;
                  const stat = platformStats?.find(s => s.platform === id);
                  return (
                    <div
                      key={id}
                      className="flex items-center justify-between gap-3 py-1.5"
                      data-testid={`platform-row-${id}`}
                    >
                      <div className="flex items-center gap-2 min-w-0">
                        <Icon className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                        <span className={`text-sm ${isEnabled ? "text-foreground" : "text-muted-foreground"}`}>{label}</span>
                      </div>
                      <div className="flex items-center gap-3 shrink-0">
                        {stat && (
                          <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
                            <span title="Valid email conversion rate per lead discovered">
                              {stat.validRatePerLead}% valid{stat.isHistorical ? "" : "*"}
                            </span>
                            <span className="text-muted-foreground/40">|</span>
                            <span title="Estimated cost per valid email">
                              ${stat.costPerValidEmail.toFixed(2)}/email
                            </span>
                          </div>
                        )}
                        <Switch
                          checked={isEnabled}
                          disabled={isLastEnabled}
                          onCheckedChange={(checked) => {
                            setAutoEnabledPlatforms(prev => {
                              if (checked) return [...prev, id];
                              return prev.filter(p => p !== id);
                            });
                          }}
                          data-testid={`switch-platform-${id}`}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
              {platformStats && !platformStats.every(s => s.isHistorical) && (
                <p className="text-[10px] text-muted-foreground">* Estimated rates (not enough historical data yet)</p>
              )}
            </Card>


            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Keywords</Label>
              </div>
              {autoKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {autoKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-auto-kw-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => setAutoKeywords((prev) => prev.filter((k) => k !== kw))}
                      />
                    </Badge>
                  ))}
                </div>
              )}
              <Separator />
              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add:</p>
                <div className="flex flex-wrap gap-1.5">
                  {RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => autoKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""}`}
                        onClick={() => {
                          if (isActive) {
                            setAutoKeywords((prev) => prev.filter((kw) => !(rec.keywords as readonly string[]).includes(kw)));
                          } else {
                            setAutoKeywords((prev) => Array.from(new Set([...prev, ...rec.keywords])));
                          }
                        }}
                        data-testid={`badge-auto-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                      </Badge>
                    );
                  })}
                </div>
              </div>
              <form
                className="flex gap-2"
                onSubmit={(e) => {
                  e.preventDefault();
                  const trimmed = autoCustomKeyword.trim();
                  if (trimmed && !autoKeywords.includes(trimmed)) {
                    setAutoKeywords((prev) => [...prev, trimmed]);
                    setAutoCustomKeyword("");
                  }
                }}
              >
                <Input
                  placeholder="Add custom keyword..."
                  value={autoCustomKeyword}
                  onChange={(e) => setAutoCustomKeyword(e.target.value)}
                  data-testid="input-auto-custom-keyword"
                />
                <Button type="submit" size="default" variant="outline" data-testid="button-auto-add-keyword">
                  <Plus className="w-4 h-4" />
                </Button>
              </form>
            </Card>

            <div className="flex items-center justify-between gap-4 flex-wrap">
              <p className="text-sm text-muted-foreground">
                {autoKeywords.length} keyword{autoKeywords.length !== 1 ? "s" : ""}
                {autoEmailTarget ? ` / ${autoEmailTarget} email target` : ""}
                {autoBudget ? ` / $${Number(autoBudget).toFixed(2)} max` : ""}
              </p>
              <Button
                size="lg"
                onClick={() => autoRunMutation.mutate()}
                disabled={autoRunMutation.isPending || autoKeywords.length === 0 || (!autoBudget && !autoEmailTarget)}
                data-testid="button-run-autonomous"
                className="gap-2 min-w-[180px]"
              >
                {autoRunMutation.isPending ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Zap className="w-4 h-4" />
                )}
                {autoRunMutation.isPending ? "Starting..." : "Run Autonomous"}
                {!autoRunMutation.isPending && <ArrowRight className="w-4 h-4" />}
              </Button>
            </div>
          </div>
        )}

        {mode === "manual" && <><Tabs value={platformTab} onValueChange={handlePlatformTabChange}>
          <TabsList data-testid="tabs-platform">
            <TabsTrigger value="patreon" className="gap-1.5" data-testid="tab-patreon">
              <SiPatreon className="w-3.5 h-3.5" />
              Patreon
            </TabsTrigger>
            <TabsTrigger value="facebook" className="gap-1.5" data-testid="tab-facebook">
              <SiFacebook className="w-3.5 h-3.5" />
              Facebook Groups
            </TabsTrigger>
            <TabsTrigger value="podcast" className="gap-1.5" data-testid="tab-podcast">
              <SiApplepodcasts className="w-3.5 h-3.5" />
              Podcasters
            </TabsTrigger>
            <TabsTrigger value="substack" className="gap-1.5" data-testid="tab-substack">
              <SiSubstack className="w-3.5 h-3.5" />
              Substack
            </TabsTrigger>
            <TabsTrigger value="meetup" className="gap-1.5" data-testid="tab-meetup">
              <SiMeetup className="w-3.5 h-3.5" />
              Meetup
            </TabsTrigger>
            <TabsTrigger value="linkedin" className="gap-1.5" data-testid="tab-linkedin" disabled>
              <SiLinkedin className="w-3.5 h-3.5" />
              LinkedIn
            </TabsTrigger>
          </TabsList>

          <TabsContent value="patreon" className="mt-4 space-y-6">
            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Search Keywords</Label>
              </div>

              {params.seedKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {params.seedKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-keyword-active-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => removeKeyword(kw)}
                      />
                    </Badge>
                  ))}
                </div>
              )}

              <Separator />

              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add recommended searches:</p>
                <div className="flex flex-wrap gap-1.5">
                  {RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => params.seedKeywords.includes(kw));
                    const isPartial = !isActive && rec.keywords.some((kw) => params.seedKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""} ${isPartial ? "border-primary/50" : ""}`}
                        onClick={() => isActive ? removeKeywordGroup(rec.keywords) : addKeywordGroup(rec.keywords)}
                        data-testid={`badge-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                        <span className="text-[10px] opacity-60 ml-0.5">({rec.keywords.length})</span>
                      </Badge>
                    );
                  })}
                </div>
              </div>

              <Separator />

              <form onSubmit={handleCustomKeywordSubmit} className="flex gap-2">
                <Input
                  data-testid="input-custom-keyword"
                  placeholder="Add a custom keyword..."
                  value={customKeyword}
                  onChange={(e) => setCustomKeyword(e.target.value)}
                  className="text-sm"
                />
                <Button
                  type="submit"
                  size="icon"
                  variant="outline"
                  disabled={!customKeyword.trim()}
                  data-testid="button-add-keyword"
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </form>

              {usedKeywords.length > 0 && (
                <>
                  <Separator />
                  <UsedKeywordsSuggestions
                    usedKeywords={usedKeywords}
                    currentKeywords={params.seedKeywords}
                    onAddGroup={addKeywordGroup}
                  />
                </>
              )}
            </Card>

            <Card className="p-4 space-y-3">
              <div className="flex items-center gap-2">
                <MapPin className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Locations (optional)</Label>
              </div>
              <Textarea
                data-testid="input-geos"
                placeholder={"Denver, CO\nAustin, TX\nNew York, NY"}
                value={params.seedGeos.join("\n")}
                onChange={(e) => updateGeos(e.target.value)}
                className="resize-none text-sm min-h-[80px]"
              />
              <p className="text-[11px] text-muted-foreground">One location per line. Leave empty to search everywhere.</p>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Enrichment Methods</Label>
              </div>
              <p className="text-xs text-muted-foreground">Choose which methods to use for finding contact info after discovery.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {AVAILABLE_ENRICHMENTS.map((enr) => {
                  const paramKey = "enableApollo";
                  const isEnabled = params[paramKey];
                  return (
                    <label
                      key={enr.id}
                      className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                      data-testid={`enrichment-toggle-${enr.id}`}
                    >
                      <Checkbox
                        checked={isEnabled}
                        onCheckedChange={(checked) => {
                          setParams((p) => ({ ...p, [paramKey]: checked === true }));
                        }}
                        className="mt-0.5"
                      />
                      <div className="space-y-0.5">
                        <span className="text-sm font-medium leading-none">{enr.label}</span>
                        <p className="text-[11px] text-muted-foreground">{enr.description}</p>
                      </div>
                    </label>
                  );
                })}
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Filter className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Creator Filters</Label>
              </div>
              <p className="text-xs text-muted-foreground">Creators outside these ranges are excluded before enrichment. Set to 0 to disable a filter.</p>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Min Members</Label>
                  <Input
                    type="number"
                    value={params.minMemberCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, minMemberCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-min-members"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Max Members</Label>
                  <Input
                    type="number"
                    value={params.maxMemberCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, maxMemberCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-max-members"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Min Posts</Label>
                  <Input
                    type="number"
                    value={params.minPostCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, minPostCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-min-posts"
                  />
                </div>
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Settings</Label>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max Leads to Discover</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={200}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(200, Math.max(1, val)) }));
                  }}
                  data-testid="input-max-urls"
                />
                <p className="text-[11px] text-muted-foreground">Maximum 200 leads per run</p>
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="facebook" className="mt-4 space-y-6">
            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Search Keywords</Label>
              </div>

              {params.seedKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {params.seedKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-fb-keyword-active-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => removeKeyword(kw)}
                      />
                    </Badge>
                  ))}
                </div>
              )}

              <Separator />

              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add recommended searches:</p>
                <div className="flex flex-wrap gap-1.5">
                  {FB_RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => params.seedKeywords.includes(kw));
                    const isPartial = !isActive && rec.keywords.some((kw) => params.seedKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""} ${isPartial ? "border-primary/50" : ""}`}
                        onClick={() => isActive ? removeKeywordGroup(rec.keywords) : addKeywordGroup(rec.keywords)}
                        data-testid={`badge-fb-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                        <span className="text-[10px] opacity-60 ml-0.5">({rec.keywords.length})</span>
                      </Badge>
                    );
                  })}
                </div>
              </div>

              <Separator />

              <form onSubmit={handleCustomKeywordSubmit} className="flex gap-2">
                <Input
                  data-testid="input-fb-custom-keyword"
                  placeholder="Add a custom keyword..."
                  value={customKeyword}
                  onChange={(e) => setCustomKeyword(e.target.value)}
                  className="text-sm"
                />
                <Button
                  type="submit"
                  size="icon"
                  variant="outline"
                  disabled={!customKeyword.trim()}
                  data-testid="button-fb-add-keyword"
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </form>
            </Card>

            <Card className="p-4 space-y-3">
              <div className="flex items-center gap-2">
                <MapPin className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Locations (optional)</Label>
              </div>
              <Textarea
                data-testid="input-fb-geos"
                placeholder={"Denver, CO\nAustin, TX\nNew York, NY"}
                value={params.seedGeos.join("\n")}
                onChange={(e) => updateGeos(e.target.value)}
                className="resize-none text-sm min-h-[80px]"
              />
              <p className="text-[11px] text-muted-foreground">One location per line. Used in Google contact search for group leaders.</p>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Enrichment Methods</Label>
              </div>
              <p className="text-xs text-muted-foreground">Choose which methods to use for finding contact info after discovery.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {AVAILABLE_ENRICHMENTS.map((enr) => {
                  const paramKey = "enableApollo";
                  const isEnabled = params[paramKey];
                  return (
                    <label
                      key={enr.id}
                      className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                      data-testid={`fb-enrichment-toggle-${enr.id}`}
                    >
                      <Checkbox
                        checked={isEnabled}
                        onCheckedChange={(checked) => {
                          setParams((p) => ({ ...p, [paramKey]: checked === true }));
                        }}
                        className="mt-0.5"
                      />
                      <div className="space-y-0.5">
                        <span className="text-sm font-medium leading-none">{enr.label}</span>
                        <p className="text-[11px] text-muted-foreground">{enr.description}</p>
                      </div>
                    </label>
                  );
                })}
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Filter className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Group Filters</Label>
              </div>
              <p className="text-xs text-muted-foreground">Only groups within these ranges are kept. Set to 0 to disable a filter.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Min Members</Label>
                  <Input
                    type="number"
                    value={params.minMemberCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, minMemberCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-fb-min-members"
                  />
                  <p className="text-[11px] text-muted-foreground">Default 100. Higher = higher quality but fewer results.</p>
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Max Members</Label>
                  <Input
                    type="number"
                    value={params.maxMemberCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, maxMemberCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-fb-max-members"
                  />
                </div>
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Settings</Label>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max Groups to Discover</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={200}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(200, Math.max(1, val)) }));
                  }}
                  data-testid="input-fb-max-groups"
                />
                <p className="text-[11px] text-muted-foreground">Maximum 200 groups per run</p>
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="podcast" className="mt-4 space-y-6">
            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Search Keywords</Label>
              </div>

              {params.seedKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {params.seedKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-pod-keyword-active-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => removeKeyword(kw)}
                        data-testid={`button-pod-remove-keyword-${kw.replace(/\s+/g, "-")}`}
                      />
                    </Badge>
                  ))}
                </div>
              )}

              <Separator />

              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add recommended searches:</p>
                <div className="flex flex-wrap gap-1.5">
                  {PODCAST_RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => params.seedKeywords.includes(kw));
                    const isPartial = !isActive && rec.keywords.some((kw) => params.seedKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""} ${isPartial ? "border-primary/50" : ""}`}
                        onClick={() => isActive ? removeKeywordGroup(rec.keywords) : addKeywordGroup(rec.keywords)}
                        data-testid={`badge-pod-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                        <span className="text-[10px] opacity-60 ml-0.5">({rec.keywords.length})</span>
                      </Badge>
                    );
                  })}
                </div>
              </div>

              <Separator />

              <form onSubmit={handleCustomKeywordSubmit} className="flex gap-2">
                <Input
                  data-testid="input-pod-custom-keyword"
                  placeholder="Add a custom keyword..."
                  value={customKeyword}
                  onChange={(e) => setCustomKeyword(e.target.value)}
                  className="text-sm"
                />
                <Button
                  type="submit"
                  size="icon"
                  variant="outline"
                  disabled={!customKeyword.trim()}
                  data-testid="button-pod-add-keyword"
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </form>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Enrichment Methods</Label>
              </div>
              <p className="text-xs text-muted-foreground">RSS feeds are always scraped for host emails. Choose additional methods below.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {AVAILABLE_ENRICHMENTS.map((enr) => {
                  const paramKey = enr.id === "apollo" ? "enableApollo" as const : "enableApollo" as const;
                  const isEnabled = params[paramKey];
                  return (
                    <label
                      key={enr.id}
                      className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                      data-testid={`pod-enrichment-toggle-${enr.id}`}
                    >
                      <Checkbox
                        checked={isEnabled}
                        onCheckedChange={(checked) => {
                          setParams((p) => ({ ...p, [paramKey]: checked === true }));
                        }}
                        className="mt-0.5"
                      />
                      <div className="space-y-0.5">
                        <span className="text-sm font-medium leading-none">{enr.label}</span>
                        <p className="text-[11px] text-muted-foreground">{enr.description}</p>
                      </div>
                    </label>
                  );
                })}
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Filter className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Podcast Filters</Label>
              </div>
              <p className="text-xs text-muted-foreground">Filter by country and minimum episode count.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Country</Label>
                  <Select
                    value={params.podcastCountry || "US"}
                    onValueChange={(val) => setParams((p) => ({ ...p, podcastCountry: val }))}
                  >
                    <SelectTrigger data-testid="select-pod-country">
                      <SelectValue placeholder="Select country" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="US">United States</SelectItem>
                      <SelectItem value="GB">United Kingdom</SelectItem>
                      <SelectItem value="CA">Canada</SelectItem>
                      <SelectItem value="AU">Australia</SelectItem>
                      <SelectItem value="DE">Germany</SelectItem>
                      <SelectItem value="FR">France</SelectItem>
                      <SelectItem value="ES">Spain</SelectItem>
                      <SelectItem value="IT">Italy</SelectItem>
                      <SelectItem value="MX">Mexico</SelectItem>
                      <SelectItem value="BR">Brazil</SelectItem>
                      <SelectItem value="JP">Japan</SelectItem>
                      <SelectItem value="IN">India</SelectItem>
                      <SelectItem value="NZ">New Zealand</SelectItem>
                      <SelectItem value="IE">Ireland</SelectItem>
                      <SelectItem value="ZA">South Africa</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-[11px] text-muted-foreground">Apple Podcasts store region to search.</p>
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Min Episodes</Label>
                  <Input
                    type="number"
                    value={params.minEpisodeCount}
                    onChange={(e) =>
                      setParams((p) => ({ ...p, minEpisodeCount: parseInt(e.target.value) || 0 }))
                    }
                    data-testid="input-pod-min-episodes"
                  />
                  <p className="text-[11px] text-muted-foreground">Default 10. Higher = more established hosts.</p>
                </div>
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Settings</Label>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Email Target (max podcasts with emails)</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={500}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(500, Math.max(1, val)) }));
                  }}
                  data-testid="input-pod-max-podcasts"
                />
                <p className="text-[11px] text-muted-foreground">Searches until this many emails are found (max 500). Total leads may be higher.</p>
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="substack" className="mt-4 space-y-6">
            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Search Keywords</Label>
              </div>

              {params.seedKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {params.seedKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-sub-keyword-active-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => removeKeyword(kw)}
                        data-testid={`button-sub-remove-keyword-${kw.replace(/\s+/g, "-")}`}
                      />
                    </Badge>
                  ))}
                </div>
              )}

              <Separator />

              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add recommended searches:</p>
                <div className="flex flex-wrap gap-1.5">
                  {SUBSTACK_RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => params.seedKeywords.includes(kw));
                    const isPartial = !isActive && rec.keywords.some((kw) => params.seedKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""} ${isPartial ? "border-primary/50" : ""}`}
                        onClick={() => isActive ? removeKeywordGroup(rec.keywords) : addKeywordGroup(rec.keywords)}
                        data-testid={`badge-sub-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                        <span className="text-[10px] opacity-60 ml-0.5">({rec.keywords.length})</span>
                      </Badge>
                    );
                  })}
                </div>
              </div>

              <Separator />

              <form onSubmit={handleCustomKeywordSubmit} className="flex gap-2">
                <Input
                  data-testid="input-sub-custom-keyword"
                  placeholder="Add a custom keyword..."
                  value={customKeyword}
                  onChange={(e) => setCustomKeyword(e.target.value)}
                  className="text-sm"
                />
                <Button
                  type="submit"
                  size="icon"
                  variant="outline"
                  disabled={!customKeyword.trim()}
                  data-testid="button-sub-add-keyword"
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </form>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Enrichment Methods</Label>
              </div>
              <p className="text-xs text-muted-foreground">Substack about pages are always scraped for emails and social links. Choose additional methods below.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {AVAILABLE_ENRICHMENTS.map((enr) => {
                  const paramKey = "enableApollo" as const;
                  const isEnabled = params[paramKey];
                  return (
                    <label
                      key={enr.id}
                      className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                      data-testid={`sub-enrichment-toggle-${enr.id}`}
                    >
                      <Checkbox
                        checked={isEnabled}
                        onCheckedChange={(checked) => {
                          setParams((p) => ({ ...p, [paramKey]: checked === true }));
                        }}
                        className="mt-0.5"
                      />
                      <div className="space-y-0.5">
                        <span className="text-sm font-medium leading-none">{enr.label}</span>
                        <p className="text-[11px] text-muted-foreground">{enr.description}</p>
                      </div>
                    </label>
                  );
                })}
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Settings</Label>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max Leads to Discover</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={200}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(200, Math.max(1, val)) }));
                  }}
                  data-testid="input-sub-max-urls"
                />
                <p className="text-[11px] text-muted-foreground">Maximum 200 publications per run</p>
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="meetup" className="mt-4 space-y-6">
            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Search Keywords</Label>
              </div>

              {params.seedKeywords.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {params.seedKeywords.map((kw) => (
                    <Badge
                      key={kw}
                      variant="secondary"
                      className="gap-1 cursor-pointer select-none"
                      data-testid={`badge-meetup-keyword-active-${kw.replace(/\s+/g, "-")}`}
                    >
                      {kw}
                      <X
                        className="w-3 h-3"
                        onClick={() => removeKeyword(kw)}
                        data-testid={`button-meetup-remove-keyword-${kw.replace(/\s+/g, "-")}`}
                      />
                    </Badge>
                  ))}
                </div>
              )}

              <Separator />

              <div>
                <p className="text-xs text-muted-foreground mb-2">Click to add recommended searches:</p>
                <div className="flex flex-wrap gap-1.5">
                  {MEETUP_RECOMMENDED_KEYWORDS.map((rec) => {
                    const isActive = rec.keywords.every((kw) => params.seedKeywords.includes(kw));
                    const isPartial = !isActive && rec.keywords.some((kw) => params.seedKeywords.includes(kw));
                    return (
                      <Badge
                        key={rec.label}
                        variant={isActive ? "default" : "outline"}
                        className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""} ${isPartial ? "border-primary/50" : ""}`}
                        onClick={() => isActive ? removeKeywordGroup(rec.keywords) : addKeywordGroup(rec.keywords)}
                        data-testid={`badge-meetup-rec-${rec.label.replace(/\s+/g, "-")}`}
                      >
                        {rec.label}
                        <span className="text-[10px] opacity-60 ml-0.5">({rec.keywords.length})</span>
                      </Badge>
                    );
                  })}
                </div>
              </div>

              <Separator />

              <form onSubmit={handleCustomKeywordSubmit} className="flex gap-2">
                <Input
                  data-testid="input-meetup-custom-keyword"
                  placeholder="Add a custom keyword..."
                  value={customKeyword}
                  onChange={(e) => setCustomKeyword(e.target.value)}
                  className="text-sm"
                />
                <Button
                  type="submit"
                  size="icon"
                  variant="outline"
                  disabled={!customKeyword.trim()}
                  data-testid="button-meetup-add-keyword"
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </form>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Users className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Member Count Filter</Label>
              </div>
              <p className="text-xs text-muted-foreground">Filter Meetup groups by member count. Groups with larger memberships indicate more established communities.</p>
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Min Members</Label>
                  <Input
                    type="number"
                    value={params.minMemberCount || 50}
                    min={0}
                    max={100000}
                    onChange={(e) => {
                      const val = parseInt(e.target.value) || 0;
                      setParams((p) => ({ ...p, minMemberCount: Math.max(0, val) }));
                    }}
                    data-testid="input-meetup-min-members"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Max Members (0 = no limit)</Label>
                  <Input
                    type="number"
                    value={params.maxMemberCount || 0}
                    min={0}
                    max={1000000}
                    onChange={(e) => {
                      const val = parseInt(e.target.value) || 0;
                      setParams((p) => ({ ...p, maxMemberCount: Math.max(0, val) }));
                    }}
                    data-testid="input-meetup-max-members"
                  />
                </div>
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Enrichment Methods</Label>
              </div>
              <p className="text-xs text-muted-foreground">Meetup group pages are scraped for organizer names, emails, and social links. Choose additional methods below.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {AVAILABLE_ENRICHMENTS.map((enr) => {
                  const paramKey = "enableApollo" as const;
                  const isEnabled = params[paramKey];
                  return (
                    <label
                      key={enr.id}
                      className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                      data-testid={`meetup-enrichment-toggle-${enr.id}`}
                    >
                      <Checkbox
                        checked={isEnabled}
                        onCheckedChange={(checked) => {
                          setParams((p) => ({ ...p, [paramKey]: checked === true }));
                        }}
                        className="mt-0.5"
                      />
                      <div className="space-y-0.5">
                        <span className="text-sm font-medium leading-none">{enr.label}</span>
                        <p className="text-[11px] text-muted-foreground">{enr.description}</p>
                      </div>
                    </label>
                  );
                })}
              </div>
            </Card>

            <Card className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-muted-foreground" />
                <Label className="text-sm font-medium">Settings</Label>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max Groups to Discover</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={200}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(200, Math.max(1, val)) }));
                  }}
                  data-testid="input-meetup-max-urls"
                />
                <p className="text-[11px] text-muted-foreground">Maximum 200 groups per run</p>
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="linkedin" className="mt-4">
            <Card>
              <ComingSoonTab platform="LinkedIn" icon={SiLinkedin} />
            </Card>
          </TabsContent>
        </Tabs>

        <div className="flex items-center justify-between gap-4 flex-wrap">
          <p className="text-sm text-muted-foreground">
            {params.seedKeywords.length} search term{params.seedKeywords.length !== 1 ? "s" : ""} selected
            {params.seedGeos.length > 0 && ` across ${params.seedGeos.length} location${params.seedGeos.length !== 1 ? "s" : ""}`}
          </p>
          <Button
            size="lg"
            onClick={() => runMutation.mutate()}
            disabled={runMutation.isPending || !canRun}
            data-testid="button-run-finder"
            className="gap-2 min-w-[180px]"
          >
            {runMutation.isPending ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Rocket className="w-4 h-4" />
            )}
            {runMutation.isPending ? "Starting..." : "Run Finder"}
            {!runMutation.isPending && <ArrowRight className="w-4 h-4" />}
          </Button>
        </div>
        </>}
      </div>
    </div>
  );
}
