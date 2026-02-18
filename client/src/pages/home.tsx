import { useState, useMemo } from "react";
import { useLocation } from "wouter";
import { useMutation, useQuery } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  RECOMMENDED_KEYWORDS,
  FB_RECOMMENDED_KEYWORDS,
  PODCAST_RECOMMENDED_KEYWORDS,
  DEFAULT_RUN_PARAMS,
  AVAILABLE_ENRICHMENTS,
  type RunParams,
  type Run,
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
} from "lucide-react";
import { SiPatreon, SiFacebook, SiLinkedin, SiApplepodcasts } from "react-icons/si";

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

  const canRun = (platformTab === "patreon" || platformTab === "facebook" || platformTab === "podcast") && params.seedKeywords.length > 0;

  const handlePlatformTabChange = (tab: string) => {
    setPlatformTab(tab);
    if (tab === "patreon") {
      setParams((p) => ({ ...p, enabledSources: ["patreon"], seedKeywords: [], minMemberCount: 0, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    } else if (tab === "facebook") {
      setParams((p) => ({ ...p, enabledSources: ["facebook"], seedKeywords: [], minMemberCount: 100, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 0 }));
    } else if (tab === "podcast") {
      setParams((p) => ({ ...p, enabledSources: ["podcast"], seedKeywords: [], minMemberCount: 0, maxMemberCount: 0, minPostCount: 0, minEpisodeCount: 10, podcastCountry: "US" }));
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
            Select a platform and keywords to discover high-potential community hosts.
          </p>
        </div>

        <Tabs value={platformTab} onValueChange={handlePlatformTabChange}>
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
                <Label className="text-xs text-muted-foreground">Max Podcasts to Discover</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  min={1}
                  max={200}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 200;
                    setParams((p) => ({ ...p, maxDiscoveredUrls: Math.min(200, Math.max(1, val)) }));
                  }}
                  data-testid="input-pod-max-podcasts"
                />
                <p className="text-[11px] text-muted-foreground">Maximum 200 podcasts per run</p>
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
      </div>
    </div>
  );
}
