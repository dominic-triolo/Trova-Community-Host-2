import { useState } from "react";
import { useLocation } from "wouter";
import { useMutation } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  COMMUNITY_TYPES,
  INTENT_TERMS,
  SOURCE_CONNECTORS,
  DEFAULT_RUN_PARAMS,
  type RunParams,
} from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import {
  Rocket,
  Search,
  MapPin,
  Users,
  Zap,
  Globe,
  Settings2,
  Loader2,
  ArrowRight,
} from "lucide-react";

export default function Home() {
  const [, navigate] = useLocation();
  const { toast } = useToast();

  const [params, setParams] = useState<RunParams>({ ...DEFAULT_RUN_PARAMS });

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

  const updateKeywords = (val: string) => {
    setParams((p) => ({ ...p, seedKeywords: val.split("\n").filter(Boolean) }));
  };
  const updateGeos = (val: string) => {
    setParams((p) => ({ ...p, seedGeos: val.split("\n").filter(Boolean) }));
  };
  const toggleCommunityType = (type: string) => {
    setParams((p) => ({
      ...p,
      communityTypes: p.communityTypes.includes(type)
        ? p.communityTypes.filter((t) => t !== type)
        : [...p.communityTypes, type],
    }));
  };
  const toggleIntent = (term: string) => {
    setParams((p) => ({
      ...p,
      intentTerms: p.intentTerms.includes(term)
        ? p.intentTerms.filter((t) => t !== term)
        : [...p.intentTerms, term],
    }));
  };
  const toggleSource = (key: string) => {
    setParams((p) => ({
      ...p,
      sources: { ...p.sources, [key]: !p.sources[key] },
    }));
  };

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto p-6 space-y-6">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-page-title">
            Community Host Finder
          </h1>
          <p className="text-sm text-muted-foreground">
            Discover, enrich, and score high-potential community hosts for TrovaTrip.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="p-4 space-y-3">
            <div className="flex items-center gap-2">
              <Search className="w-4 h-4 text-muted-foreground" />
              <Label className="text-sm font-medium">Seed Keywords</Label>
            </div>
            <Textarea
              data-testid="input-seed-keywords"
              placeholder={"community group\nlocal club\nchurch group"}
              value={params.seedKeywords.join("\n")}
              onChange={(e) => updateKeywords(e.target.value)}
              className="resize-none text-sm min-h-[100px]"
            />
            <p className="text-[11px] text-muted-foreground">One keyword per line</p>
          </Card>

          <Card className="p-4 space-y-3">
            <div className="flex items-center gap-2">
              <MapPin className="w-4 h-4 text-muted-foreground" />
              <Label className="text-sm font-medium">Geographic Focus (optional)</Label>
            </div>
            <Textarea
              data-testid="input-geos"
              placeholder={"Denver, CO\nAustin, TX\nNew York, NY"}
              value={params.seedGeos.join("\n")}
              onChange={(e) => updateGeos(e.target.value)}
              className="resize-none text-sm min-h-[100px]"
            />
            <p className="text-[11px] text-muted-foreground">One location per line (optional)</p>
          </Card>
        </div>

        <Card className="p-4 space-y-4">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Community Categories</Label>
          </div>
          <div className="flex flex-wrap gap-2">
            {COMMUNITY_TYPES.map((ct) => {
              const selected = params.communityTypes.includes(ct.value);
              return (
                <Button
                  key={ct.value}
                  variant={selected ? "default" : "outline"}
                  size="sm"
                  onClick={() => toggleCommunityType(ct.value)}
                  data-testid={`button-category-${ct.value}`}
                  className="toggle-elevate"
                >
                  {ct.label}
                </Button>
              );
            })}
          </div>
        </Card>

        <Card className="p-4 space-y-4">
          <div className="flex items-center gap-2">
            <Zap className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Intent Terms</Label>
          </div>
          <div className="flex flex-wrap gap-2">
            {INTENT_TERMS.map((term) => {
              const selected = params.intentTerms.includes(term);
              return (
                <Badge
                  key={term}
                  variant={selected ? "default" : "outline"}
                  className={`cursor-pointer select-none toggle-elevate ${selected ? "toggle-elevated" : ""}`}
                  onClick={() => toggleIntent(term)}
                  data-testid={`badge-intent-${term}`}
                >
                  {term}
                </Badge>
              );
            })}
          </div>
        </Card>

        <Card className="p-4 space-y-4">
          <div className="flex items-center gap-2">
            <Globe className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Data Sources</Label>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
            {SOURCE_CONNECTORS.map((src) => (
              <div
                key={src.key}
                className="flex items-center justify-between gap-2 rounded-md border p-3"
              >
                <Label className="text-sm cursor-pointer" htmlFor={`source-${src.key}`}>
                  {src.label}
                </Label>
                <Switch
                  id={`source-${src.key}`}
                  checked={params.sources[src.key] ?? false}
                  onCheckedChange={() => toggleSource(src.key)}
                  disabled={"required" in src && src.required}
                  data-testid={`switch-source-${src.key}`}
                />
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-4 space-y-5">
          <div className="flex items-center gap-2">
            <Settings2 className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Budget & Thresholds</Label>
          </div>

          <div className="space-y-4">
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2 flex-wrap">
                <Label className="text-sm">Qualification Threshold</Label>
                <Badge variant="secondary">{params.threshold}</Badge>
              </div>
              <Slider
                value={[params.threshold]}
                onValueChange={([v]) => setParams((p) => ({ ...p, threshold: v }))}
                min={30}
                max={95}
                step={5}
                data-testid="slider-threshold"
              />
              <p className="text-[11px] text-muted-foreground">Leads scoring at or above this are marked Qualified</p>
            </div>

            <Separator />

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max Discovered URLs</Label>
                <Input
                  type="number"
                  value={params.maxDiscoveredUrls}
                  onChange={(e) =>
                    setParams((p) => ({ ...p, maxDiscoveredUrls: parseInt(e.target.value) || 200 }))
                  }
                  data-testid="input-max-urls"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Results per Query</Label>
                <Input
                  type="number"
                  value={params.maxGoogleResultsPerQuery}
                  onChange={(e) =>
                    setParams((p) => ({
                      ...p,
                      maxGoogleResultsPerQuery: parseInt(e.target.value) || 10,
                    }))
                  }
                  data-testid="input-results-per-query"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Crawl Pages per Site</Label>
                <Input
                  type="number"
                  value={params.maxCrawlPagesPerSite}
                  onChange={(e) =>
                    setParams((p) => ({
                      ...p,
                      maxCrawlPagesPerSite: parseInt(e.target.value) || 3,
                    }))
                  }
                  data-testid="input-crawl-pages"
                />
              </div>
            </div>
          </div>
        </Card>

        <div className="flex justify-end">
          <Button
            size="lg"
            onClick={() => runMutation.mutate()}
            disabled={runMutation.isPending || params.seedKeywords.length === 0}
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
