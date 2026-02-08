import { useState } from "react";
import { useLocation } from "wouter";
import { useMutation } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  RECOMMENDED_KEYWORDS,
  DEFAULT_RUN_PARAMS,
  AVAILABLE_SOURCES,
  type RunParams,
} from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Slider } from "@/components/ui/slider";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Rocket,
  Search,
  MapPin,
  Settings2,
  Loader2,
  ArrowRight,
  X,
  Plus,
  Globe,
} from "lucide-react";

export default function Home() {
  const [, navigate] = useLocation();
  const { toast } = useToast();

  const [params, setParams] = useState<RunParams>({ ...DEFAULT_RUN_PARAMS });
  const [customKeyword, setCustomKeyword] = useState("");

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

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-3xl mx-auto p-6 space-y-6">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-page-title">
            Community Host Finder
          </h1>
          <p className="text-sm text-muted-foreground">
            Select keywords to discover high-potential community hosts for TrovaTrip.
          </p>
        </div>

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
                const isActive = params.seedKeywords.includes(rec.keyword);
                return (
                  <Badge
                    key={rec.keyword}
                    variant={isActive ? "default" : "outline"}
                    className={`cursor-pointer select-none toggle-elevate ${isActive ? "toggle-elevated" : ""}`}
                    onClick={() => isActive ? removeKeyword(rec.keyword) : addKeyword(rec.keyword)}
                    data-testid={`badge-rec-${rec.keyword.replace(/\s+/g, "-")}`}
                  >
                    {rec.label}
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
            <Globe className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Data Sources</Label>
          </div>
          <p className="text-xs text-muted-foreground">Choose which platforms to search. Each requires a rented Apify actor.</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {AVAILABLE_SOURCES.map((src) => {
              const isEnabled = params.enabledSources.includes(src.id);
              return (
                <label
                  key={src.id}
                  className="flex items-start gap-3 p-2.5 rounded-md cursor-pointer hover-elevate"
                  data-testid={`source-toggle-${src.id}`}
                >
                  <Checkbox
                    checked={isEnabled}
                    onCheckedChange={(checked) => {
                      setParams((p) => ({
                        ...p,
                        enabledSources: checked === true
                          ? [...p.enabledSources, src.id]
                          : p.enabledSources.filter((s) => s !== src.id),
                      }));
                    }}
                    className="mt-0.5"
                  />
                  <div className="space-y-0.5">
                    <span className="text-sm font-medium leading-none">{src.label}</span>
                    <p className="text-[11px] text-muted-foreground">{src.description}</p>
                  </div>
                </label>
              );
            })}
          </div>
        </Card>

        <Card className="p-4 space-y-5">
          <div className="flex items-center gap-2">
            <Settings2 className="w-4 h-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Settings</Label>
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

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <Label className="text-xs text-muted-foreground">Max URLs to Discover</Label>
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
                <Label className="text-xs text-muted-foreground">Results per Search Query</Label>
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
            </div>
          </div>
        </Card>

        <div className="flex items-center justify-between gap-4 flex-wrap">
          <p className="text-sm text-muted-foreground">
            {params.seedKeywords.length} keyword{params.seedKeywords.length !== 1 ? "s" : ""} selected
            {params.seedGeos.length > 0 && ` across ${params.seedGeos.length} location${params.seedGeos.length !== 1 ? "s" : ""}`}
            {` using ${params.enabledSources.length} source${params.enabledSources.length !== 1 ? "s" : ""}`}
          </p>
          <Button
            size="lg"
            onClick={() => runMutation.mutate()}
            disabled={runMutation.isPending || params.seedKeywords.length === 0 || params.enabledSources.length === 0}
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
