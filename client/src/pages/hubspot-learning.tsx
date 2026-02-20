import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Loader2, Brain, TrendingUp, Users, Target, Lightbulb, RefreshCw, BarChart3 } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface ScoringInsights {
  topTraits: { trait: string; prevalence: number }[];
  topPlatforms: { platform: string; count: number }[];
  topCommunityTypes: { type: string; count: number }[];
  avgAudienceSize: number;
  avgPlatformCount: number;
  avgScore: number;
  suggestedKeywords: { keyword: string; score: number }[];
}

interface LearnedWeights {
  nicheIdentity: number;
  trustLeadership: number;
  engagement: number;
  monetization: number;
  ownedChannels: number;
  tripFit: number;
}

interface InsightsData {
  hasData: boolean;
  weights: LearnedWeights | null;
  insights: ScoringInsights | null;
  sampleSize: number;
  topHostCount: number;
  computedAt: string;
}

const DEFAULT_WEIGHTS: LearnedWeights = {
  nicheIdentity: 20,
  trustLeadership: 15,
  engagement: 20,
  monetization: 15,
  ownedChannels: 20,
  tripFit: 10,
};

const WEIGHT_LABELS: Record<string, string> = {
  nicheIdentity: "Niche Identity",
  trustLeadership: "Trust & Leadership",
  engagement: "Engagement",
  monetization: "Monetization",
  ownedChannels: "Owned Channels",
  tripFit: "Trip Fit",
};

export default function HubSpotLearning() {
  const { toast } = useToast();
  const [syncResult, setSyncResult] = useState<any>(null);

  const { data: insights, isLoading } = useQuery<InsightsData>({
    queryKey: ["/api/hubspot/insights"],
  });

  const syncMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/hubspot/sync");
      return res.json();
    },
    onSuccess: (data) => {
      setSyncResult(data);
      queryClient.invalidateQueries({ queryKey: ["/api/hubspot/insights"] });
      toast({
        title: "HubSpot Sync Complete",
        description: `Analyzed ${data.sync.profilesCreated} host profiles from ${data.sync.dealsFound} deals`,
      });
    },
    onError: (err: any) => {
      toast({
        title: "Sync Failed",
        description: err.message || "Could not sync with HubSpot",
        variant: "destructive",
      });
    },
  });

  const hasInsights = insights?.hasData && insights.weights && insights.insights;

  return (
    <div className="flex-1 overflow-auto p-6 space-y-6" data-testid="page-hubspot-learning">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2" data-testid="text-page-title">
            <Brain className="h-6 w-6 text-primary" />
            HubSpot Learning
          </h1>
          <p className="text-muted-foreground mt-1">
            Analyze your top-performing Hosts to discover patterns and high-converting keywords
          </p>
        </div>
        <Button
          data-testid="button-sync-hubspot"
          onClick={() => syncMutation.mutate()}
          disabled={syncMutation.isPending}
          size="lg"
        >
          {syncMutation.isPending ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin mr-2" />
              Syncing...
            </>
          ) : (
            <>
              <RefreshCw className="h-4 w-4 mr-2" />
              Sync & Learn from HubSpot
            </>
          )}
        </Button>
      </div>

      {syncResult && (
        <Card className="border-green-200 dark:border-green-800 bg-green-50 dark:bg-green-950/30">
          <CardContent className="pt-4">
            <p className="text-sm text-green-700 dark:text-green-300" data-testid="text-sync-result">
              Synced {syncResult.sync.dealsFound} deals, created {syncResult.sync.profilesCreated} host profiles.
              Computed weights from {syncResult.topHostCount} top Hosts (out of {syncResult.sampleSize} total).
            </p>
          </CardContent>
        </Card>
      )}

      {isLoading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : !hasInsights ? (
        <Card>
          <CardContent className="py-12 text-center">
            <Brain className="h-12 w-12 mx-auto mb-4 text-muted-foreground/50" />
            <h3 className="text-lg font-medium mb-2">No Learning Data Yet</h3>
            <p className="text-muted-foreground max-w-md mx-auto mb-4">
              Click "Sync & Learn from HubSpot" to analyze your Trips pipeline deals and identify what makes your best Hosts successful.
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card data-testid="card-sample-size">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Hosts Analyzed</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{insights.sampleSize}</div>
                <p className="text-xs text-muted-foreground">
                  {insights.topHostCount} top performers (2+ confirmed trips)
                </p>
              </CardContent>
            </Card>

            <Card data-testid="card-avg-score">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Avg Top Host Score</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{insights.insights!.avgScore}</div>
                <p className="text-xs text-muted-foreground">ICP score of top performers</p>
              </CardContent>
            </Card>

            <Card data-testid="card-avg-audience">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Avg Audience Size</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{(insights.insights!.avgAudienceSize || 0).toLocaleString()}</div>
                <p className="text-xs text-muted-foreground">
                  Across {insights.insights!.avgPlatformCount} platforms avg
                </p>
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card data-testid="card-learned-weights">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <BarChart3 className="h-4 w-4" />
                  Learned Scoring Weights
                  <Badge variant="outline" className="ml-auto text-xs font-normal">View Only</Badge>
                </CardTitle>
                <CardDescription>
                  What scoring weights would look like based on top Host traits. These are not applied to scoring yet.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {Object.entries(insights.weights!).map(([key, value]) => {
                  const defaultVal = DEFAULT_WEIGHTS[key as keyof LearnedWeights];
                  const diff = value - defaultVal;
                  const diffLabel = diff > 0 ? `+${diff}` : diff < 0 ? `${diff}` : "=";
                  const diffColor = diff > 0 ? "text-green-600 dark:text-green-400" : diff < 0 ? "text-red-600 dark:text-red-400" : "text-muted-foreground";
                  return (
                    <div key={key} className="flex items-center justify-between" data-testid={`weight-${key}`}>
                      <span className="text-sm font-medium">{WEIGHT_LABELS[key] || key}</span>
                      <div className="flex items-center gap-3">
                        <div className="w-32 h-2 bg-muted rounded-full overflow-hidden">
                          <div
                            className="h-full bg-primary rounded-full transition-all"
                            style={{ width: `${(value / 30) * 100}%` }}
                          />
                        </div>
                        <span className="text-sm font-mono w-8 text-right">{value}</span>
                        <span className={`text-xs font-mono w-8 ${diffColor}`}>{diffLabel}</span>
                      </div>
                    </div>
                  );
                })}
                <p className="text-xs text-muted-foreground pt-2 border-t">
                  Last computed: {new Date(insights.computedAt).toLocaleString()}
                </p>
              </CardContent>
            </Card>

            <Card data-testid="card-suggested-keywords">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Lightbulb className="h-4 w-4" />
                  Suggested Keywords
                </CardTitle>
                <CardDescription>
                  Keywords from top-performing Hosts that may yield high-quality leads
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {insights.insights!.suggestedKeywords.map((kw) => (
                    <Badge
                      key={kw.keyword}
                      variant={kw.score >= 50 ? "default" : "secondary"}
                      className="cursor-default"
                      data-testid={`keyword-${kw.keyword}`}
                    >
                      {kw.keyword}
                      <span className="ml-1 opacity-70">{kw.score}%</span>
                    </Badge>
                  ))}
                  {insights.insights!.suggestedKeywords.length === 0 && (
                    <p className="text-sm text-muted-foreground">No keyword patterns detected yet</p>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card data-testid="card-top-traits">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <TrendingUp className="h-4 w-4" />
                  Top Host Traits
                </CardTitle>
                <CardDescription>
                  Most common characteristics among your best-performing Hosts
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                {insights.insights!.topTraits.map((t) => (
                  <div key={t.trait} className="flex items-center justify-between" data-testid={`trait-${t.trait}`}>
                    <span className="text-sm capitalize">{t.trait.replace(/_/g, " ")}</span>
                    <div className="flex items-center gap-2">
                      <div className="w-20 h-1.5 bg-muted rounded-full overflow-hidden">
                        <div
                          className="h-full bg-primary/70 rounded-full"
                          style={{ width: `${t.prevalence}%` }}
                        />
                      </div>
                      <span className="text-xs text-muted-foreground w-10 text-right">{t.prevalence}%</span>
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>

            <Card data-testid="card-top-platforms">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Users className="h-4 w-4" />
                  Source Platforms & Community Types
                </CardTitle>
                <CardDescription>
                  Where your top Hosts were discovered
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <h4 className="text-xs font-medium text-muted-foreground uppercase mb-2">Discovery Platforms</h4>
                  <div className="flex flex-wrap gap-2">
                    {insights.insights!.topPlatforms.map((p) => (
                      <Badge key={p.platform} variant="outline" data-testid={`platform-${p.platform}`}>
                        {p.platform} ({p.count})
                      </Badge>
                    ))}
                  </div>
                </div>
                <div>
                  <h4 className="text-xs font-medium text-muted-foreground uppercase mb-2">Community Types</h4>
                  <div className="flex flex-wrap gap-2">
                    {insights.insights!.topCommunityTypes.map((ct) => (
                      <Badge key={ct.type} variant="outline" data-testid={`community-type-${ct.type}`}>
                        {ct.type} ({ct.count})
                      </Badge>
                    ))}
                    {insights.insights!.topCommunityTypes.length === 0 && (
                      <span className="text-sm text-muted-foreground">No data yet</span>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}
