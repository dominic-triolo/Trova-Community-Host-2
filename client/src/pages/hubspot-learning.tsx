import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Loader2, Brain, TrendingUp, Users, Lightbulb, RefreshCw, MapPin, Briefcase, Tag } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface ScoringInsights {
  topTraits: { trait: string; prevalence: number }[];
  topPlatforms: { platform: string; count: number }[];
  topCommunityTypes: { type: string; count: number }[];
  avgAudienceSize: number;
  avgPlatformCount: number;
  avgScore: number;
  suggestedKeywords: { keyword: string; score: number }[];
  topJobTitles?: { title: string; count: number }[];
  topLocations?: { location: string; count: number }[];
  topDealKeywords?: { keyword: string; count: number }[];
  topCompanies?: { company: string; count: number }[];
  avgConfirmedTrips?: number;
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

  const hasInsights = insights?.hasData && insights.insights;
  const ins = insights?.insights;

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
              Sync & Analyze
            </>
          )}
        </Button>
      </div>

      {syncResult && (
        <Card className="border-green-200 dark:border-green-800 bg-green-50 dark:bg-green-950/30">
          <CardContent className="pt-4">
            <p className="text-sm text-green-700 dark:text-green-300" data-testid="text-sync-result">
              Synced {syncResult.sync.dealsFound} deals, created {syncResult.sync.profilesCreated} host profiles.
              Found {syncResult.topHostCount} top Hosts (2+ confirmed trips) out of {syncResult.sampleSize} total contacts.
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
              Click "Sync & Analyze" to pull your Trips pipeline data and discover what makes your best Hosts successful.
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card data-testid="card-sample-size">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Total Contacts</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{insights!.sampleSize.toLocaleString()}</div>
                <p className="text-xs text-muted-foreground">
                  From your Trips pipeline
                </p>
              </CardContent>
            </Card>

            <Card data-testid="card-top-hosts">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Top Hosts</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{insights!.topHostCount.toLocaleString()}</div>
                <p className="text-xs text-muted-foreground">
                  2+ confirmed trips
                </p>
              </CardContent>
            </Card>

            <Card data-testid="card-avg-trips">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Avg Confirmed Trips</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{ins!.avgConfirmedTrips || 0}</div>
                <p className="text-xs text-muted-foreground">Per top Host</p>
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card data-testid="card-job-titles">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Briefcase className="h-4 w-4" />
                  Top Job Titles
                </CardTitle>
                <CardDescription>
                  Most common roles among your best Hosts
                </CardDescription>
              </CardHeader>
              <CardContent>
                {ins!.topJobTitles && ins!.topJobTitles.length > 0 ? (
                  <div className="space-y-2">
                    {ins!.topJobTitles.map((jt) => (
                      <div key={jt.title} className="flex items-center justify-between" data-testid={`jobtitle-${jt.title}`}>
                        <span className="text-sm capitalize">{jt.title}</span>
                        <Badge variant="secondary" className="text-xs">{jt.count}</Badge>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No job title data available</p>
                )}
              </CardContent>
            </Card>

            <Card data-testid="card-locations">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <MapPin className="h-4 w-4" />
                  Top Locations
                </CardTitle>
                <CardDescription>
                  Where your most successful Hosts are based
                </CardDescription>
              </CardHeader>
              <CardContent>
                {ins!.topLocations && ins!.topLocations.length > 0 ? (
                  <div className="space-y-2">
                    {ins!.topLocations.map((loc) => (
                      <div key={loc.location} className="flex items-center justify-between" data-testid={`location-${loc.location}`}>
                        <span className="text-sm">{loc.location}</span>
                        <Badge variant="secondary" className="text-xs">{loc.count}</Badge>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No location data available</p>
                )}
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card data-testid="card-deal-keywords">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Tag className="h-4 w-4" />
                  Deal Name Keywords
                </CardTitle>
                <CardDescription>
                  Recurring themes from trip deal names of top Hosts
                </CardDescription>
              </CardHeader>
              <CardContent>
                {ins!.topDealKeywords && ins!.topDealKeywords.length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {ins!.topDealKeywords.map((dk) => (
                      <Badge
                        key={dk.keyword}
                        variant={dk.count >= 5 ? "default" : "secondary"}
                        className="cursor-default"
                        data-testid={`deal-keyword-${dk.keyword}`}
                      >
                        {dk.keyword}
                        <span className="ml-1 opacity-70">({dk.count})</span>
                      </Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No recurring keywords found in deal names</p>
                )}
              </CardContent>
            </Card>

            <Card data-testid="card-suggested-keywords">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Lightbulb className="h-4 w-4" />
                  Suggested Discovery Keywords
                </CardTitle>
                <CardDescription>
                  Keywords from matched leads that produced top Hosts
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {ins!.suggestedKeywords.map((kw) => (
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
                  {ins!.suggestedKeywords.length === 0 && (
                    <p className="text-sm text-muted-foreground">Run more discovery pipelines to see keyword suggestions</p>
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
                  Online presence patterns among your best Hosts
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                {ins!.topTraits.length > 0 ? (
                  ins!.topTraits.map((t) => (
                    <div key={t.trait} className="flex items-center justify-between" data-testid={`trait-${t.trait}`}>
                      <span className="text-sm">{t.trait}</span>
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
                  ))
                ) : (
                  <p className="text-sm text-muted-foreground">No trait data available yet</p>
                )}
              </CardContent>
            </Card>

            <Card data-testid="card-companies">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Users className="h-4 w-4" />
                  Top Companies
                </CardTitle>
                <CardDescription>
                  Organizations with multiple successful Hosts
                </CardDescription>
              </CardHeader>
              <CardContent>
                {ins!.topCompanies && ins!.topCompanies.length > 0 ? (
                  <div className="space-y-2">
                    {ins!.topCompanies.map((c) => (
                      <div key={c.company} className="flex items-center justify-between" data-testid={`company-${c.company}`}>
                        <span className="text-sm">{c.company}</span>
                        <Badge variant="secondary" className="text-xs">{c.count} hosts</Badge>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No company patterns detected</p>
                )}
              </CardContent>
            </Card>
          </div>

          <p className="text-xs text-muted-foreground text-center">
            Last synced: {new Date(insights!.computedAt).toLocaleString()}
          </p>
        </>
      )}
    </div>
  );
}
