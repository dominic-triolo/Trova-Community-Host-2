import { useQuery, useMutation } from "@tanstack/react-query";
import { useParams, Link } from "wouter";
import type { Run } from "@shared/schema";
import { PIPELINE_STEP_LABELS } from "@shared/schema";
import type { PipelineStep } from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  ArrowLeft,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  Globe,
  Users,
  Mail,
  BarChart3,
  Download,
  RefreshCw,
  StopCircle,
  DollarSign,
  AlertTriangle,
  Play,
  RotateCcw,
} from "lucide-react";

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, { variant: "default" | "secondary" | "destructive" | "outline"; icon: typeof Clock }> = {
    queued: { variant: "secondary", icon: Clock },
    running: { variant: "default", icon: Loader2 },
    succeeded: { variant: "outline", icon: CheckCircle2 },
    failed: { variant: "destructive", icon: XCircle },
    interrupted: { variant: "secondary", icon: AlertTriangle },
  };
  const config = variants[status] || variants.queued;
  const Icon = config.icon;
  return (
    <Badge variant={config.variant} className="gap-1" data-testid={`badge-status-${status}`}>
      <Icon className={`w-3 h-3 ${status === "running" ? "animate-spin" : ""}`} />
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </Badge>
  );
}

function downloadRunCsv(runId: string) {
  const url = `/api/exports/csv?runId=${runId}`;
  fetch(url)
    .then((res) => res.blob())
    .then((blob) => {
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = `run${runId}_leads.csv`;
      a.click();
      URL.revokeObjectURL(href);
    });
}

export default function RunStatus() {
  const { id } = useParams<{ id: string }>();
  const { toast } = useToast();

  const { data: run, isLoading } = useQuery<Run>({
    queryKey: ["/api/runs", id],
    refetchInterval: (query) => {
      const d = query.state.data as Run | undefined;
      if (d && (d.status === "succeeded" || d.status === "failed" || d.status === "interrupted")) return false;
      return 2000;
    },
  });

  const cancelMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", `/api/runs/${id}/cancel`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs", id] });
      toast({ title: "Run stopping", description: "The run will stop at the next checkpoint." });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to stop run", description: err.message, variant: "destructive" });
    },
  });

  const reEnrichMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", `/api/runs/${id}/re-enrich`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs", id] });
      toast({ title: "Re-enrichment started", description: "Running Apollo + Leads Finder enrichment..." });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to start re-enrichment", description: err.message, variant: "destructive" });
    },
  });

  const resumeMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", `/api/runs/${id}/resume`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs", id] });
      toast({ title: "Resume started", description: "Continuing pipeline from where it left off..." });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to resume run", description: err.message, variant: "destructive" });
    },
  });

  const restartMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", `/api/runs/${id}/restart`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/runs", id] });
      toast({ title: "Restart started", description: "Re-running pipeline from the beginning..." });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to restart run", description: err.message, variant: "destructive" });
    },
  });

  if (isLoading) {
    return (
      <div className="flex-1 overflow-auto p-6 max-w-4xl mx-auto space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (!run) {
    return (
      <div className="flex-1 overflow-auto p-6 max-w-4xl mx-auto">
        <Card className="p-8 text-center space-y-3">
          <XCircle className="w-10 h-10 text-muted-foreground mx-auto" />
          <p className="text-sm text-muted-foreground">Run not found</p>
          <Link href="/">
            <Button variant="outline" size="sm" data-testid="button-back-home">
              <ArrowLeft className="w-4 h-4 mr-1" /> Back to Home
            </Button>
          </Link>
        </Card>
      </div>
    );
  }

  const logLines = (run.logs || "").split("\n").filter(Boolean);

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="space-y-1">
            <div className="flex items-center gap-3 flex-wrap">
              <h1 className="text-xl font-semibold tracking-tight" data-testid="text-run-title">
                Run #{run.id}
              </h1>
              <StatusBadge status={run.status} />
            </div>
            <p className="text-sm text-muted-foreground">
              {run.step || "Waiting to start..."}
            </p>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <Link href="/">
              <Button variant="outline" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-1" /> New Run
              </Button>
            </Link>
            {(run.status === "running" || run.status === "queued") && (
              <Button
                variant="destructive"
                size="sm"
                onClick={() => cancelMutation.mutate()}
                disabled={cancelMutation.isPending}
                data-testid="button-stop-run"
              >
                <StopCircle className={`w-4 h-4 mr-1 ${cancelMutation.isPending ? "animate-spin" : ""}`} />
                Stop Run
              </Button>
            )}
            {run.status === "interrupted" && (
              <Button
                variant="destructive"
                size="sm"
                onClick={() => cancelMutation.mutate()}
                disabled={cancelMutation.isPending}
                data-testid="button-cancel-interrupted"
              >
                <StopCircle className={`w-4 h-4 mr-1 ${cancelMutation.isPending ? "animate-spin" : ""}`} />
                End Run
              </Button>
            )}
            {(run.status === "interrupted" || run.status === "failed") && (run as any).lastCompletedStep && (
              <Button
                size="sm"
                onClick={() => resumeMutation.mutate()}
                disabled={resumeMutation.isPending || restartMutation.isPending}
                data-testid="button-resume"
              >
                <Play className={`w-4 h-4 mr-1 ${resumeMutation.isPending ? "animate-spin" : ""}`} />
                Resume (from {PIPELINE_STEP_LABELS[(run as any).lastCompletedStep as PipelineStep] || (run as any).lastCompletedStep})
              </Button>
            )}
            {(run.status === "interrupted" || run.status === "failed") && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => restartMutation.mutate()}
                disabled={restartMutation.isPending || resumeMutation.isPending}
                data-testid="button-restart"
              >
                <RotateCcw className={`w-4 h-4 mr-1 ${restartMutation.isPending ? "animate-spin" : ""}`} />
                Restart from beginning
              </Button>
            )}
            {(run.status === "succeeded" || run.status === "failed" || run.status === "interrupted") && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => reEnrichMutation.mutate()}
                disabled={reEnrichMutation.isPending}
                data-testid="button-re-enrich"
              >
                <RefreshCw className={`w-4 h-4 mr-1 ${reEnrichMutation.isPending ? "animate-spin" : ""}`} />
                Re-enrich
              </Button>
            )}
            {(run.status === "succeeded" || run.status === "interrupted") && (
              <>
                <Link href={`/results?runId=${run.id}`}>
                  <Button size="sm" data-testid="button-view-results">
                    <BarChart3 className="w-4 h-4 mr-1" /> View Results
                  </Button>
                </Link>
                <Button variant="outline" size="sm" onClick={() => downloadRunCsv(String(run.id))} data-testid="button-export-csv">
                  <Download className="w-4 h-4 mr-1" /> Export CSV
                </Button>
              </>
            )}
          </div>
        </div>

        <Card className="p-4 space-y-3">
          <div className="flex items-center justify-between gap-2 flex-wrap">
            <span className="text-sm font-medium">Progress</span>
            <span className="text-sm text-muted-foreground">{run.progress}%</span>
          </div>
          <Progress value={run.progress} className="h-2" data-testid="progress-bar" />
        </Card>

        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Globe className="w-3.5 h-3.5" />
              <span className="text-xs">URLs Found</span>
            </div>
            <p className="text-lg font-semibold" data-testid="text-urls-discovered">
              {run.urlsDiscovered || 0}
            </p>
          </Card>
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Users className="w-3.5 h-3.5" />
              <span className="text-xs">Leads</span>
            </div>
            <p className="text-lg font-semibold" data-testid="text-leads-extracted">
              {run.leadsExtracted || 0}
            </p>
          </Card>
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Mail className="w-3.5 h-3.5" />
              <span className="text-xs">Emails Found</span>
            </div>
            <p className="text-lg font-semibold" data-testid="text-with-email">
              {(run as any).leadsWithEmail || 0}
            </p>
          </Card>
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <CheckCircle2 className="w-3.5 h-3.5" />
              <span className="text-xs">{(run as any).emailTarget > 0 ? "Valid / Target" : "Valid Emails"}</span>
            </div>
            <p className="text-lg font-semibold text-chart-3" data-testid="text-valid-emails">
              {(run as any).leadsWithValidEmail || 0}
              {(run as any).emailTarget > 0 && (
                <span className="text-sm font-normal text-muted-foreground"> / {(run as any).emailTarget}</span>
              )}
            </p>
          </Card>
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <DollarSign className="w-3.5 h-3.5" />
              <span className="text-xs">{(run as any).isAutonomous ? "Budget Used" : "Apify Spend"}</span>
            </div>
            <p className="text-lg font-semibold" data-testid="text-apify-spend">
              ${((run as any).apifySpendUsd || 0).toFixed(2)}
              {(run as any).isAutonomous && (run as any).budgetUsd > 0 && (
                <span className="text-sm font-normal text-muted-foreground"> / ${((run as any).budgetUsd || 0).toFixed(2)}</span>
              )}
            </p>
          </Card>
        </div>

        <Card className="p-4 space-y-3">
          <div className="flex items-center justify-between gap-2 flex-wrap">
            <span className="text-sm font-medium">Live Logs</span>
            <Badge variant="secondary" className="text-[10px]">
              {logLines.length} entries
            </Badge>
          </div>
          <ScrollArea className="h-[300px] rounded-md border p-3 bg-background">
            <div className="space-y-1 font-mono text-xs" data-testid="log-output">
              {logLines.length === 0 ? (
                <p className="text-muted-foreground">No logs yet...</p>
              ) : (
                logLines.map((line, i) => (
                  <p key={i} className={line.startsWith("[ERROR]") ? "text-destructive" : "text-muted-foreground"}>
                    {line}
                  </p>
                ))
              )}
            </div>
          </ScrollArea>
        </Card>
      </div>
    </div>
  );
}
