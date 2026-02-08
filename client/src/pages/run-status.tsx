import { useQuery } from "@tanstack/react-query";
import { useParams, Link } from "wouter";
import type { Run } from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  ArrowLeft,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  Globe,
  Users,
  Award,
  Eye,
  BarChart3,
  Download,
} from "lucide-react";

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, { variant: "default" | "secondary" | "destructive" | "outline"; icon: typeof Clock }> = {
    queued: { variant: "secondary", icon: Clock },
    running: { variant: "default", icon: Loader2 },
    succeeded: { variant: "outline", icon: CheckCircle2 },
    failed: { variant: "destructive", icon: XCircle },
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

  const { data: run, isLoading } = useQuery<Run>({
    queryKey: ["/api/runs", id],
    refetchInterval: (query) => {
      const d = query.state.data as Run | undefined;
      if (d && (d.status === "succeeded" || d.status === "failed")) return false;
      return 2000;
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
            {run.status === "succeeded" && (
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

        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
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
              <Award className="w-3.5 h-3.5" />
              <span className="text-xs">Qualified</span>
            </div>
            <p className="text-lg font-semibold text-chart-3" data-testid="text-qualified">
              {run.qualified || 0}
            </p>
          </Card>
          <Card className="p-3 space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Eye className="w-3.5 h-3.5" />
              <span className="text-xs">Watchlist</span>
            </div>
            <p className="text-lg font-semibold text-chart-4" data-testid="text-watchlist">
              {run.watchlist || 0}
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
