import { useQuery } from "@tanstack/react-query";
import { Link } from "wouter";
import type { Run } from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  ArrowRight,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  Plus,
  Activity,
} from "lucide-react";

function statusIcon(status: string) {
  switch (status) {
    case "running": return <Loader2 className="w-3.5 h-3.5 animate-spin" />;
    case "succeeded": return <CheckCircle2 className="w-3.5 h-3.5" />;
    case "failed": return <XCircle className="w-3.5 h-3.5" />;
    default: return <Clock className="w-3.5 h-3.5" />;
  }
}

function statusVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "running": return "default";
    case "succeeded": return "outline";
    case "failed": return "destructive";
    default: return "secondary";
  }
}

export default function RunsList() {
  const { data: runs, isLoading } = useQuery<Run[]>({
    queryKey: ["/api/runs"],
    refetchInterval: 5000,
  });

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="space-y-1">
            <h1 className="text-2xl font-semibold tracking-tight" data-testid="text-runs-title">
              Pipeline Runs
            </h1>
            <p className="text-sm text-muted-foreground">
              View active and past discovery pipeline runs.
            </p>
          </div>
          <Link href="/">
            <Button size="sm" data-testid="button-new-run">
              <Plus className="w-4 h-4 mr-1" /> New Run
            </Button>
          </Link>
        </div>

        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-20 w-full" />
            ))}
          </div>
        ) : !runs || runs.length === 0 ? (
          <Card className="p-8 text-center space-y-3">
            <Activity className="w-10 h-10 text-muted-foreground mx-auto" />
            <p className="text-sm font-medium">No runs yet</p>
            <p className="text-xs text-muted-foreground">Start a new discovery pipeline to find community hosts.</p>
            <Link href="/">
              <Button size="sm" data-testid="button-start-first-run">
                <Plus className="w-4 h-4 mr-1" /> Start First Run
              </Button>
            </Link>
          </Card>
        ) : (
          <div className="space-y-3">
            {runs.map((run) => (
              <Link key={run.id} href={`/runs/${run.id}`}>
                <Card className="p-4 hover-elevate cursor-pointer" data-testid={`card-run-${run.id}`}>
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    <div className="flex items-center gap-3">
                      <Badge variant={statusVariant(run.status)} className="gap-1">
                        {statusIcon(run.status)}
                        {run.status}
                      </Badge>
                      <div>
                        <p className="text-sm font-medium">Run #{run.id}</p>
                        <p className="text-xs text-muted-foreground">{run.step || "Queued"}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right text-xs text-muted-foreground space-y-0.5">
                        <p>{run.urlsDiscovered || 0} URLs</p>
                        <p>{run.qualified || 0} qualified</p>
                      </div>
                      <ArrowRight className="w-4 h-4 text-muted-foreground" />
                    </div>
                  </div>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
