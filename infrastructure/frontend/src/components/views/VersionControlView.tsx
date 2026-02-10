import { useState, useEffect } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  GitBranch,
  GitCommit,
  GitPullRequest,
  GithubLogo,
  Clock,
  FileCode,
  LinkSimple,
  Plus,
} from "@phosphor-icons/react";
import { toast } from "sonner";
import type { GitHubRepo, VersionHistory } from "@/types";

export function VersionControlView() {
  const [connectedRepo, setConnectedRepo] = useState<GitHubRepo | null>(null);
  const [versionHistory, setVersionHistory] = useState<VersionHistory[]>([]);
  const [availableRepos, setAvailableRepos] = useState<GitHubRepo[]>([]);
  const [isConnectDialogOpen, setIsConnectDialogOpen] = useState(false);
  const [selectedRepo, setSelectedRepo] = useState<string>("");

  useEffect(() => {
    const storedRepo = localStorage.getItem("github_repo");
    const storedHistory = localStorage.getItem("version_history");
    if (storedRepo) setConnectedRepo(JSON.parse(storedRepo));
    if (storedHistory) setVersionHistory(JSON.parse(storedHistory));
  }, []);

  useEffect(() => {
    if (connectedRepo) {
      localStorage.setItem("github_repo", JSON.stringify(connectedRepo));
    } else {
      localStorage.removeItem("github_repo");
    }
  }, [connectedRepo]);

  useEffect(() => {
    localStorage.setItem("version_history", JSON.stringify(versionHistory));
  }, [versionHistory]);

  const handleConnectRepo = () => {
    const repo = availableRepos.find((r) => r.id === selectedRepo);
    if (repo) {
      setConnectedRepo(repo);
      toast.success(`Connected to ${repo.fullName}`);
      setIsConnectDialogOpen(false);
    }
  };

  const handleDisconnect = () => {
    setConnectedRepo(null);
    toast.info("Disconnected from GitHub repository");
  };

  const handleCommit = () => {
    const newCommit: VersionHistory = {
      id: `v${Date.now()}`,
      commitHash: Math.random().toString(36).substring(2, 9),
      message: "Manual commit via Data Amplifier",
      author: "John Doe",
      timestamp: new Date().toISOString(),
      branch: "main",
      filesChanged: Math.floor(Math.random() * 5) + 1,
    };
    setVersionHistory((current) => [newCommit, ...(current ?? [])]);
    toast.success("Changes committed successfully");
  };

  const formatTimestamp = (dateString: string) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffHours < 24)
      return `${diffHours} hour${diffHours !== 1 ? "s" : ""} ago`;
    if (diffDays < 7) return `${diffDays} day${diffDays !== 1 ? "s" : ""} ago`;
    return date.toLocaleDateString();
  };

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Version Control</h1>
          <p className="text-muted-foreground mt-2">
            Track and manage database schema changes with GitHub
          </p>
        </div>
      </div>

      {connectedRepo ? (
        <div className="grid gap-6 lg:grid-cols-3">
          <div className="lg:col-span-1 space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <GithubLogo weight="fill" className="h-5 w-5" />
                  Connected Repository
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <div className="flex items-start justify-between">
                    <div>
                      <p className="font-semibold">{connectedRepo.name}</p>
                      <p className="text-xs text-muted-foreground">
                        {connectedRepo.fullName}
                      </p>
                    </div>
                    <Badge
                      variant={connectedRepo.private ? "secondary" : "default"}
                      className="text-xs"
                    >
                      {connectedRepo.private ? "Private" : "Public"}
                    </Badge>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    className="w-full gap-2"
                    onClick={() => window.open(connectedRepo.url, "_blank")}
                  >
                    <LinkSimple className="h-4 w-4" />
                    View on GitHub
                  </Button>
                </div>

                <Separator />

                <div className="space-y-3">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">
                      Current Branch
                    </span>
                    <Badge variant="outline" className="gap-1">
                      <GitBranch className="h-3 w-3" />
                      main
                    </Badge>
                  </div>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">Total Commits</span>
                    <span className="font-medium">
                      {(versionHistory ?? []).length}
                    </span>
                  </div>
                </div>

                <Separator />

                <div className="space-y-2">
                  <Button className="w-full gap-2" onClick={handleCommit}>
                    <Plus weight="bold" className="h-4 w-4" />
                    Commit Changes
                  </Button>
                  <Button variant="outline" className="w-full gap-2">
                    <GitPullRequest className="h-4 w-4" />
                    Create Pull Request
                  </Button>
                  <Button
                    variant="outline"
                    className="w-full text-destructive hover:text-destructive"
                    onClick={handleDisconnect}
                  >
                    Disconnect Repository
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Quick Stats</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Files Changed
                  </span>
                  <span className="font-semibold">
                    {(versionHistory ?? []).reduce(
                      (sum, v) => sum + v.filesChanged,
                      0
                    )}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Contributors
                  </span>
                  <span className="font-semibold">3</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Branches
                  </span>
                  <span className="font-semibold">2</span>
                </div>
              </CardContent>
            </Card>
          </div>

          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Commit History</CardTitle>
                    <CardDescription>
                      Recent changes to your database configurations
                    </CardDescription>
                  </div>
                  <Select defaultValue="all">
                    <SelectTrigger className="w-32">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Branches</SelectItem>
                      <SelectItem value="main">main</SelectItem>
                      <SelectItem value="feature">feature/*</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {(versionHistory ?? []).map((commit, index) => (
                    <div key={commit.id}>
                      <div className="flex gap-4">
                        <div className="flex flex-col items-center">
                          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10">
                            <GitCommit className="h-4 w-4 text-primary" />
                          </div>
                          {index < (versionHistory ?? []).length - 1 && (
                            <div className="w-px flex-1 bg-border mt-2" />
                          )}
                        </div>
                        <div className="flex-1 pb-6">
                          <div className="flex items-start justify-between gap-4">
                            <div className="space-y-1 flex-1">
                              <p className="font-medium">{commit.message}</p>
                              <div className="flex items-center gap-3 text-xs text-muted-foreground">
                                <span className="font-mono">
                                  {commit.commitHash}
                                </span>
                                <span>•</span>
                                <span>{commit.author}</span>
                                <span>•</span>
                                <div className="flex items-center gap-1">
                                  <Clock className="h-3 w-3" />
                                  {formatTimestamp(commit.timestamp)}
                                </div>
                              </div>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant="outline" className="gap-1">
                                <GitBranch className="h-3 w-3" />
                                {commit.branch}
                              </Badge>
                              <Badge variant="secondary" className="gap-1">
                                <FileCode className="h-3 w-3" />
                                {commit.filesChanged}
                              </Badge>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      ) : (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-16">
            <div className="flex h-16 w-16 items-center justify-center rounded-full bg-muted mb-4">
              <GithubLogo weight="fill" className="h-8 w-8" />
            </div>
            <h3 className="text-lg font-semibold mb-2">Connect to GitHub</h3>
            <p className="text-sm text-muted-foreground mb-6 text-center max-w-md">
              Link your GitHub repository to track schema changes, collaborate
              with your team, and maintain version history
            </p>
            <Dialog
              open={isConnectDialogOpen}
              onOpenChange={setIsConnectDialogOpen}
            >
              <DialogTrigger asChild>
                <Button className="gap-2">
                  <GithubLogo weight="fill" className="h-4 w-4" />
                  Connect GitHub Repository
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Connect GitHub Repository</DialogTitle>
                  <DialogDescription>
                    Select a repository to track your database configurations
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4 py-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">
                      Select Repository
                    </label>
                    <Select
                      value={selectedRepo}
                      onValueChange={setSelectedRepo}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Choose a repository..." />
                      </SelectTrigger>
                      <SelectContent>
                        {availableRepos.map((repo) => (
                          <SelectItem key={repo.id} value={repo.id}>
                            <div className="flex items-center gap-2">
                              <span>{repo.fullName}</span>
                              {repo.private && (
                                <Badge variant="secondary" className="text-xs">
                                  Private
                                </Badge>
                              )}
                            </div>
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="rounded-lg border bg-muted/50 p-3">
                    <p className="text-xs text-muted-foreground">
                      Data Amplifier will need access to read and write to this
                      repository
                    </p>
                  </div>
                </div>
                <DialogFooter>
                  <Button
                    variant="outline"
                    onClick={() => setIsConnectDialogOpen(false)}
                  >
                    Cancel
                  </Button>
                  <Button onClick={handleConnectRepo} disabled={!selectedRepo}>
                    Connect Repository
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
