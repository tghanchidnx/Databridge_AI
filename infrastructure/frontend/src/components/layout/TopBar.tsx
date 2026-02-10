import { useState, useEffect, useCallback } from "react";
import {
  Bell,
  CaretDown,
  MagnifyingGlass,
  Moon,
  Sun,
  Sparkle,
  Question,
  BookOpen,
  Play,
  Keyboard,
  Command,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { useTheme } from "@/contexts/ThemeContext";
import type { Notification } from "@/types";
import { Chatbot } from "./Chatbot";
import { getFullAvatarUrl } from "@/lib/avatar-utils";
import { CommandPalette } from "./CommandPalette";

interface TopBarProps {
  onViewChange: (view: string) => void;
}

interface SearchResult {
  id: string;
  title: string;
  type: "connection" | "project" | "schema" | "query" | "page";
  path: string;
  description?: string;
}

export function TopBar({ onViewChange }: TopBarProps) {
  const { user, logout } = useAuthStore();
  const { currentOrganization } = useOrganizationStore();
  const { theme, toggleTheme } = useTheme();
  const [notifications, setNotifications] = useState<Notification[]>(() => {
    const stored = localStorage.getItem("notifications");
    return stored ? JSON.parse(stored) : [];
  });
  const [isNotificationsOpen, setIsNotificationsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [isChatbotOpen, setIsChatbotOpen] = useState(false);
  const [isCommandPaletteOpen, setIsCommandPaletteOpen] = useState(false);
  const [commandPaletteQuery, setCommandPaletteQuery] = useState("");

  // Persist notifications to localStorage
  useEffect(() => {
    localStorage.setItem("notifications", JSON.stringify(notifications));
  }, [notifications]);

  // Keyboard shortcut for command palette (Cmd+K / Ctrl+K)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setIsCommandPaletteOpen(true);
        setCommandPaletteQuery("");
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  const unreadCount = notifications.filter((n) => !n.read).length;

  const searchableContent: SearchResult[] = [
    {
      id: "1",
      title: "Dashboard",
      type: "page",
      path: "dashboard",
      description: "Overview and analytics",
    },
    {
      id: "2",
      title: "Schema Matcher",
      type: "page",
      path: "schema-matcher",
      description: "Compare database schemas",
    },
    {
      id: "3",
      title: "Report Matcher",
      type: "page",
      path: "report-matcher",
      description: "Compare data reports",
    },
    {
      id: "4",
      title: "Connections",
      type: "page",
      path: "connections",
      description: "Manage database connections",
    },
    {
      id: "5",
      title: "Query Builder",
      type: "page",
      path: "query-builder",
      description: "Build and execute queries",
    },
    {
      id: "6",
      title: "AI Assistant",
      type: "page",
      path: "ai-assistant",
      description: "AI-powered database help",
    },
    {
      id: "7",
      title: "Version Control",
      type: "page",
      path: "version-control",
      description: "GitHub integration",
    },
    {
      id: "8",
      title: "Settings",
      type: "page",
      path: "settings",
      description: "Profile and workspace settings",
    },
    {
      id: "9",
      title: "Production Snowflake",
      type: "connection",
      path: "connections",
      description: "Snowflake connection",
    },
    {
      id: "10",
      title: "Staging PostgreSQL",
      type: "connection",
      path: "connections",
      description: "PostgreSQL connection",
    },
    {
      id: "11",
      title: "Documentation",
      type: "page",
      path: "docs",
      description: "User guide and feature documentation",
    },
    {
      id: "12",
      title: "Feature Demo",
      type: "page",
      path: "demo",
      description: "Interactive feature demonstrations",
    },
  ];

  const performSearch = useCallback((query: string) => {
    if (!query || query.length < 2) {
      setSearchResults([]);
      setIsSearchOpen(false);
      return;
    }

    setIsSearching(true);

    setTimeout(() => {
      const filtered = searchableContent.filter(
        (item) =>
          item.title.toLowerCase().includes(query.toLowerCase()) ||
          item.description?.toLowerCase().includes(query.toLowerCase())
      );
      setSearchResults(filtered);
      setIsSearchOpen(filtered.length > 0);
      setIsSearching(false);
    }, 300);
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => {
      performSearch(searchQuery);
    }, 500);

    return () => clearTimeout(timer);
  }, [searchQuery, performSearch]);

  const handleMarkAsRead = (id: string) => {
    setNotifications((current) =>
      current.map((n) => (n.id === id ? { ...n, read: true } : n))
    );
  };

  const handleMarkAllAsRead = () => {
    setNotifications((current) => current.map((n) => ({ ...n, read: true })));
  };

  const handleSearch = (query: string) => {
    setSearchQuery(query);

    // Detect slash commands and open command palette
    if (query.startsWith("/")) {
      setCommandPaletteQuery(query);
      setIsCommandPaletteOpen(true);
      setSearchQuery(""); // Clear the search input
    }
  };

  const handleSearchResultClick = (result: SearchResult) => {
    onViewChange(result.path);
    setIsSearchOpen(false);
    setSearchQuery("");
  };

  const getNotificationIcon = (type: Notification["type"]) => {
    const colors = {
      success: "text-green-500",
      warning: "text-yellow-500",
      error: "text-red-500",
      info: "text-blue-500",
    };
    return (
      <div className={`h-2 w-2 rounded-full ${colors[type]} bg-current`} />
    );
  };

  const formatNotificationTime = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);

    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    return date.toLocaleDateString();
  };

  return (
    <div className="flex h-16 items-center justify-between border-b border-border bg-background px-4 lg:px-8">
      <div className="flex items-center gap-4 lg:gap-6 flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <div className="font-semibold text-lg truncate max-w-[200px]">
            {currentOrganization?.name || "My Organization"}
          </div>
          <Badge variant="secondary" className="capitalize text-xs">
            {currentOrganization?.plan || "free"}
          </Badge>
        </div>

        <div className="relative w-full max-w-md hidden md:block">
          <MagnifyingGlass className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search or type / for commands... (⌘K)"
            className="pl-10 pr-16"
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
            onFocus={() => {
              // Show hint when focused
            }}
          />
          <div className="absolute right-3 top-1/2 -translate-y-1/2 flex items-center gap-1">
            <kbd className="hidden sm:inline-flex h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
              <span className="text-xs">⌘</span>K
            </kbd>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-2 lg:gap-4">
        {/* Help Menu */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="hidden sm:flex">
              <Question className="h-5 w-5" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuLabel>Help & Resources</DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => onViewChange("demo")}>
              <Play className="h-4 w-4 mr-2" />
              Feature Demo
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => onViewChange("docs")}>
              <BookOpen className="h-4 w-4 mr-2" />
              Documentation
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => {
              // Trigger keyboard shortcuts modal (can be implemented via context)
              const event = new KeyboardEvent('keydown', { key: '?' });
              document.dispatchEvent(event);
            }}>
              <Keyboard className="h-4 w-4 mr-2" />
              Keyboard Shortcuts
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => {
              // Reset welcome dialog to show tour again
              localStorage.removeItem("hideWelcomeDialog");
              localStorage.removeItem("lastSeenVersion");
              window.location.reload();
            }}>
              <Play className="h-4 w-4 mr-2" />
              Restart Onboarding Tour
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

        <Button
          variant="ghost"
          size="icon"
          onClick={toggleTheme}
          className="hidden sm:flex"
        >
          {theme === "dark" ? (
            <Sun className="h-5 w-5" />
          ) : (
            <Moon className="h-5 w-5" />
          )}
        </Button>

        <DropdownMenu
          open={isNotificationsOpen}
          onOpenChange={setIsNotificationsOpen}
        >
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="relative">
              <Bell className="h-5 w-5" />
              {unreadCount > 0 && (
                <Badge
                  variant="destructive"
                  className="absolute -right-1 -top-1 h-5 w-5 rounded-full p-0 text-xs flex items-center justify-center"
                >
                  {unreadCount}
                </Badge>
              )}
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-96">
            <div className="flex items-center justify-between p-2">
              <DropdownMenuLabel className="p-0">
                Notifications
              </DropdownMenuLabel>
              {unreadCount > 0 && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-auto p-1 text-xs"
                  onClick={handleMarkAllAsRead}
                >
                  Mark all as read
                </Button>
              )}
            </div>
            <DropdownMenuSeparator />
            <ScrollArea className="max-h-96">
              {(notifications ?? []).length === 0 ? (
                <div className="py-8 text-center text-sm text-muted-foreground">
                  No notifications
                </div>
              ) : (
                <div className="space-y-1">
                  {(notifications ?? []).map((notification) => (
                    <DropdownMenuItem
                      key={notification.id}
                      className="flex gap-3 p-3 cursor-pointer"
                      onClick={() => {
                        handleMarkAsRead(notification.id);
                        if (notification.actionUrl) {
                          onViewChange(notification.actionUrl.replace("/", ""));
                        }
                      }}
                    >
                      {getNotificationIcon(notification.type)}
                      <div className="flex-1 space-y-1">
                        <p
                          className={`text-sm font-medium ${
                            notification.read ? "text-muted-foreground" : ""
                          }`}
                        >
                          {notification.title}
                        </p>
                        <p className="text-xs text-muted-foreground line-clamp-2">
                          {notification.message}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {formatNotificationTime(notification.timestamp)}
                        </p>
                      </div>
                      {!notification.read && (
                        <div className="h-2 w-2 rounded-full bg-primary" />
                      )}
                    </DropdownMenuItem>
                  ))}
                </div>
              )}
            </ScrollArea>
          </DropdownMenuContent>
        </DropdownMenu>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" className="gap-2">
              {user?.avatarUrl ? (
                <img
                  src={getFullAvatarUrl(user.avatarUrl)}
                  alt={user.name}
                  className="h-8 w-8 rounded-full object-cover border border-primary/20"
                  onError={(e) => {
                    // Hide image on error and show initials instead
                    (e.target as HTMLImageElement).style.display = "none";
                  }}
                />
              ) : (
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 border border-primary/20 text-primary font-semibold">
                  {user?.name
                    .split(" ")
                    .map((n) => n[0])
                    .join("")}
                </div>
              )}
              <span className="text-sm hidden lg:inline">{user?.name}</span>
              <CaretDown weight="bold" className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuLabel>
              <div className="flex flex-col gap-1">
                <span>{user?.name}</span>
                <span className="text-xs font-normal text-muted-foreground">
                  {user?.email}
                </span>
              </div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => onViewChange("settings")}>
              Profile Settings
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => onViewChange("settings")}>
              Workspace Settings
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => onViewChange("settings")}>
              Billing
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem className="text-destructive" onClick={logout}>
              Sign Out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <Dialog open={isSearchOpen} onOpenChange={setIsSearchOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Search Results for "{searchQuery}"</DialogTitle>
          </DialogHeader>
          <ScrollArea className="max-h-96">
            {isSearching ? (
              <div className="py-8 text-center text-sm text-muted-foreground">
                Searching...
              </div>
            ) : searchResults.length === 0 ? (
              <div className="py-8 text-center text-sm text-muted-foreground">
                No results found
              </div>
            ) : (
              <div className="space-y-2">
                {searchResults.map((result) => (
                  <Button
                    key={result.id}
                    variant="ghost"
                    className="w-full justify-start h-auto p-4"
                    onClick={() => handleSearchResultClick(result)}
                  >
                    <div className="flex flex-col items-start gap-1 w-full">
                      <div className="flex items-center gap-2 w-full">
                        <span className="font-medium">{result.title}</span>
                        <Badge
                          variant="secondary"
                          className="text-xs capitalize"
                        >
                          {result.type}
                        </Badge>
                      </div>
                      {result.description && (
                        <span className="text-xs text-muted-foreground">
                          {result.description}
                        </span>
                      )}
                    </div>
                  </Button>
                ))}
              </div>
            )}
          </ScrollArea>
        </DialogContent>
      </Dialog>

      {!isChatbotOpen && (
        <Button
          onClick={() => setIsChatbotOpen(true)}
          size="icon"
          className="fixed bottom-4 right-4 h-12 w-12 sm:h-14 sm:w-14 rounded-full shadow-lg z-40 hover:scale-110 transition-transform"
        >
          <Sparkle className="h-5 w-5 sm:h-6 sm:w-6" weight="fill" />
        </Button>
      )}

      {isChatbotOpen && <Chatbot onClose={() => setIsChatbotOpen(false)} />}

      {/* Command Palette */}
      <CommandPalette
        open={isCommandPaletteOpen}
        onOpenChange={setIsCommandPaletteOpen}
        onViewChange={onViewChange}
        initialQuery={commandPaletteQuery}
      />
    </div>
  );
}
