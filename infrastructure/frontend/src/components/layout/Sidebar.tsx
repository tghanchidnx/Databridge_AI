import { useState, useEffect } from "react";
import {
  Brain,
  ChartBar,
  Database,
  Gear,
  GitBranch,
  House,
  MagnifyingGlass,
  PushPin,
  Robot,
  SignOut,
  Table,
  Tree,
  TreeStructure,
  BookOpen,
  Play,
  Question,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useAuthStore } from "@/stores/auth.store";
import { cn } from "@/lib/utils";
import logo from "@/assets/logo.png";
import logoIcon from "@/assets/logoIcon.png";

interface SidebarProps {
  activeView: string;
  onViewChange: (view: string) => void;
}

export function Sidebar({ activeView, onViewChange }: SidebarProps) {
  const { logout } = useAuthStore();
  const [isPinned, setIsPinned] = useState(() => {
    const stored = localStorage.getItem("sidebar-pinned");
    return stored ? JSON.parse(stored) : true;
  });
  const [isHovered, setIsHovered] = useState(false);

  // Persist pinned state
  useEffect(() => {
    localStorage.setItem("sidebar-pinned", JSON.stringify(isPinned));
  }, [isPinned]);

  const isExpanded = isPinned || isHovered;

  const navigation = [
    { id: "dashboard", label: "Dashboard", icon: House },
    { id: "schema-matcher", label: "Schema Matcher", icon: Table },
    { id: "report-matcher", label: "Report Matcher", icon: ChartBar },
    {
      id: "hierarchy-knowledge-base",
      label: "Hierarchy KnowledgeBase",
      icon: GitBranch,
    },
    { id: "hierarchy-viewer", label: "Hierarchy Viewer", icon: TreeStructure },
    { id: "ai-config", label: "AI Configuration", icon: Brain },
    { id: "connections", label: "Connections", icon: Database },
    { id: "query-builder", label: "Query Builder", icon: MagnifyingGlass },
    { id: "ai-assistant", label: "AI Assistant", icon: Robot },
    { id: "version-control", label: "Version Control", icon: GitBranch },
  ];

  const helpNavigation = [
    { id: "demo", label: "Feature Demo", icon: Play },
    { id: "docs", label: "Documentation", icon: BookOpen },
  ];

  const NavButton = ({ item }: { item: (typeof navigation)[0] }) => {
    const Icon = item.icon;
    const isActive = activeView === item.id;

    const button = (
      <Button
        variant={isActive ? "secondary" : "ghost"}
        className={cn(
          "w-full justify-start gap-3",
          !isExpanded && "justify-center px-2"
        )}
        onClick={() => onViewChange(item.id)}
      >
        <Icon
          weight={isActive ? "fill" : "regular"}
          className="h-5 w-5 shrink-0"
        />
        {isExpanded && (
          <span className="text-sm text-left flex-1">{item.label}</span>
        )}
      </Button>
    );

    if (!isExpanded) {
      return (
        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>{button}</TooltipTrigger>
            <TooltipContent side="right">{item.label}</TooltipContent>
          </Tooltip>
        </TooltipProvider>
      );
    }

    return button;
  };

  return (
    <div
      className={cn(
        "flex h-full flex-col border-r border-border bg-card transition-all duration-300 ease-in-out relative",
        isExpanded ? "w-64" : "w-16"
      )}
      onMouseEnter={() => !isPinned && setIsHovered(true)}
      onMouseLeave={() => !isPinned && setIsHovered(false)}
    >
      <div
        className={cn(
          "flex h-16 items-center border-b border-border transition-all duration-300",
          isExpanded ? "gap-3 px-6" : "justify-center px-2"
        )}
      >
        <img
          src={isExpanded ? logo : logoIcon}
          alt="Data Amplifier"
          className={isExpanded ? "h-16 shrink-0" : "h-10 shrink-0"}
        />
        {/* {isExpanded && (
          <div className="flex flex-col flex-1 min-w-0">
            <span className="text-sm font-bold tracking-tight text-foreground truncate">
              Data Amplifier
            </span>
            <span className="text-xs text-muted-foreground">v2.0</span>
          </div>
        )} */}
      </div>

      <nav
        className={cn(
          "flex-1 space-y-1 transition-all duration-300",
          isExpanded ? "p-4 pt-4" : "p-2 pt-2"
        )}
      >
        {navigation.map((item) => (
          <NavButton key={item.id} item={item} />
        ))}
      </nav>

      <Separator />

      {/* Help & Documentation Section */}
      <div
        className={cn(
          "space-y-1 transition-all duration-300",
          isExpanded ? "p-4 py-2" : "p-2 py-1"
        )}
      >
        {isExpanded && (
          <span className="text-xs font-medium text-muted-foreground px-2 mb-2 block">
            Help & Resources
          </span>
        )}
        {helpNavigation.map((item) => (
          <NavButton key={item.id} item={item} />
        ))}
      </div>

      <Separator />

      <div
        className={cn(
          "space-y-1 transition-all duration-300",
          isExpanded ? "p-4" : "p-2"
        )}
      >
        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                className={cn(
                  "w-full justify-start gap-3",
                  !isExpanded && "justify-center px-2"
                )}
                onClick={() => setIsPinned((current) => !current)}
              >
                <PushPin
                  weight={isPinned ? "fill" : "regular"}
                  className="h-5 w-5 shrink-0"
                />
                {isExpanded && (
                  <span className="text-sm text-left flex-1">Pin Sidebar</span>
                )}
              </Button>
            </TooltipTrigger>
            {!isExpanded && (
              <TooltipContent side="right">
                {isPinned ? "Unpin Sidebar" : "Pin Sidebar"}
              </TooltipContent>
            )}
          </Tooltip>
        </TooltipProvider>

        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                className={cn(
                  "w-full justify-start gap-3",
                  !isExpanded && "justify-center px-2"
                )}
                onClick={() => onViewChange("settings")}
              >
                <Gear className="h-5 w-5 shrink-0" />
                {isExpanded && (
                  <span className="text-sm text-left flex-1">Settings</span>
                )}
              </Button>
            </TooltipTrigger>
            {!isExpanded && (
              <TooltipContent side="right">Settings</TooltipContent>
            )}
          </Tooltip>
        </TooltipProvider>

        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                className={cn(
                  "w-full justify-start gap-3 text-muted-foreground hover:text-destructive",
                  !isExpanded && "justify-center px-2"
                )}
                onClick={logout}
              >
                <SignOut className="h-5 w-5 shrink-0" />
                {isExpanded && (
                  <span className="text-sm text-left flex-1">Logout</span>
                )}
              </Button>
            </TooltipTrigger>
            {!isExpanded && (
              <TooltipContent side="right">Logout</TooltipContent>
            )}
          </Tooltip>
        </TooltipProvider>
      </div>
    </div>
  );
}
