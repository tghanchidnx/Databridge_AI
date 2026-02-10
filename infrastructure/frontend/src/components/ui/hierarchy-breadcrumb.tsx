/**
 * Hierarchy Breadcrumb Navigation
 * Shows full path to current hierarchy node with clickable navigation
 */
import { cn } from "@/lib/utils";
import { Button } from "./button";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "./breadcrumb";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "./dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./tooltip";
import {
  ChevronRight,
  Home,
  MoreHorizontal,
  Copy,
  Check,
  Layers,
} from "lucide-react";
import { useState, Fragment } from "react";

export interface BreadcrumbNode {
  id: string;
  name: string;
  hierarchyId?: string;
}

interface HierarchyBreadcrumbProps {
  projectName?: string;
  path: BreadcrumbNode[];
  onNavigate: (nodeId: string | null) => void; // null for project root
  maxVisible?: number;
  showCopyPath?: boolean;
  className?: string;
}

export function HierarchyBreadcrumb({
  projectName,
  path,
  onNavigate,
  maxVisible = 4,
  showCopyPath = true,
  className,
}: HierarchyBreadcrumbProps) {
  const [copied, setCopied] = useState(false);

  // Build full path string for copying
  const fullPath = [projectName, ...path.map((n) => n.name)]
    .filter(Boolean)
    .join(" > ");

  const handleCopyPath = async () => {
    try {
      await navigator.clipboard.writeText(fullPath);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error("Failed to copy path:", err);
    }
  };

  // Determine which items to show
  const shouldCollapse = path.length > maxVisible;
  const visibleCount = maxVisible - 1; // -1 for the ellipsis dropdown
  const hiddenItems = shouldCollapse ? path.slice(0, path.length - visibleCount) : [];
  const visibleItems = shouldCollapse ? path.slice(path.length - visibleCount) : path;

  return (
    <div className={cn("flex items-center gap-2", className)}>
      <Breadcrumb>
        <BreadcrumbList>
          {/* Project root */}
          <BreadcrumbItem>
            <BreadcrumbLink
              onClick={() => onNavigate(null)}
              className="flex items-center gap-1 cursor-pointer hover:text-foreground"
            >
              <Home className="h-4 w-4" />
              {projectName && <span className="hidden sm:inline">{projectName}</span>}
            </BreadcrumbLink>
          </BreadcrumbItem>

          {path.length > 0 && <BreadcrumbSeparator />}

          {/* Collapsed items dropdown */}
          {shouldCollapse && hiddenItems.length > 0 && (
            <>
              <BreadcrumbItem>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 w-6 p-0 hover:bg-muted"
                    >
                      <MoreHorizontal className="h-4 w-4" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start">
                    {hiddenItems.map((item) => (
                      <DropdownMenuItem
                        key={item.id}
                        onClick={() => onNavigate(item.id)}
                      >
                        <Layers className="h-4 w-4 mr-2 text-muted-foreground" />
                        {item.name}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
            </>
          )}

          {/* Visible items */}
          {visibleItems.map((item, index) => (
            <Fragment key={item.id}>
              <BreadcrumbItem>
                {index === visibleItems.length - 1 ? (
                  <BreadcrumbPage className="font-medium">
                    {item.name}
                  </BreadcrumbPage>
                ) : (
                  <BreadcrumbLink
                    onClick={() => onNavigate(item.id)}
                    className="cursor-pointer hover:text-foreground max-w-[150px] truncate"
                  >
                    {item.name}
                  </BreadcrumbLink>
                )}
              </BreadcrumbItem>
              {index < visibleItems.length - 1 && <BreadcrumbSeparator />}
            </Fragment>
          ))}
        </BreadcrumbList>
      </Breadcrumb>

      {/* Copy path button */}
      {showCopyPath && path.length > 0 && (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0 text-muted-foreground hover:text-foreground"
                onClick={handleCopyPath}
              >
                {copied ? (
                  <Check className="h-3.5 w-3.5 text-green-600" />
                ) : (
                  <Copy className="h-3.5 w-3.5" />
                )}
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              {copied ? "Copied!" : "Copy path"}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )}
    </div>
  );
}

// Helper hook to build breadcrumb path from hierarchy data
export function useBreadcrumbPath(
  selectedId: string | null,
  hierarchies: Map<string, { id: string; name: string; parentId: string | null }>
): BreadcrumbNode[] {
  if (!selectedId) return [];

  const path: BreadcrumbNode[] = [];
  let currentId: string | null = selectedId;

  while (currentId) {
    const node = hierarchies.get(currentId);
    if (!node) break;

    path.unshift({
      id: node.id,
      name: node.name,
    });

    currentId = node.parentId;
  }

  return path;
}
