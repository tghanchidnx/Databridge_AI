/**
 * Empty State Components
 * Provides beautiful empty states with illustrations and CTAs
 */
import { cn } from "@/lib/utils";
import { Button } from "./button";
import {
  FolderOpen,
  Database,
  GitBranch,
  FileSpreadsheet,
  Search,
  Users,
  Settings,
  Layers,
  FileCode,
  AlertCircle,
  Plus,
  Upload,
  RefreshCw,
  type LucideIcon,
} from "lucide-react";

interface EmptyStateProps {
  icon?: LucideIcon;
  title: string;
  description: string;
  action?: {
    label: string;
    onClick: () => void;
    icon?: LucideIcon;
    variant?: "default" | "outline" | "secondary";
  };
  secondaryAction?: {
    label: string;
    onClick: () => void;
  };
  className?: string;
  size?: "sm" | "md" | "lg";
}

export function EmptyState({
  icon: Icon = FolderOpen,
  title,
  description,
  action,
  secondaryAction,
  className,
  size = "md",
}: EmptyStateProps) {
  const sizeClasses = {
    sm: {
      container: "py-8",
      icon: "h-10 w-10",
      iconWrapper: "h-16 w-16",
      title: "text-base",
      description: "text-sm",
    },
    md: {
      container: "py-12",
      icon: "h-12 w-12",
      iconWrapper: "h-20 w-20",
      title: "text-lg",
      description: "text-sm",
    },
    lg: {
      container: "py-16",
      icon: "h-16 w-16",
      iconWrapper: "h-24 w-24",
      title: "text-xl",
      description: "text-base",
    },
  };

  const sizes = sizeClasses[size];

  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center text-center",
        sizes.container,
        className
      )}
    >
      <div
        className={cn(
          "flex items-center justify-center rounded-full bg-muted/50 mb-4",
          sizes.iconWrapper
        )}
      >
        <Icon className={cn("text-muted-foreground/60", sizes.icon)} />
      </div>
      <h3 className={cn("font-semibold text-foreground mb-2", sizes.title)}>
        {title}
      </h3>
      <p
        className={cn(
          "text-muted-foreground max-w-sm mb-6",
          sizes.description
        )}
      >
        {description}
      </p>
      {(action || secondaryAction) && (
        <div className="flex items-center gap-3">
          {action && (
            <Button
              onClick={action.onClick}
              variant={action.variant || "default"}
              size={size === "sm" ? "sm" : "default"}
            >
              {action.icon && <action.icon className="h-4 w-4 mr-2" />}
              {action.label}
            </Button>
          )}
          {secondaryAction && (
            <Button
              onClick={secondaryAction.onClick}
              variant="ghost"
              size={size === "sm" ? "sm" : "default"}
            >
              {secondaryAction.label}
            </Button>
          )}
        </div>
      )}
    </div>
  );
}

// Pre-configured empty states for common scenarios
export function NoProjectsEmpty({ onCreate }: { onCreate: () => void }) {
  return (
    <EmptyState
      icon={Layers}
      title="No projects yet"
      description="Create your first hierarchy project to start organizing and mapping your data structures."
      action={{
        label: "Create Project",
        onClick: onCreate,
        icon: Plus,
      }}
    />
  );
}

export function NoHierarchiesEmpty({ onCreate }: { onCreate: () => void }) {
  return (
    <EmptyState
      icon={GitBranch}
      title="No hierarchies defined"
      description="Start building your hierarchy structure by adding the first hierarchy node."
      action={{
        label: "Add Hierarchy",
        onClick: onCreate,
        icon: Plus,
      }}
    />
  );
}

export function NoMappingsEmpty({ onAdd }: { onAdd: () => void }) {
  return (
    <EmptyState
      icon={Database}
      title="No source mappings"
      description="Connect this hierarchy node to your data sources by adding source mappings."
      action={{
        label: "Add Mapping",
        onClick: onAdd,
        icon: Plus,
      }}
      size="sm"
    />
  );
}

export function NoConnectionsEmpty({ onCreate }: { onCreate: () => void }) {
  return (
    <EmptyState
      icon={Database}
      title="No database connections"
      description="Connect to your databases to start comparing schemas and mapping data."
      action={{
        label: "Add Connection",
        onClick: onCreate,
        icon: Plus,
      }}
    />
  );
}

export function NoSearchResultsEmpty({ onClear }: { onClear: () => void }) {
  return (
    <EmptyState
      icon={Search}
      title="No results found"
      description="Try adjusting your search terms or filters to find what you're looking for."
      action={{
        label: "Clear Search",
        onClick: onClear,
        icon: RefreshCw,
        variant: "outline",
      }}
      size="sm"
    />
  );
}

export function NoMembersEmpty({ onInvite }: { onInvite: () => void }) {
  return (
    <EmptyState
      icon={Users}
      title="No team members"
      description="Invite team members to collaborate on this project."
      action={{
        label: "Invite Members",
        onClick: onInvite,
        icon: Plus,
      }}
      size="sm"
    />
  );
}

export function NoFormulasEmpty({ onCreate }: { onCreate: () => void }) {
  return (
    <EmptyState
      icon={FileCode}
      title="No formulas defined"
      description="Create calculation formulas to define how hierarchy values are computed."
      action={{
        label: "Create Formula",
        onClick: onCreate,
        icon: Plus,
      }}
      size="sm"
    />
  );
}

export function ImportFileEmpty({ onUpload }: { onUpload: () => void }) {
  return (
    <EmptyState
      icon={FileSpreadsheet}
      title="Import from CSV"
      description="Upload your hierarchy and mapping CSV files to import existing structures."
      action={{
        label: "Upload Files",
        onClick: onUpload,
        icon: Upload,
      }}
    />
  );
}

export function ErrorEmpty({
  message,
  onRetry,
}: {
  message?: string;
  onRetry?: () => void;
}) {
  return (
    <EmptyState
      icon={AlertCircle}
      title="Something went wrong"
      description={message || "An error occurred while loading the data. Please try again."}
      action={
        onRetry
          ? {
              label: "Try Again",
              onClick: onRetry,
              icon: RefreshCw,
              variant: "outline",
            }
          : undefined
      }
    />
  );
}

export function SelectItemEmpty({ itemType = "item" }: { itemType?: string }) {
  return (
    <EmptyState
      icon={Settings}
      title={`Select a ${itemType}`}
      description={`Choose a ${itemType} from the list to view and edit its details.`}
      size="sm"
    />
  );
}

export function ComingSoonEmpty({ feature }: { feature: string }) {
  return (
    <EmptyState
      icon={Layers}
      title="Coming Soon"
      description={`${feature} is currently in development and will be available soon.`}
      size="sm"
    />
  );
}
