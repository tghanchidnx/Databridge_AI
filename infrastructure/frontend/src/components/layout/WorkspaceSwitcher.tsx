import { Check, Plus, Buildings, CaretUpDown } from '@phosphor-icons/react'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import { useAuth } from '@/contexts/AuthContext'

export function WorkspaceSwitcher() {
  const { currentWorkspace, userWorkspaces, switchWorkspace } = useAuth()

  if (!currentWorkspace || userWorkspaces.length === 0) {
    return null
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button 
          variant="outline" 
          className="w-full justify-between gap-2 h-auto py-3 px-3"
        >
          <div className="flex items-center gap-3 min-w-0 flex-1">
            <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground flex-shrink-0">
              <Buildings className="h-4 w-4" weight="fill" />
            </div>
            <div className="flex flex-col items-start min-w-0 flex-1">
              <span className="text-sm font-medium truncate w-full text-left">
                {currentWorkspace.name}
              </span>
              <span className="text-xs text-muted-foreground truncate w-full text-left">
                {currentWorkspace.plan}
              </span>
            </div>
          </div>
          <CaretUpDown className="h-4 w-4 opacity-50 flex-shrink-0" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-[280px]">
        <DropdownMenuLabel>Workspaces</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {userWorkspaces.map((workspace) => (
          <DropdownMenuItem
            key={workspace.id}
            onClick={() => switchWorkspace(workspace.id)}
            className="cursor-pointer"
          >
            <div className="flex items-center justify-between w-full">
              <div className="flex items-center gap-3 min-w-0 flex-1">
                <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground flex-shrink-0">
                  <Buildings className="h-4 w-4" weight="fill" />
                </div>
                <div className="flex flex-col items-start min-w-0 flex-1">
                  <span className="text-sm font-medium truncate w-full">
                    {workspace.name}
                  </span>
                  <span className="text-xs text-muted-foreground truncate w-full">
                    {workspace.plan}
                  </span>
                </div>
              </div>
              {workspace.id === currentWorkspace.id && (
                <Check className="h-4 w-4 text-primary" weight="bold" />
              )}
            </div>
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem className="cursor-pointer text-primary">
          <Plus className="h-4 w-4 mr-2" weight="bold" />
          Create New Workspace
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
