import React from "react";
import type { HierarchyLevel } from "@/services/api/hierarchy";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Plus, Trash2 } from "lucide-react";

interface LevelConfigEditorProps {
  levels: HierarchyLevel;
  onChange: (levels: HierarchyLevel) => void;
  disabled: boolean;
}

export const LevelConfigEditor: React.FC<LevelConfigEditorProps> = ({
  levels,
  onChange,
  disabled,
}) => {
  // Count active levels
  const activeLevels = Object.keys(levels).filter(
    (k) => levels[k as keyof HierarchyLevel]
  ).length;

  const handleChange = (levelNum: number, value: string) => {
    const key = `level_${levelNum}` as keyof HierarchyLevel;
    onChange({ ...levels, [key]: value || undefined });
  };

  const handleAddLevel = () => {
    // Find first empty level
    for (let i = 1; i <= 15; i++) {
      const key = `level_${i}` as keyof HierarchyLevel;
      if (!levels[key]) {
        onChange({ ...levels, [key]: "" });
        break;
      }
    }
  };

  const handleRemoveLevel = (levelNum: number) => {
    const key = `level_${levelNum}` as keyof HierarchyLevel;
    const updated = { ...levels };
    delete updated[key];
    onChange(updated);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <Label className="text-base font-semibold">
            Hierarchy Levels (2-15)
          </Label>
          <p className="text-sm text-muted-foreground mt-1">
            Currently configured: {activeLevels} levels
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleAddLevel}
          disabled={disabled || activeLevels >= 15}
        >
          <Plus className="w-4 h-4" />
          Add Level
        </Button>
      </div>

      {activeLevels === 0 ? (
        <div className="text-center py-8 text-muted-foreground text-sm bg-card shadow-md rounded-lg ring-1 ring-border/50">
          No levels configured. Click "Add Level" to create one.
        </div>
      ) : (
        <div className="space-y-3 max-w-2xl">
          {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].map(
            (levelNum) => {
              const key = `level_${levelNum}` as keyof HierarchyLevel;
              const value = levels[key];

              if (!value && value !== "") return null;

              return (
                <div key={levelNum} className="flex items-center gap-3">
                  <div className="flex items-center justify-center w-8 h-8 rounded-full bg-primary/10 text-primary text-sm font-medium shrink-0">
                    {levelNum}
                  </div>
                  <div className="flex-1">
                    <Input
                      value={value || ""}
                      onChange={(e) => handleChange(levelNum, e.target.value)}
                      disabled={disabled}
                      placeholder={`Level ${levelNum} name (e.g., BUSINESS_UNIT, DEPARTMENT, etc.)`}
                    />
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleRemoveLevel(levelNum)}
                    disabled={disabled}
                    className="shrink-0"
                  >
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              );
            }
          )}
        </div>
      )}

      <div className="border-t pt-4 mt-6">
        <Label className="text-sm font-medium">Level Naming Guidelines</Label>
        <ul className="text-sm text-muted-foreground mt-2 space-y-1 list-disc list-inside">
          <li>
            Use uppercase with underscores (e.g., BUSINESS_UNIT, DEPARTMENT)
          </li>
          <li>Keep names concise and descriptive</li>
          <li>Minimum 2 levels required, maximum 15 levels supported</li>
          <li>
            Levels should represent a hierarchical structure from top to bottom
          </li>
        </ul>
      </div>
    </div>
  );
};
