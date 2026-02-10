import { Info } from "@phosphor-icons/react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";

interface PlaceholderViewProps {
  title: string;
  description: string;
  icon: React.ElementType;
  features?: string[];
}

export function PlaceholderView({
  title,
  description,
  icon: Icon,
  features,
}: PlaceholderViewProps) {
  return (
    <div className="space-y-6 px-6 pt-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">{title}</h1>
        <p className="text-muted-foreground mt-2">{description}</p>
      </div>

      <Alert>
        <Info className="h-4 w-4" />
        <AlertDescription>
          This module is under development and will be available in the next
          iteration.
        </AlertDescription>
      </Alert>

      <Card>
        <CardHeader>
          <div className="flex items-center gap-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
              <Icon className="h-6 w-6 text-primary" />
            </div>
            <div>
              <CardTitle>Coming Soon</CardTitle>
              <CardDescription>
                Planned features for this module
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        {features && features.length > 0 && (
          <CardContent>
            <ul className="space-y-2">
              {features.map((feature, index) => (
                <li key={index} className="flex items-start gap-2 text-sm">
                  <span className="text-primary mt-1">•</span>
                  <span>{feature}</span>
                </li>
              ))}
            </ul>
          </CardContent>
        )}
      </Card>
    </div>
  );
}
