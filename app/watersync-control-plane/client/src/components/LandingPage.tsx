import { useState } from 'react';
import type { FormEvent } from 'react';
import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Input,
  Label,
} from '@databricks/appkit-ui/react';
import { Droplets } from 'lucide-react';
import type { Location } from '../types';

export function LandingPage({ onSelect }: { onSelect: (location: Location) => void }) {
  const [catalog, setCatalog] = useState('');
  const [schema, setSchema] = useState('');
  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (catalog.trim() && schema.trim()) onSelect({ catalog: catalog.trim(), schema: schema.trim() });
  };
  return (
    <div className="min-h-screen bg-background text-foreground flex items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardHeader>
          <div className="brand-mark mb-4 flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
            <Droplets className="h-5 w-5 text-primary" />
          </div>
          <CardTitle>WaterSync Control Plane</CardTitle>
          <CardDescription>
            Select the catalog and schema containing your JDBC ingestion configuration and watermark tables.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-1.5">
              <Label htmlFor="landing-catalog">Catalog</Label>
              <Input
                id="landing-catalog"
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
                placeholder="e.g. main"
                autoFocus
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="landing-schema">Schema</Label>
              <Input
                id="landing-schema"
                value={schema}
                onChange={(e) => setSchema(e.target.value)}
                placeholder="e.g. watersync"
              />
            </div>
            <Button type="submit" className="w-full" disabled={!catalog.trim() || !schema.trim()}>
              Continue
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
