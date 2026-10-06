import { useEffect, useState } from 'react';
import { NavLink, Outlet, useOutletContext } from 'react-router';
import {
  Badge,
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  Input,
  Label,
} from '@databricks/appkit-ui/react';
import { Activity, Database, Droplets, Settings2, Workflow } from 'lucide-react';
import { api } from '../lib/api';
import { reportWarning } from '../lib/logging';
import type { Context, Location } from '../types';
import { readLocation, writeLocation } from '../utils';
import { LandingPage } from './LandingPage';

export function Layout() {
  const [location, setLocationState] = useState<Location | null>(readLocation);
  const [revision, setRevision] = useState(0);
  const [draft, setDraft] = useState<Location>(location ?? { catalog: '', schema: '' });
  const setLocation = (next: Location) => {
    setLocationState(next);
    setDraft(next);
    writeLocation(next);
    setRevision((v) => v + 1);
  };
  useEffect(() => {
    if (!location) return;
    api<{ ok: boolean; error?: string }>('/api/ensure-schema', { method: 'POST', body: JSON.stringify(location) })
      .then((result) => {
        if (!result.ok) {
          reportWarning('migrations.ensure_schema_failed', 'Schema migration check failed; tables may not exist yet', {
            detail: result.error?.slice(0, 200) ?? 'Unknown error',
          });
        }
      })
      .catch((error) => {
        reportWarning('migrations.ensure_schema_failed', 'Schema migration check failed; tables may not exist yet', {
          detail: String(error).slice(0, 200),
        });
      });
  }, [location]);
  if (!location) {
    return <LandingPage onSelect={setLocation} />;
  }
  const context: Context = { location, setLocation, revision, refresh: () => setRevision((v) => v + 1) };
  const links = [
    ['/', 'Overview', Activity],
    ['/config', 'Configuration', Settings2],
    ['/watermarks', 'Watermarks', Droplets],
    ['/jobs', 'Jobs', Workflow],
  ] as const;
  return (
    <div className="app-shell min-h-screen bg-background text-foreground">
      <header className="app-header">
        <div className="mx-auto flex max-w-screen-2xl flex-wrap items-center gap-4 px-6 py-4">
          <div className="brand-lockup mr-auto">
            <div className="brand-mark">
              <Droplets className="h-5 w-5" />
            </div>
            <div>
              <div className="brand-name">
                WaterSync <span>Control Plane</span>
              </div>
              <div className="brand-subtitle">JDBC ingestion control plane</div>
            </div>
          </div>
          <Badge variant="outline" className="hidden sm:flex">
            <span className="status-dot" /> Connected
          </Badge>
          <Dialog>
            <DialogTrigger asChild>
              <Button variant="outline" className="context-button">
                <Database className="mr-2 h-4 w-4" />
                <span className="max-w-64 truncate">
                  {location.catalog}.{location.schema}
                </span>
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Metadata location</DialogTitle>
                <DialogDescription>
                  Select the catalog and schema containing the WaterSync configuration and watermark tables.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-4 py-2">
                <div className="space-y-1.5">
                  <Label htmlFor="catalog">Catalog</Label>
                  <Input
                    id="catalog"
                    value={draft.catalog}
                    onChange={(e) => setDraft({ ...draft, catalog: e.target.value })}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="schema">Schema</Label>
                  <Input
                    id="schema"
                    value={draft.schema}
                    onChange={(e) => setDraft({ ...draft, schema: e.target.value })}
                  />
                </div>
              </div>
              <DialogFooter>
                <Button onClick={() => setLocation(draft)} disabled={!draft.catalog || !draft.schema}>
                  Use location
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
        <div className="nav-strip">
          <nav className="mx-auto flex max-w-screen-2xl gap-1 overflow-x-auto px-6">
            {links.map(([to, label, Icon]) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) => `nav-link ${isActive ? 'nav-link-active' : ''}`}
              >
                <Icon className="h-4 w-4" />
                {label}
              </NavLink>
            ))}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-screen-2xl p-6 lg:py-8">
        <Outlet context={context} />
      </main>
    </div>
  );
}

export const useControl = () => useOutletContext<Context>();
