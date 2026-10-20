import { useState } from 'react';
import type { ChangeEvent } from 'react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Input,
  Label,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { Download, Upload } from 'lucide-react';
import { api } from '../lib/api';
import { reportError } from '../lib/logging';
import type { ImportResult, Location } from '../types';
import { ErrorState, Toggle } from './shared';

// Both live in the app header: they act on the whole selected location, not on one page.
export function ExportButton({ location }: { location: Location }) {
  const [busy, setBusy] = useState(false);
  const download = async () => {
    setBusy(true);
    try {
      const file = await api<unknown>(`/api/export?${new URLSearchParams(location).toString()}`);
      const url = URL.createObjectURL(new Blob([JSON.stringify(file, null, 2)], { type: 'application/json' }));
      const link = document.createElement('a');
      link.href = url;
      link.download = `watersync-${location.catalog}.${location.schema}-${new Date().toISOString().slice(0, 10)}.json`;
      link.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      // The header has no error area, so report the failure the way other one-off actions do.
      alert(`Export failed: ${reportError('config.export', e, location)}`);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Button
      variant="outline"
      className="context-button"
      disabled={busy}
      onClick={() => void download()}
      title={`Download the configuration of ${location.catalog}.${location.schema} as JSON`}
    >
      <Download className="mr-2 h-4 w-4" /> {busy ? 'Exporting…' : 'Export'}
    </Button>
  );
}

export function ImportButton({ location, onImported }: { location: Location; onImported: () => void }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button
        variant="outline"
        className="context-button"
        onClick={() => setOpen(true)}
        title={`Load a configuration file into ${location.catalog}.${location.schema}`}
      >
        <Upload className="mr-2 h-4 w-4" /> Import
      </Button>
      <ImportDialog open={open} onOpenChange={setOpen} location={location} onImported={onImported} />
    </>
  );
}

function ImportDialog({
  open,
  onOpenChange,
  location,
  onImported,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  location: Location;
  onImported: () => void;
}) {
  const [file, setFile] = useState<unknown>(null);
  const [fileName, setFileName] = useState('');
  const [preview, setPreview] = useState<ImportResult | null>(null);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [replaceGroups, setReplaceGroups] = useState(false);
  const [deployJobs, setDeployJobs] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const reset = () => {
    setFile(null);
    setFileName('');
    setPreview(null);
    setResult(null);
    setReplaceGroups(false);
    setDeployJobs(true);
    setError(null);
  };
  const send = (body: unknown, dryRun: boolean) =>
    api<ImportResult>('/api/import', {
      method: 'POST',
      body: JSON.stringify({ ...location, file: body, replaceGroups, deployJobs, dryRun }),
    });
  // Validate on the server as soon as a file is picked, so problems show before anything is written.
  const choose = async (event: ChangeEvent<HTMLInputElement>) => {
    const picked = event.target.files?.[0];
    reset();
    if (!picked) return;
    setFileName(picked.name);
    setBusy(true);
    try {
      let parsed: unknown;
      try {
        parsed = JSON.parse(await picked.text());
      } catch {
        throw new Error(`${picked.name} is not valid JSON`);
      }
      setPreview(await send(parsed, true));
      setFile(parsed);
    } catch (e) {
      setError(reportError('config.import_validate', e, { ...location, fileName: picked.name }));
    } finally {
      setBusy(false);
    }
  };
  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      setResult(await send(file, false));
      onImported();
    } catch (e) {
      setError(reportError('config.import', e, { ...location, fileName, replaceGroups, deployJobs }));
    } finally {
      setBusy(false);
    }
  };
  const summary = result ?? preview;
  const deploying = deployJobs && Boolean(preview?.jobCount);
  return (
    <Dialog
      open={open}
      onOpenChange={(value) => {
        if (!value) reset();
        onOpenChange(value);
      }}
    >
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>Import configuration</DialogTitle>
          <DialogDescription>
            Load a WaterSync export into {location.catalog}.{location.schema}. Source tables are added or updated by
            ingestion group and source table name.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          {error && <ErrorState message={error} />}
          {!result && (
            <div className="space-y-2">
              <Label htmlFor="import-file">Export file</Label>
              <Input id="import-file" type="file" accept="application/json,.json" onChange={(e) => void choose(e)} />
            </div>
          )}
          {summary && (
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground">
                {result
                  ? `Imported ${result.configCount} source tables${result.removedCount ? `, removed ${result.removedCount} no longer in the file` : ''}.`
                  : `${summary.configCount} source tables in ${summary.groups.length} groups, job settings for ${summary.jobCount} groups.`}
              </p>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Group</TableHead>
                    <TableHead>Source tables</TableHead>
                    <TableHead>Job</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {summary.groups.map((group) => {
                    const job = result?.jobs?.find((item) => item.ingestionGroup === group.ingestionGroup);
                    return (
                      <TableRow key={group.ingestionGroup}>
                        <TableCell>{group.ingestionGroup}</TableCell>
                        <TableCell>{group.configCount}</TableCell>
                        <TableCell>
                          {job?.error ? (
                            <Badge variant="destructive" title={job.error}>
                              Failed
                            </Badge>
                          ) : job ? (
                            <div className="flex flex-wrap gap-1">
                              <Badge>{`Job ${job.jobId} ${job.action}`}</Badge>
                              {Object.entries(job.rowCheckJobs ?? {}).map(([mode, check]) => (
                                <Badge key={mode} variant="outline">{`${mode} row check ${check.action}`}</Badge>
                              ))}
                            </div>
                          ) : group.hasJobSettings ? (
                            <div className="flex flex-wrap gap-1">
                              <Badge variant="secondary">Settings included</Badge>
                              {group.rowChecks.map((mode) => (
                                <Badge key={mode} variant="outline">{`Scheduled ${mode} row check`}</Badge>
                              ))}
                            </div>
                          ) : (
                            <span className="text-muted-foreground">{'—'}</span>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
              {result?.jobs
                ?.filter((job) => job.error)
                .map((job) => (
                  <Alert key={job.ingestionGroup} variant="destructive">
                    <AlertTitle>Job for {job.ingestionGroup} was not saved</AlertTitle>
                    <AlertDescription>{job.error}</AlertDescription>
                  </Alert>
                ))}
            </div>
          )}
          {preview && !result && (
            <div className="grid gap-2">
              <Toggle
                name="replaceGroups"
                label="Replace imported groups: delete their source tables that are not in the file"
                checked={replaceGroups}
                onCheckedChange={setReplaceGroups}
              />
              <Toggle
                name="deployJobs"
                label="Create or update the Databricks job (with its CDC pipeline and scheduled row checks) of each group with job settings"
                checked={deploying}
                disabled={!preview.jobCount}
                onCheckedChange={setDeployJobs}
              />
              <p className="text-xs text-muted-foreground">
                Secret scopes, UC connections, target catalogs and run-as principals referenced in the file must exist
                in this workspace. Watermarks are not imported, so the first run of each table is a full load.
              </p>
            </div>
          )}
        </div>
        <DialogFooter>
          {result ? (
            <Button
              onClick={() => {
                reset();
                onOpenChange(false);
              }}
            >
              Close
            </Button>
          ) : (
            <Button disabled={busy || !preview} onClick={() => void submit()}>
              <Upload className="mr-2 h-4 w-4" />
              {busy ? (preview ? 'Importing…' : 'Validating…') : 'Import'}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
