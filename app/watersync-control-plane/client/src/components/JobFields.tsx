import { useState } from 'react';
import {
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@databricks/appkit-ui/react';
import { defaultSparkVersion } from '../lib/jobCompute';
import type { ComputeMode, NodeTypes, PerformanceTarget } from '../lib/jobCompute';
import type { JobCompute, JobLibrary, JobNotebooks } from '../types';

type GitNotebooks = Extract<JobNotebooks, { source: 'GIT' }>;

// Compute and identity fields shared by the ingestion job dialog and the row check job dialog.

/** The suggestions the node type inputs offer (`list="node-type-options"`); render once per form. */
export function NodeTypeOptions({ nodeTypes }: { nodeTypes: NodeTypes | null }) {
  return (
    <datalist id="node-type-options">
      {nodeTypes?.nodeTypeIds.map((id) => (
        <option key={id} value={id} />
      ))}
    </datalist>
  );
}

export function PerformanceTargetField({
  id,
  value,
  onChange,
  sharedWith,
}: {
  id: string;
  value: PerformanceTarget;
  onChange: (value: PerformanceTarget) => void;
  sharedWith: string | null;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id}>Serverless performance mode</Label>
      <Select value={value} onValueChange={(next) => onChange(next as PerformanceTarget)}>
        <SelectTrigger id={id} className="w-full">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="STANDARD">Standard</SelectItem>
          <SelectItem value="PERFORMANCE_OPTIMIZED">Performance optimized</SelectItem>
        </SelectContent>
      </Select>
      <p className="text-xs text-muted-foreground">
        Standard favors cost efficiency; performance optimized favors faster startup and execution.
        {sharedWith &&
          ` It also applies to ${sharedWith}: Databricks has one serverless performance mode per job, set here.`}
      </p>
    </div>
  );
}

/**
 * Serverless (with a performance mode) or a shared autoscaling job cluster. The cluster fields are named
 * sparkVersion, driverNodeTypeId, workerNodeTypeId, minWorkers and maxWorkers; read them from the form.
 * `saved` prefills them with a job's current cluster; otherwise the node types default to the suggested one.
 */
export function JobComputeFields({
  mode,
  onModeChange,
  performanceTarget,
  onPerformanceTargetChange,
  performanceSharedWith = null,
  nodeTypes,
  saved,
  clusterNote,
}: {
  mode: ComputeMode;
  onModeChange: (mode: ComputeMode) => void;
  performanceTarget: PerformanceTarget;
  onPerformanceTargetChange: (value: PerformanceTarget) => void;
  performanceSharedWith?: string | null;
  nodeTypes: NodeTypes | null;
  saved?: JobCompute | null;
  clusterNote: string;
}) {
  const cluster = saved?.mode === 'JOB_CLUSTER' ? saved : null;
  const suggested = nodeTypes?.defaultNodeTypeId ?? '';
  const placeholder = nodeTypes ? 'For example: i3.xlarge' : 'Loading node types…';
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Label htmlFor="compute-mode">Compute mode</Label>
        <Select value={mode} onValueChange={(value) => onModeChange(value as ComputeMode)}>
          <SelectTrigger id="compute-mode" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="SERVERLESS">Serverless</SelectItem>
            <SelectItem value="JOB_CLUSTER">Shared autoscaling job cluster</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {mode === 'SERVERLESS' && (
        <PerformanceTargetField
          id="performance-target"
          value={performanceTarget}
          onChange={onPerformanceTargetChange}
          sharedWith={performanceSharedWith}
        />
      )}
      {mode === 'JOB_CLUSTER' && (
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2 md:col-span-2">
            <Label htmlFor="spark-version">Databricks Runtime</Label>
            <Input
              id="spark-version"
              name="sparkVersion"
              defaultValue={cluster?.sparkVersion || defaultSparkVersion}
              placeholder={defaultSparkVersion}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="driver-node-type">Driver node type</Label>
            <Input
              key={`driver-${suggested}`}
              id="driver-node-type"
              name="driverNodeTypeId"
              list="node-type-options"
              defaultValue={cluster?.driverNodeTypeId || suggested}
              placeholder={placeholder}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="worker-node-type">Worker node type</Label>
            <Input
              key={`worker-${suggested}`}
              id="worker-node-type"
              name="workerNodeTypeId"
              list="node-type-options"
              defaultValue={cluster?.workerNodeTypeId || suggested}
              placeholder={placeholder}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="min-workers">Minimum workers</Label>
            <Input
              id="min-workers"
              name="minWorkers"
              type="number"
              min="0"
              defaultValue={String(cluster?.minWorkers ?? 1)}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="max-workers">Maximum workers</Label>
            <Input
              id="max-workers"
              name="maxWorkers"
              type="number"
              min="1"
              defaultValue={String(cluster?.maxWorkers ?? 4)}
              required
            />
          </div>
          <p className="text-xs text-muted-foreground md:col-span-2">{clusterNote}</p>
        </div>
      )}
    </div>
  );
}

export function RunAsFields({
  id,
  label,
  mode,
  onModeChange,
  principalName,
  defaultPrincipal = '',
  allowSameAsJob = false,
}: {
  id: string;
  label: string;
  mode: string;
  onModeChange: (value: string) => void;
  principalName: string;
  defaultPrincipal?: string;
  allowSameAsJob?: boolean;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id}>{label}</Label>
      <Select value={mode} onValueChange={onModeChange}>
        <SelectTrigger id={id} className="w-full">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {allowSameAsJob && <SelectItem value="SAME_AS_JOB">Same as job</SelectItem>}
          <SelectItem value="DEFAULT">Owner (app service principal)</SelectItem>
          <SelectItem value="USER">User</SelectItem>
          <SelectItem value="SERVICE_PRINCIPAL">Service principal</SelectItem>
        </SelectContent>
      </Select>
      {(mode === 'USER' || mode === 'SERVICE_PRINCIPAL') && (
        <Input
          key={mode}
          id={`${id}-principal`}
          name={principalName}
          aria-label={mode === 'USER' ? `${label}: user email` : `${label}: service principal application ID`}
          type={mode === 'USER' ? 'email' : 'text'}
          pattern={mode === 'SERVICE_PRINCIPAL' ? '[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}' : undefined}
          title={mode === 'SERVICE_PRINCIPAL' ? 'Service principal application ID (UUID)' : undefined}
          placeholder={
            mode === 'USER' ? 'user@example.com' : 'Application ID, e.g. 00000000-0000-0000-0000-000000000000'
          }
          defaultValue={defaultPrincipal}
          required
        />
      )}
    </div>
  );
}

// The Git hosts whose provider is detected from the URL, as on the server (server/gitSource.ts).
const detectedHost = (url: string) => {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return (
      ['github.com', 'dev.azure.com', 'gitlab.com', 'bitbucket.org'].includes(host) ||
      host.endsWith('.visualstudio.com') ||
      /^git-codecommit\.[a-z0-9-]+\.amazonaws\.com$/.test(host)
    );
  } catch {
    return true; // Not a URL yet: the field's own validation says so.
  }
};
const selfHostedProviders = [
  ['gitHubEnterprise', 'GitHub Enterprise Server'],
  ['gitLabEnterpriseEdition', 'GitLab Self-Managed'],
  ['bitbucketServer', 'Bitbucket Data Center / Server'],
] as const;
const referenceLabels = { BRANCH: 'Branch', TAG: 'Tag', COMMIT: 'Commit hash' } as const;

const defaultNotebooks: GitNotebooks = {
  source: 'GIT',
  gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
  gitProvider: null,
  referenceType: 'BRANCH',
  reference: 'main',
};

/**
 * Where a job's notebooks come from and where its tasks install WaterSync from. Each choice is a select, and only
 * the fields of the chosen option are shown; sourceFromForm (lib/jobSource.ts) reads them back.
 */
export function SourceFields({
  id,
  defaultNotebooks: initialNotebooks = defaultNotebooks,
  defaultLibrary = { source: 'GIT' },
}: {
  id: string;
  defaultNotebooks?: JobNotebooks;
  defaultLibrary?: JobLibrary;
}) {
  const git = initialNotebooks.source === 'GIT' ? initialNotebooks : defaultNotebooks;
  const [notebookSource, setNotebookSource] = useState(initialNotebooks.source);
  const [gitUrl, setGitUrl] = useState(git.gitUrl);
  const [gitProvider, setGitProvider] = useState(git.gitProvider ?? '');
  const [referenceType, setReferenceType] = useState<'BRANCH' | 'TAG' | 'COMMIT'>(git.referenceType);
  const [reference, setReference] = useState(git.reference);
  const [basePath, setBasePath] = useState(initialNotebooks.source === 'WORKSPACE' ? initialNotebooks.basePath : '');
  const [librarySource, setLibrarySource] = useState<JobLibrary['source']>(defaultLibrary.source);
  const [wheelPath, setWheelPath] = useState(defaultLibrary.source === 'WHEEL' ? defaultLibrary.path : '');
  const [requirement, setRequirement] = useState(defaultLibrary.source === 'PIP' ? defaultLibrary.requirement : '');
  const fromGit = notebookSource === 'GIT';
  // The Git repository is only an install option when the notebooks come from one.
  const library = !fromGit && librarySource === 'GIT' ? 'WHEEL' : librarySource;
  const needsProvider = fromGit && !detectedHost(gitUrl);
  const gitRequirement = `watersync@git+${
    /^https?:\/\/(www\.)?github\.com\//i.test(gitUrl) ? `${gitUrl.replace(/\.git$/, '')}.git` : gitUrl
  }@${reference || 'main'}`;
  return (
    <>
      <input type="hidden" name="notebookSource" value={notebookSource} />
      <input type="hidden" name="librarySource" value={library} />
      <div className="space-y-2 md:col-span-2">
        <Label htmlFor={`${id}-notebooks`}>Notebooks from</Label>
        <Select value={notebookSource} onValueChange={(value) => setNotebookSource(value as JobNotebooks['source'])}>
          <SelectTrigger id={`${id}-notebooks`} className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="GIT">Git repository</SelectItem>
            <SelectItem value="WORKSPACE">Workspace folder</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {fromGit ? (
        <>
          <div className="space-y-2 md:col-span-2">
            <Label htmlFor={`${id}-git-url`}>Git repository URL</Label>
            <Input
              id={`${id}-git-url`}
              name="gitUrl"
              type="url"
              value={gitUrl}
              onChange={(event) => setGitUrl(event.target.value)}
              placeholder="https://github.com/org/repo or https://dev.azure.com/org/project/_git/repo"
              required
            />
            <p className="text-xs text-muted-foreground">
              A private repository needs Git credentials for the identity the job runs as.
            </p>
          </div>
          {needsProvider && (
            <div className="space-y-2 md:col-span-2">
              <Label htmlFor={`${id}-git-provider`}>Git provider</Label>
              <Select value={gitProvider} onValueChange={setGitProvider}>
                <SelectTrigger id={`${id}-git-provider`} className="w-full">
                  <SelectValue placeholder="Choose the self-hosted Git server" />
                </SelectTrigger>
                <SelectContent>
                  {selfHostedProviders.map(([value, label]) => (
                    <SelectItem key={value} value={value}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <input type="hidden" name="gitProvider" value={gitProvider} />
            </div>
          )}
          <div className="space-y-2">
            <Label htmlFor={`${id}-git-reference-type`}>Git reference</Label>
            <Select
              value={referenceType}
              onValueChange={(value) => setReferenceType(value as 'BRANCH' | 'TAG' | 'COMMIT')}
            >
              <SelectTrigger id={`${id}-git-reference-type`} className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="BRANCH">Branch</SelectItem>
                <SelectItem value="TAG">Tag</SelectItem>
                <SelectItem value="COMMIT">Commit</SelectItem>
              </SelectContent>
            </Select>
            <input type="hidden" name="gitReferenceType" value={referenceType} />
          </div>
          <div className="space-y-2">
            <Label htmlFor={`${id}-git-reference`}>{referenceLabels[referenceType]}</Label>
            <Input
              id={`${id}-git-reference`}
              name="gitReference"
              value={reference}
              onChange={(event) => setReference(event.target.value)}
              placeholder={referenceType === 'BRANCH' ? 'main' : referenceType === 'TAG' ? 'v0.4.0' : '0123abc'}
              pattern={referenceType === 'COMMIT' ? '[0-9a-fA-F]{7,40}' : '[^\\s"\'`\\\\$]+'}
              required
            />
          </div>
        </>
      ) : (
        <div className="space-y-2 md:col-span-2">
          <Label htmlFor={`${id}-base-path`}>Workspace folder</Label>
          <Input
            id={`${id}-base-path`}
            name="notebookBasePath"
            value={basePath}
            onChange={(event) => setBasePath(event.target.value)}
            placeholder="/Workspace/Users/me@example.com/.bundle/watersync/dev/files"
            pattern={'/[^"\'`\\\\$]+'}
            title="An absolute workspace path, without quotes or backslashes"
            required
          />
          <p className="text-xs text-muted-foreground">
            The folder that holds notebooks/ and src/: a bundle&apos;s deployed files or a Git folder.
          </p>
        </div>
      )}
      <div className="space-y-2 md:col-span-2">
        <Label htmlFor={`${id}-library`}>Install WaterSync from</Label>
        <Select value={library} onValueChange={(value) => setLibrarySource(value as JobLibrary['source'])}>
          <SelectTrigger id={`${id}-library`} className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {fromGit && <SelectItem value="GIT">Git repository</SelectItem>}
            <SelectItem value="WHEEL">Wheel file</SelectItem>
            <SelectItem value="PIP">Pip requirement</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {library === 'GIT' && (
        <div className="space-y-2 md:col-span-2">
          <div className="break-all rounded-md border bg-muted/30 p-3 font-mono text-xs">{gitRequirement}</div>
          <p className="text-xs text-muted-foreground">
            pip reads the repository without credentials, so this only works for a public repository. For a private one,
            use a wheel file or a package index.
          </p>
        </div>
      )}
      {library === 'WHEEL' && (
        <div className="space-y-2 md:col-span-2">
          <Label htmlFor={`${id}-wheel`}>Wheel path</Label>
          <Input
            id={`${id}-wheel`}
            name="libraryWheelPath"
            value={wheelPath}
            onChange={(event) => setWheelPath(event.target.value)}
            pattern="\/(Volumes|Workspace)\/[^\s]+\.whl"
            title="A wheel file in a volume or the workspace"
            placeholder="/Volumes/<catalog>/<schema>/<volume>/watersync-0.4.0-py3-none-any.whl"
            required
          />
          <p className="text-xs text-muted-foreground">
            A wheel in a Unity Catalog volume, or in the workspace, where a bundle deploys the one it builds. Use a
            wheel built from the same version as the notebooks.
          </p>
        </div>
      )}
      {library === 'PIP' && (
        <div className="space-y-2 md:col-span-2">
          <Label htmlFor={`${id}-requirement`}>Pip requirement</Label>
          <Input
            id={`${id}-requirement`}
            name="libraryRequirement"
            value={requirement}
            onChange={(event) => setRequirement(event.target.value)}
            placeholder="watersync==0.4.0"
            required
          />
          <p className="text-xs text-muted-foreground">
            A package from PyPI or the workspace&apos;s default package repository (such as an Azure Artifacts feed), or
            a wheel URL.
          </p>
        </div>
      )}
    </>
  );
}
