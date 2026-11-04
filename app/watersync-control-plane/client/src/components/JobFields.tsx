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
import type { JobCompute } from '../types';

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
