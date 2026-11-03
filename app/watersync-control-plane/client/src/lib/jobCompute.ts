import { useEffect, useState } from 'react';
import { api } from './api';

// Compute helpers shared by the ingestion job dialog and the row check job dialog (see components/JobFields).

export type PerformanceTarget = 'STANDARD' | 'PERFORMANCE_OPTIMIZED';
export type ComputeMode = 'SERVERLESS' | 'JOB_CLUSTER';
export type NodeTypes = { nodeTypeIds: string[]; defaultNodeTypeId: string | null };

export const defaultSparkVersion = '18.x-scala2.13';

/** Node types for the compute fields, loaded the first time `enabled` is true. Best effort: empty on failure. */
export function useNodeTypes(enabled: boolean) {
  const [nodeTypes, setNodeTypes] = useState<NodeTypes | null>(null);
  useEffect(() => {
    if (!enabled || nodeTypes) return;
    // Without the list the node type fields stay empty and must be filled in manually.
    api<NodeTypes>('/api/node-types')
      .then(setNodeTypes)
      .catch(() => setNodeTypes({ nodeTypeIds: [], defaultNodeTypeId: null }));
  }, [enabled, nodeTypes]);
  return nodeTypes;
}

/** Compute settings as the job APIs take them, read from a form that renders JobComputeFields. */
export const computeFromForm = (form: FormData, mode: ComputeMode, performanceTarget: PerformanceTarget) => {
  const text = (name: string) => {
    const value = form.get(name);
    return typeof value === 'string' ? value : '';
  };
  return {
    mode,
    performanceTarget,
    sparkVersion: text('sparkVersion'),
    driverNodeTypeId: text('driverNodeTypeId'),
    workerNodeTypeId: text('workerNodeTypeId'),
    minWorkers: Number(form.get('minWorkers') ?? 1),
    maxWorkers: Number(form.get('maxWorkers') ?? 4),
  };
};
