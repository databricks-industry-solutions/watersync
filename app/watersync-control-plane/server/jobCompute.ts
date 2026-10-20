import type { jobs } from '@databricks/sdk-experimental';
import type { JobCompute } from './schemas.js';
import { workspace } from './sql.js';

// Shared by the ingestion jobs and the row check job, so both offer and read back the same compute and identity.
export const environmentKey = 'watersync_environment';
export const jobClusterKey = 'watersync_cluster';

/**
 * What a job needs for the chosen compute: `task` goes on every task, `settings` on the job. Serverless tasks
 * install the package through the job environment; job cluster tasks install it as a library.
 */
export function jobCompute(compute: JobCompute, dependency: string) {
  if (compute.mode === 'SERVERLESS') {
    return {
      task: { environment_key: environmentKey },
      settings: {
        environments: [
          { environment_key: environmentKey, spec: { environment_version: '4', dependencies: [dependency] } },
        ],
      },
    };
  }
  return {
    task: { job_cluster_key: jobClusterKey, libraries: [{ pypi: { package: dependency } }] },
    settings: {
      job_clusters: [
        {
          job_cluster_key: jobClusterKey,
          new_cluster: {
            spark_version: compute.sparkVersion,
            driver_node_type_id: compute.driverNodeTypeId,
            node_type_id: compute.workerNodeTypeId,
            autoscale: { min_workers: compute.minWorkers, max_workers: compute.maxWorkers },
          },
        },
      ],
    },
  };
}

/** The compute a job was generated with, read back from its settings. */
export function readJobCompute(settings: jobs.JobSettings | undefined): JobCompute {
  const cluster = settings?.job_clusters?.find((item) => item.job_cluster_key === jobClusterKey)?.new_cluster;
  const performanceTarget = settings?.performance_target ?? 'STANDARD';
  return cluster
    ? {
        mode: 'JOB_CLUSTER',
        performanceTarget,
        sparkVersion: cluster.spark_version ?? '',
        driverNodeTypeId: cluster.driver_node_type_id ?? cluster.node_type_id ?? '',
        workerNodeTypeId: cluster.node_type_id ?? '',
        minWorkers: cluster.autoscale?.min_workers ?? cluster.num_workers ?? 1,
        maxWorkers: cluster.autoscale?.max_workers ?? Math.max(cluster.num_workers ?? 1, 1),
      }
    : {
        mode: 'SERVERLESS',
        performanceTarget,
        sparkVersion: '',
        driverNodeTypeId: '',
        workerNodeTypeId: '',
        minWorkers: 1,
        maxWorkers: 4,
      };
}

const servicePrincipalId = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const runAsIdentity = (runAs?: { user_name?: string; service_principal_name?: string }) =>
  runAs?.user_name ?? runAs?.service_principal_name;
// The app service principal creates (and so owns) every generated resource; running as it is what DEFAULT means.
export const runAsTarget = (identity: string | undefined, creator: string | undefined) =>
  !identity || identity === creator
    ? ({ mode: 'DEFAULT', principal: '' } as const)
    : servicePrincipalId.test(identity)
      ? ({ mode: 'SERVICE_PRINCIPAL', principal: identity } as const)
      : ({ mode: 'USER', principal: identity } as const);

/**
 * A job WaterSync generated for a location, found by its name: only jobs whose `configuration_fqn` parameter
 * points at this location's config table count, so a job of the same name for another location is ignored.
 */
export async function findLocationJob(name: string, configurationFqn: string) {
  for await (const listed of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
    if (listed.settings?.name !== name || !listed.job_id) continue;
    const job = await workspace.jobs.get({ job_id: listed.job_id });
    const parameters = new Map((job.settings?.parameters ?? []).map((item) => [item.name, item.default]));
    if (parameters.get('configuration_fqn') === configurationFqn) return job;
  }
  return undefined;
}

/** Creates the job, or resets the existing one in place, and says which. */
export async function upsertJob(existingJobId: number | undefined, settings: jobs.JobSettings) {
  if (existingJobId) {
    await workspace.jobs.reset({ job_id: existingJobId, new_settings: settings });
    return { jobId: existingJobId, action: 'updated' as const };
  }
  return { jobId: (await workspace.jobs.create(settings as jobs.CreateJob)).job_id, action: 'created' as const };
}
