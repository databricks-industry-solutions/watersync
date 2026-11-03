import { describe, expect, it, vi } from 'vitest';
import type { jobs } from '@databricks/sdk-experimental';
import { jobsListQuerySchema } from './schemas.js';

// Three jobs: sales at this location, ops at another location (same generated name pattern), no job for hr.
const jobsById: Record<number, jobs.Job> = {
  1: {
    job_id: 1,
    creator_user_name: 'app',
    settings: {
      name: '[sales] Ingestion Pipeline',
      parameters: [{ name: 'configuration_fqn', default: 'dev.watersync.jdbc_ingestion_config' }],
      schedule: { quartz_cron_expression: '0 0 8 * * ?', timezone_id: 'UTC' },
    },
  },
  2: {
    job_id: 2,
    settings: {
      name: '[ops] Ingestion Pipeline',
      parameters: [{ name: 'configuration_fqn', default: 'prod.watersync.jdbc_ingestion_config' }],
    },
  },
};
async function* listed<T>(items: T[]) {
  await Promise.resolve();
  yield* items;
}
const workspace = {
  jobs: {
    list: vi.fn(({ name }: { name: string }) =>
      listed(Object.values(jobsById).filter((job) => job.settings?.name === name))
    ),
    get: vi.fn(({ job_id }: { job_id: number }) => Promise.resolve(jobsById[job_id])),
    listRuns: vi.fn(({ job_id }: { job_id: number }) =>
      listed(
        Array.from({ length: 12 }, (_, i) => ({ run_id: job_id * 100 + i, state: { life_cycle_state: 'TERMINATED' } }))
      )
    ),
  },
};
vi.mock('./sql.js', () => ({ workspace, workspaceBaseUrl: () => 'https://example.cloud.databricks.com' }));

const { listGroupJobs } = await import('./ingestionJobs.js');

describe('listGroupJobs', () => {
  it("returns the location's job of each group with its last 10 runs, and the groups without one", async () => {
    const result = await listGroupJobs('dev', 'watersync', ['sales', 'ops', 'hr']);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      job_id: 1,
      ingestion_group: 'sales',
      settings: { name: '[sales] Ingestion Pipeline', schedule: { timezone_id: 'UTC' } },
      workspace_url: 'https://example.cloud.databricks.com/jobs/1',
    });
    expect(result.jobs[0].runs).toHaveLength(10);
    expect(result.jobs[0].runs[0].run_url).toBe('https://example.cloud.databricks.com/jobs/1/runs/100');
    // ops has a job of that name, but for another location.
    expect(result.missing).toEqual(['ops', 'hr']);
  });

  it('looks jobs up by name only for the groups asked for', async () => {
    workspace.jobs.list.mockClear();
    await listGroupJobs('dev', 'watersync', ['sales']);
    expect(workspace.jobs.list.mock.calls.map(([options]) => options.name)).toEqual(['[sales] Ingestion Pipeline']);
  });
});

describe('jobsListQuerySchema', () => {
  it('takes up to 100 groups', () => {
    expect(jobsListQuerySchema.parse({ catalog: 'dev', schema: 'watersync', group: ['a', 'b'] }).group).toEqual([
      'a',
      'b',
    ]);
    expect(() =>
      jobsListQuerySchema.parse({
        catalog: 'dev',
        schema: 'watersync',
        group: Array.from({ length: 101 }, (_, i) => `g${i}`),
      })
    ).toThrow();
  });
});
