import { z } from 'zod';
import type { jobs } from '@databricks/sdk-experimental';
import { gitProviderOf, withoutUserInfo } from './gitSource.js';

// Where a job's notebooks come from (`notebooks`) and where its tasks and CDC pipeline install WaterSync from
// (`library`). Shared by the ingestion jobs and the row check jobs.

export const gitProviders = [
  'gitHub',
  'gitHubEnterprise',
  'azureDevOpsServices',
  'gitLab',
  'gitLabEnterpriseEdition',
  'bitbucketCloud',
  'bitbucketServer',
  'awsCodeCommit',
] as const;

// Git references and pip requirements end up in a pip requirement and in a %pip line of the classic pipeline.
const noQuotes = /^[^"'`\\$\n]+$/;
const pipToken = /^[^\s"'`\\$]+$/;

const gitNotebooksSchema = z.object({
  source: z.literal('GIT'),
  // A user name in the URL (the org@ of Azure DevOps clone URLs) is dropped: Git credentials come from Databricks.
  gitUrl: z.preprocess((value) => (typeof value === 'string' ? withoutUserInfo(value) : value), z.url()),
  // Null means detected from the URL; self-hosted servers need it set.
  gitProvider: z
    .enum(gitProviders)
    .nullish()
    .transform((value) => value ?? null),
  referenceType: z.enum(['BRANCH', 'TAG', 'COMMIT']).default('BRANCH'),
  reference: z.string().trim().min(1).regex(pipToken, 'Must not contain whitespace, quotes or backslashes'),
});
const workspaceNotebooksSchema = z.object({
  source: z.literal('WORKSPACE'),
  // The folder that holds notebooks/ and src/: a bundle's files folder or a Git folder.
  basePath: z
    .string()
    .trim()
    .transform((value) => value.replace(/\/+$/, ''))
    .pipe(z.string().regex(/^\/[^"'`\\$\n]+$/, 'Use an absolute workspace path, e.g. /Workspace/Users/me/watersync')),
});
export const notebooksSchema = z.discriminatedUnion('source', [gitNotebooksSchema, workspaceNotebooksSchema]);

export const librarySchema = z.discriminatedUnion('source', [
  // The package from the same repository and reference as the notebooks; pip reads it anonymously.
  z.object({ source: z.literal('GIT') }),
  z.object({
    source: z.literal('WHEEL'),
    path: z
      .string()
      .trim()
      .regex(
        /^\/(Volumes\/[^/\s"'`\\$]+\/[^/\s"'`\\$]+\/[^/\s"'`\\$]+|Workspace)\/[^\s"'`\\$]*[^/\s"'`\\$]\.whl$/,
        'Use a wheel file in a volume (/Volumes/<catalog>/<schema>/<volume>/<file>.whl) or the workspace (/Workspace/.../<file>.whl)'
      ),
  }),
  // Any pip requirement: watersync==0.4.0 from PyPI or the workspace's package repository, or a wheel URL.
  z.object({
    source: z.literal('PIP'),
    requirement: z.string().trim().min(1).regex(noQuotes, 'Must not contain quotes, backslashes or $'),
  }),
]);

export type Notebooks = z.infer<typeof notebooksSchema>;
export type Library = z.infer<typeof librarySchema>;

/**
 * Maps the fields of export files written before `notebooks` and `library` existed: `gitUrl` and `gitBranch` for
 * the Git source, and `libraryPath` for a volume wheel.
 */
export const withLegacySource = (value: unknown) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return value;
  const { gitUrl, gitBranch, libraryPath, ...rest } = value as Record<string, unknown>;
  const legacyWheel = typeof libraryPath === 'string' && libraryPath.trim() ? libraryPath : null;
  return {
    ...rest,
    notebooks:
      rest.notebooks ??
      (gitUrl === undefined ? undefined : { source: 'GIT', gitUrl, referenceType: 'BRANCH', reference: gitBranch }),
    library: rest.library ?? (legacyWheel ? { source: 'WHEEL', path: legacyWheel } : { source: 'GIT' }),
  };
};

/** The checks that span both settings, for a job payload schema's superRefine. */
export const checkJobSource = (value: { notebooks: Notebooks; library: Library }, context: z.core.$RefinementCtx) => {
  const { notebooks, library } = value;
  if (notebooks.source === 'GIT') {
    if (!notebooks.gitProvider && !gitProviderOf(notebooks.gitUrl)) {
      context.addIssue({
        code: 'custom',
        path: ['notebooks', 'gitProvider'],
        message:
          'Choose the Git provider: it is only detected for github.com, dev.azure.com, gitlab.com, bitbucket.org and AWS CodeCommit',
      });
    }
    if (notebooks.referenceType === 'COMMIT' && !/^[0-9a-f]{7,40}$/i.test(notebooks.reference)) {
      context.addIssue({ code: 'custom', path: ['notebooks', 'reference'], message: 'Use a commit hash' });
    }
  } else if (library.source === 'GIT') {
    context.addIssue({
      code: 'custom',
      path: ['library', 'source'],
      message: 'Installing from the Git repository needs notebooks from a Git repository',
    });
  }
};

const gitProviderFor = (notebooks: Extract<Notebooks, { source: 'GIT' }>) =>
  notebooks.gitProvider ?? gitProviderOf(notebooks.gitUrl) ?? 'gitHub';

/** The job's git_source, or undefined for notebooks in the workspace. */
export function gitSource(notebooks: Notebooks): jobs.GitSource | undefined {
  if (notebooks.source !== 'GIT') return undefined;
  const reference =
    notebooks.referenceType === 'TAG'
      ? { git_tag: notebooks.reference }
      : notebooks.referenceType === 'COMMIT'
        ? { git_commit: notebooks.reference }
        : { git_branch: notebooks.reference };
  return { git_url: notebooks.gitUrl, git_provider: gitProviderFor(notebooks), ...reference };
}

/** A notebook task's path and source; `relativePath` is the notebook's path in the repository. */
export const notebookTask = (notebooks: Notebooks, relativePath: string) =>
  notebooks.source === 'GIT'
    ? { notebook_path: relativePath, source: 'GIT' as const }
    : { notebook_path: `${notebooks.basePath}/${relativePath}`, source: 'WORKSPACE' as const };

/** The pip requirement every task and the CDC pipeline install. */
export function watersyncRequirement(notebooks: Notebooks, library: Library) {
  if (library.source === 'WHEEL') return library.path;
  if (library.source === 'PIP') return library.requirement;
  if (notebooks.source !== 'GIT') throw new Error('Installing from Git needs notebooks from a Git repository');
  // GitHub URLs keep the .git suffix WaterSync always used; Azure DevOps and CodeCommit URLs are used as they are.
  const url = ['gitHub', 'gitHubEnterprise'].includes(gitProviderFor(notebooks))
    ? `${notebooks.gitUrl.replace(/\.git$/, '')}.git`
    : notebooks.gitUrl;
  return `watersync@git+${url}@${notebooks.reference}`;
}

/** Whether a requirement is a wheel file in a volume or the workspace, rather than a pip requirement. */
export const isWheelFile = (requirement: string) =>
  /^\/(Volumes|Workspace)\//.test(requirement) && requirement.endsWith('.whl');

/** The notebooks a job runs, read back from its settings; `task` is one of its notebook tasks. */
export function readNotebooks(
  settings: jobs.JobSettings | undefined,
  task: { notebook_path?: string; source?: string } | undefined,
  relativePath: string
): Notebooks {
  const git = settings?.git_source;
  if (git?.git_url) {
    // Only a provider the URL doesn't imply was chosen by hand.
    const provider = gitProviders.find((item) => item === git.git_provider);
    return {
      source: 'GIT',
      gitUrl: git.git_url,
      gitProvider: provider && provider !== gitProviderOf(git.git_url) ? provider : null,
      referenceType: git.git_tag ? 'TAG' : git.git_commit ? 'COMMIT' : 'BRANCH',
      reference: git.git_tag ?? git.git_commit ?? git.git_branch ?? 'main',
    };
  }
  const path = task?.notebook_path ?? '';
  const basePath = path.endsWith(`/${relativePath}`) ? path.slice(0, -relativePath.length - 1) : path;
  return { source: 'WORKSPACE', basePath };
}

/** Where a job installs WaterSync from, read back from the requirement its tasks install. */
export function readLibrary(requirement: string | undefined): Library {
  if (!requirement || requirement.startsWith('watersync@git+')) return { source: 'GIT' };
  return isWheelFile(requirement) ? { source: 'WHEEL', path: requirement } : { source: 'PIP', requirement };
}
