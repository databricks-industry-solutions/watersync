import type { jobs } from '@databricks/sdk-experimental';

// The Git hosts a job's notebooks can come from, by hostname, with the provider Databricks expects in git_source.
// Self-hosted servers (GitHub Enterprise, GitLab EE, Bitbucket Server) can't be told apart by hostname, so their
// provider is chosen by hand (see jobSource.ts).
const providers: [test: (hostname: string) => boolean, provider: jobs.GitProvider][] = [
  [(host) => host === 'github.com', 'gitHub'],
  [(host) => host === 'dev.azure.com' || host.endsWith('.visualstudio.com'), 'azureDevOpsServices'],
  [(host) => host === 'gitlab.com', 'gitLab'],
  [(host) => host === 'bitbucket.org', 'bitbucketCloud'],
  [(host) => /^git-codecommit\.[a-z0-9-]+\.amazonaws\.com$/.test(host), 'awsCodeCommit'],
];

const hostname = (url: string) => {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return '';
  }
};

/** The Databricks git_provider of a repository URL, or undefined for a host WaterSync doesn't recognise. */
export const gitProviderOf = (url: string) => providers.find(([test]) => test(hostname(url)))?.[1];

/**
 * The repository URL without a user name, such as the `org@` Azure DevOps puts in its clone URLs: Databricks
 * reaches the repository with the run-as identity's Git credentials, not with a user in the URL.
 */
export const withoutUserInfo = (url: string) => url.trim().replace(/^(https?:\/\/)[^/@]+@/i, '$1');
