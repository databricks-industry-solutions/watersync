import type { JobLibrary, JobNotebooks } from '../types';

/** The `notebooks` and `library` of a job payload, from the fields components/JobFields.tsx SourceFields renders. */
export function sourceFromForm(form: FormData): { notebooks: JobNotebooks; library: JobLibrary } {
  const value = (name: string) => {
    const field = form.get(name);
    return typeof field === 'string' ? field.trim() : '';
  };
  const notebooks: JobNotebooks =
    value('notebookSource') === 'WORKSPACE'
      ? { source: 'WORKSPACE', basePath: value('notebookBasePath') }
      : {
          source: 'GIT',
          gitUrl: value('gitUrl'),
          gitProvider: value('gitProvider') || null,
          referenceType: (value('gitReferenceType') || 'BRANCH') as 'BRANCH' | 'TAG' | 'COMMIT',
          reference: value('gitReference'),
        };
  const source = value('librarySource');
  const library: JobLibrary =
    source === 'WHEEL'
      ? { source: 'WHEEL', path: value('libraryWheelPath') }
      : source === 'PIP'
        ? { source: 'PIP', requirement: value('libraryRequirement') }
        : { source: 'GIT' };
  return { notebooks, library };
}
