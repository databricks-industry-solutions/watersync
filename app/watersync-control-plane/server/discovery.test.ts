import { afterEach, describe, expect, it, vi } from 'vitest';

const execute = vi.fn();

vi.mock('./sql.js', () => ({
  execute,
  parameter: vi.fn(),
  sqlLiteral: (value: string) => `'${value.replace(/'/g, "''")}'`,
  workspaceBaseUrl: () => 'https://example.cloud.databricks.com',
  responseRows: (response: { columns: string[]; rows: string[][] }) =>
    response.rows.map((values) => Object.fromEntries(response.columns.map((name, index) => [name, values[index]]))),
}));

vi.mock('./auth.js', () => ({
  appWorkspace: vi.fn(),
  currentUserName: () => Promise.resolve('someone@example.com'),
}));

const { assertUserCanReadSecret } = await import('./discovery.js');

const grants = (...actionTypes: string[]) => ({
  columns: ['Principal', 'ActionType', 'ObjectType', 'ObjectKey'],
  rows: actionTypes.map((actionType) => ['someone@example.com', actionType, 'SECRET', 'main.default.db_pass']),
});

afterEach(() => {
  execute.mockReset();
  vi.restoreAllMocks();
});

describe('assertUserCanReadSecret', () => {
  it('checks the grants of the signed-in user with quoted identifiers', async () => {
    execute.mockResolvedValue(grants('READ SECRET'));

    await assertUserCanReadSecret('main.default.db_pass');

    expect(execute).toHaveBeenCalledWith('SHOW GRANTS `someone@example.com` ON SECRET `main`.`default`.`db_pass`');
  });

  it.each(['READ SECRET', 'ALL PRIVILEGES', 'MANAGE'])('allows %s', async (privilege) => {
    execute.mockResolvedValue(grants('USE SCHEMA', privilege));

    await expect(assertUserCanReadSecret('main.default.db_pass')).resolves.toBeUndefined();
  });

  it('rejects users without a read privilege', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    execute.mockResolvedValue(grants('USE CATALOG', 'REFERENCE SECRET'));

    await expect(assertUserCanReadSecret('main.default.db_pass')).rejects.toThrow(/need READ SECRET/);
  });

  it('fails closed when the access check itself fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    execute.mockRejectedValue(new Error('PERMISSION_DENIED'));

    await expect(assertUserCanReadSecret('main.default.db_pass')).rejects.toThrow(/Could not verify your access/);
  });
});
