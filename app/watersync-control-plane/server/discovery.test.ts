import { afterEach, describe, expect, it, vi } from 'vitest';

const getEffective = vi.fn();

vi.mock('./sql.js', () => ({
  execute: vi.fn(),
  parameter: vi.fn(),
  sqlLiteral: (value: string) => `'${value.replace(/'/g, "''")}'`,
  workspaceBaseUrl: () => 'https://example.cloud.databricks.com',
}));

vi.mock('./auth.js', () => ({
  appWorkspace: () => ({ grants: { getEffective } }),
  currentUserName: () => Promise.resolve('someone@example.com'),
}));

const { assertUserCanReadSecret } = await import('./discovery.js');

// Shape returned by GET /api/2.1/unity-catalog/effective-permissions/secret/{name}?principal=...
const effective = (...assignments: Array<[principal: string, privileges: string[]]>) => ({
  privilege_assignments: assignments.map(([principal, privileges]) => ({
    principal,
    privileges: privileges.map((privilege) => ({ privilege })),
  })),
});

afterEach(() => {
  getEffective.mockReset();
  vi.restoreAllMocks();
});

describe('assertUserCanReadSecret', () => {
  it('asks for the effective permissions of the signed-in user on the secret', async () => {
    getEffective.mockResolvedValue(effective(['someone@example.com', ['READ_SECRET']]));

    await assertUserCanReadSecret('main.default.db_pass');

    expect(getEffective).toHaveBeenCalledWith({
      securable_type: 'secret',
      full_name: 'main.default.db_pass',
      principal: 'someone@example.com',
    });
  });

  it.each(['READ_SECRET', 'ALL_PRIVILEGES', 'MANAGE'])('allows %s', async (privilege) => {
    getEffective.mockResolvedValue(effective(['someone@example.com', ['USE_SCHEMA', privilege]]));

    await expect(assertUserCanReadSecret('main.default.db_pass')).resolves.toBeUndefined();
  });

  it('allows a read privilege held through one of the user’s groups', async () => {
    getEffective.mockResolvedValue(
      effective(['account users', ['READ_METADATA']], ['data-engineers', ['READ_SECRET']])
    );

    await expect(assertUserCanReadSecret('main.default.db_pass')).resolves.toBeUndefined();
  });

  it('rejects users who can only see the secret', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    getEffective.mockResolvedValue(effective(['account users', ['READ_METADATA']]));

    await expect(assertUserCanReadSecret('main.default.db_pass')).rejects.toThrow(/need READ SECRET/);
  });

  it('fails closed when the access check itself fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    getEffective.mockRejectedValue(new Error('PERMISSION_DENIED'));

    await expect(assertUserCanReadSecret('main.default.db_pass')).rejects.toThrow(/Could not verify your access/);
  });
});
