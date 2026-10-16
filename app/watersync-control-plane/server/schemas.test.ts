import { describe, expect, it } from 'vitest';
import { sourceDiscoverySchema } from './schemas.js';

const jdbcDiscovery = {
  databaseType: 'postgresql',
  database: 'app',
  jdbcUrl: 'jdbc:postgresql://db.example.com:5432/app',
  jdbcUser: 'reader',
};

const issueMessages = (input: unknown) => {
  const result = sourceDiscoverySchema.safeParse(input);
  return result.success ? [] : result.error.issues.map((issue) => issue.message);
};

describe('sourceDiscoverySchema', () => {
  it('accepts direct JDBC discovery with a secret scope and key', () => {
    expect(issueMessages({ ...jdbcDiscovery, jdbcSecretScope: 'scope', jdbcSecretKey: 'key' })).toEqual([]);
  });

  it('accepts an existing UC connection without JDBC details', () => {
    expect(issueMessages({ databaseType: 'postgresql', connectionName: 'pg_conn' })).toEqual([]);
  });

  it('rejects UC secrets, which would put the password in the statement text', () => {
    expect(issueMessages({ ...jdbcDiscovery, ucSecretName: 'main.default.db_pass' })).toEqual(
      expect.arrayContaining([expect.stringMatching(/UC secrets cannot be used for source discovery/)])
    );
  });

  it('requires a secret scope and key for direct JDBC discovery', () => {
    expect(issueMessages(jdbcDiscovery)).toEqual([
      'Required for direct JDBC discovery',
      'Required for direct JDBC discovery',
    ]);
  });
});
