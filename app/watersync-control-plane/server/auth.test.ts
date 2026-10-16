import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Request, Response } from 'express';
import { requiredScopes, scopeStatus, tokenScopes, userContext, workspace } from './auth.js';

const fakeRequest = (headers: Record<string, string> = {}) =>
  ({ method: 'POST', path: '/api/jobs', headers }) as unknown as Request;

const fakeResponse = () => {
  const sent: { status?: number; body?: unknown } = {};
  const response = {
    status(code: number) {
      sent.status = code;
      return response;
    },
    json(body: unknown) {
      sent.body = body;
      return response;
    },
  };
  return { response: response as unknown as Response, sent };
};

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe('userContext', () => {
  it('rejects requests without a forwarded user token in production', () => {
    vi.stubEnv('NODE_ENV', 'production');
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { response, sent } = fakeResponse();
    const next = vi.fn();

    userContext()(fakeRequest(), response, next);

    expect(next).not.toHaveBeenCalled();
    expect(sent.status).toBe(401);
  });

  it('runs the request with a client authenticated by the forwarded user token', async () => {
    vi.stubEnv('NODE_ENV', 'production');
    vi.stubEnv('DATABRICKS_HOST', 'https://example.cloud.databricks.com');
    const { response } = fakeResponse();
    let headers: Headers | undefined;

    await new Promise<void>((resolve, reject) => {
      userContext()(fakeRequest({ 'x-forwarded-access-token': 'user-token' }), response, () => {
        headers = new Headers();
        workspace().config.authenticate(headers).then(resolve, reject);
      });
    });

    expect(headers?.get('Authorization')).toBe('Bearer user-token');
  });

  it('refuses to fall back to the service principal outside a request in production', () => {
    vi.stubEnv('NODE_ENV', 'production');

    expect(() => workspace()).toThrow(/No user context/);
  });
});

const jwt = (claims: Record<string, unknown>) =>
  ['header', Buffer.from(JSON.stringify(claims)).toString('base64url'), 'signature'].join('.');

describe('tokenScopes', () => {
  it('reads a space separated scope claim', () => {
    expect(tokenScopes(jwt({ scope: 'sql jobs offline_access' }))).toEqual(['sql', 'jobs', 'offline_access']);
  });

  it('reads an array scp claim', () => {
    expect(tokenScopes(jwt({ scp: ['sql', 'pipelines'] }))).toEqual(['sql', 'pipelines']);
  });

  it('returns undefined for tokens that are not JWTs', () => {
    expect(tokenScopes('dapi-not-a-jwt')).toBeUndefined();
  });
});

describe('scopeStatus', () => {
  it('flags the required scopes the token does not carry', () => {
    vi.stubEnv('NODE_ENV', 'production');
    const status = scopeStatus(jwt({ scope: 'sql jobs pipelines clusters workspace' }));

    expect(status.mode).toBe('user');
    expect(status.scopes.filter((item) => !item.granted).map((item) => item.scope)).toEqual(['unity-catalog']);
  });

  it('treats all-apis as granting every scope', () => {
    const status = scopeStatus(jwt({ scope: 'all-apis' }));

    expect(status.scopes.every((item) => item.granted)).toBe(true);
  });

  it('reports every scope as missing when user authorization is off in production', () => {
    vi.stubEnv('NODE_ENV', 'production');
    const status = scopeStatus(undefined);

    expect(status.mode).toBe('missing-token');
    expect(status.scopes).toHaveLength(requiredScopes.length);
    expect(status.scopes.every((item) => item.granted === false)).toBe(true);
  });

  it('does not report missing scopes in local development', () => {
    vi.stubEnv('NODE_ENV', 'development');
    const status = scopeStatus(undefined);

    expect(status.mode).toBe('development');
    expect(status.scopes.some((item) => item.granted === false)).toBe(false);
  });
});
