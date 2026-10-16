import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Request, Response } from 'express';
import { userContext, workspace } from './auth.js';

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
