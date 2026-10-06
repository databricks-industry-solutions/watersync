import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Request, Response } from 'express';
import { z } from 'zod';
import { clientLogBatchSchema, logger, respondWithError, serializeError, zodErrorMessage } from './logging.js';

const capture = () => vi.spyOn(console, 'error').mockImplementation(() => undefined);
const lastRecord = (spy: ReturnType<typeof capture>) => {
  const calls = spy.mock.calls;
  return JSON.parse(String(calls[calls.length - 1]?.[0])) as Record<string, unknown>;
};

const fakeRequest = (overrides: Partial<Request> = {}) =>
  ({ method: 'POST', path: '/api/config', headers: {}, ...overrides }) as unknown as Request;

const fakeResponse = () => {
  const sent: { status?: number; body?: unknown } = {};
  const response = {
    headersSent: false,
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
  vi.restoreAllMocks();
});

describe('zodErrorMessage', () => {
  it('reads as a single sentence instead of a JSON blob', () => {
    const schema = z.object({ ingestionGroup: z.string().min(1), fetchSize: z.number() });
    const result = schema.safeParse({ ingestionGroup: '', fetchSize: 'many' });

    expect(zodErrorMessage(result.error as z.ZodError)).toBe(
      'ingestionGroup: Too small: expected string to have >=1 characters; ' +
        'fetchSize: Invalid input: expected number, received string'
    );
  });
});

describe('serializeError', () => {
  it('keeps the stack and any transport status of an Error', () => {
    const error = Object.assign(new Error('Warehouse unavailable'), { status: 503 });

    expect(serializeError(error)).toMatchObject({ name: 'Error', message: 'Warehouse unavailable', status: 503 });
    expect(serializeError(error).stack).toContain('Warehouse unavailable');
  });

  it('follows the cause chain', () => {
    const error = Object.assign(new Error('Save failed'), { cause: new Error('Connection refused') });

    expect(serializeError(error).cause).toMatchObject({ message: 'Connection refused' });
  });

  it('describes values that are not errors', () => {
    expect(serializeError('boom')).toEqual({ name: 'NonError', message: 'boom' });
  });
});

describe('logger', () => {
  it('writes one JSON line per event', () => {
    const spy = capture();

    logger.error('sql.statement_failed', { statementId: 'abc' });

    expect(spy).toHaveBeenCalledTimes(1);
    expect(lastRecord(spy)).toMatchObject({ level: 'error', event: 'sql.statement_failed', statementId: 'abc' });
  });

  it('redacts credentials anywhere in the fields', () => {
    const spy = capture();

    logger.error('config.saved', { jdbcSecretKey: 'super-secret', nested: { password: 'hunter2', catalog: 'main' } });

    expect(lastRecord(spy)).toMatchObject({
      jdbcSecretKey: '[redacted]',
      nested: { password: '[redacted]', catalog: 'main' },
    });
  });

  it('truncates oversized fields rather than flooding the log', () => {
    const spy = capture();

    logger.error('client.error', { message: 'x'.repeat(5000) });

    expect(String(lastRecord(spy).message)).toMatch(/^x{4000}… \[1000 more characters]$/);
  });
});

describe('respondWithError', () => {
  it('answers a validation failure with 400, a readable message, and the request id', () => {
    capture();
    const { response, sent } = fakeResponse();
    const request = fakeRequest({ headers: { 'x-request-id': 'req_from_client' } });

    respondWithError('config_save', request, response, z.object({ a: z.string() }).safeParse({}).error);

    expect(sent.status).toBe(400);
    expect(sent.body).toEqual({
      error: 'a: Invalid input: expected string, received undefined',
      requestId: 'req_from_client',
    });
  });

  it('logs unexpected failures at error level and answers with 500', () => {
    const spy = capture();
    const { response, sent } = fakeResponse();

    respondWithError('config_save', fakeRequest(), response, new Error('Warehouse unavailable'));

    expect(sent.status).toBe(500);
    expect(lastRecord(spy)).toMatchObject({
      level: 'error',
      event: 'config_save.failed',
      method: 'POST',
      path: '/api/config',
      status: 500,
    });
  });
});

describe('clientLogBatchSchema', () => {
  it('accepts a browser batch', () => {
    const batch = clientLogBatchSchema.parse({
      sessionId: 'session-1',
      events: [{ level: 'error', scope: 'config.save', message: 'Request failed (500)' }],
    });

    expect(batch.events[0].scope).toBe('config.save');
  });

  it('rejects a batch large enough to be abusive', () => {
    const event = { level: 'error', scope: 'window.error', message: 'boom' };

    expect(() => clientLogBatchSchema.parse({ events: Array.from({ length: 51 }, () => event) })).toThrow();
  });
});
