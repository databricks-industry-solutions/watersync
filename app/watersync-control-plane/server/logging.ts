import { randomUUID } from 'node:crypto';
import type { Request, RequestHandler, Response } from 'express';
import { z } from 'zod';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LogFields = Record<string, unknown>;

const levelWeight: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
const configuredLevel = (process.env.WATERSYNC_LOG_LEVEL ?? 'info').toLowerCase();
const threshold = levelWeight[configuredLevel as LogLevel] ?? levelWeight.info;

const redactedKey = /secret|password|passwd|pwd|token|credential|authorization|cookie/i;
const maxFieldLength = 4000;
const maxStackLength = 8000;

const truncate = (value: string, limit = maxFieldLength) =>
  value.length > limit ? `${value.slice(0, limit)}… [${value.length - limit} more characters]` : value;

const zodIssues = (error: z.ZodError) =>
  error.issues.map((issue) => ({
    path: issue.path.join('.'),
    code: issue.code,
    message: issue.message,
  }));

/** Turn a Zod failure into one readable line instead of the default JSON blob. */
export const zodErrorMessage = (error: z.ZodError) =>
  zodIssues(error)
    .map((issue) => (issue.path ? `${issue.path}: ${issue.message}` : issue.message))
    .join('; ') || 'Invalid request';

const numericProperty = (error: object, key: string) => {
  const value = (error as Record<string, unknown>)[key];
  return typeof value === 'number' || typeof value === 'string' ? { [key]: value } : {};
};

// `Error.cause` predates the ES2020 lib this server compiles against.
const causeOf = (error: Error) => ('cause' in error ? (error as { cause?: unknown }).cause : undefined);

export const serializeError = (error: unknown): LogFields => {
  if (error instanceof z.ZodError) {
    return { name: 'ZodError', message: zodErrorMessage(error), issues: zodIssues(error) };
  }
  if (error instanceof Error) {
    const cause = causeOf(error);
    return {
      name: error.name,
      message: truncate(error.message),
      ...(error.stack ? { stack: truncate(error.stack, maxStackLength) } : {}),
      ...numericProperty(error, 'status'),
      ...numericProperty(error, 'statusCode'),
      ...numericProperty(error, 'code'),
      ...(cause == null ? {} : { cause: serializeError(cause) }),
    };
  }
  return { name: 'NonError', message: truncate(String(error)) };
};

export const errorMessage = (error: unknown) => {
  if (error instanceof z.ZodError) return zodErrorMessage(error);
  return error instanceof Error ? error.message : 'Unexpected error';
};

/** Drop secrets, flatten errors, and bound the size of anything we hand to JSON.stringify. */
const sanitize = (value: unknown, depth = 0): unknown => {
  if (typeof value === 'string') return truncate(value);
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Error) return depth >= 4 ? value.message : serializeError(value);
  if (value === null || typeof value !== 'object') return value;
  if (depth >= 4) return '[truncated]';
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => sanitize(item, depth + 1));
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [
      key,
      redactedKey.test(key) ? '[redacted]' : sanitize(item, depth + 1),
    ])
  );
};

const emit = (level: LogLevel, event: string, fields: LogFields = {}) => {
  if (levelWeight[level] < threshold) return;
  const record = { timestamp: new Date().toISOString(), level, event, ...(sanitize(fields) as LogFields) };
  let line: string;
  try {
    line = JSON.stringify(record);
  } catch {
    line = JSON.stringify({ timestamp: record.timestamp, level, event, message: 'Log fields were not serializable' });
  }
  // Databricks Apps collect stdout and stderr, so writing there is what makes an entry show up in app logs.
  if (level === 'error' || level === 'warn') console.error(line);
  else console.log(line);
};

export const logger = {
  debug: (event: string, fields?: LogFields) => emit('debug', event, fields),
  info: (event: string, fields?: LogFields) => emit('info', event, fields),
  warn: (event: string, fields?: LogFields) => emit('warn', event, fields),
  error: (event: string, fields?: LogFields) => emit('error', event, fields),
};

type RequestState = { id: string; startedAt: number };
const requestStates = new WeakMap<Request, RequestState>();
const idPattern = /[^A-Za-z0-9_.-]/g;

const incomingRequestId = (req: Request) => {
  const header = req.headers['x-request-id'] ?? req.headers['x-correlation-id'];
  const value = Array.isArray(header) ? header[0] : header;
  const sanitized = value?.replace(idPattern, '').slice(0, 64);
  return sanitized || undefined;
};

const requestState = (req: Request): RequestState => {
  const existing = requestStates.get(req);
  if (existing) return existing;
  const created = { id: incomingRequestId(req) ?? `req_${randomUUID()}`, startedAt: Date.now() };
  requestStates.set(req, created);
  return created;
};

export const requestId = (req: Request) => requestState(req).id;

const headerValue = (req: Request, name: string) => {
  const header = req.headers[name];
  const value = Array.isArray(header) ? header[0] : header;
  return value ? value.slice(0, 128) : undefined;
};

const requestFields = (req: Request): LogFields => ({
  requestId: requestId(req),
  method: req.method,
  path: req.path,
  user: headerValue(req, 'x-forwarded-email') ?? headerValue(req, 'x-forwarded-user'),
});

/**
 * Assigns every request an id, echoes it back so the browser can quote it, and records one
 * completion line per API call.
 */
export const requestLogging = (): RequestHandler => (req, res, next) => {
  const state = requestState(req);
  res.setHeader('x-request-id', state.id);
  res.on('finish', () => {
    if (!req.path.startsWith('/api/')) return;
    logger.info('http.request', {
      ...requestFields(req),
      status: res.statusCode,
      durationMs: Date.now() - state.startedAt,
    });
  });
  next();
};

/** Log a failed request with its stack, then answer with the message and the correlating id. */
export const respondWithError = (event: string, req: Request, res: Response, error: unknown) => {
  const status = error instanceof z.ZodError ? 400 : 500;
  logger[status >= 500 ? 'error' : 'warn'](`${event}.failed`, {
    ...requestFields(req),
    status,
    error: serializeError(error),
  });
  if (res.headersSent) return;
  res.status(status).json({ error: errorMessage(error), requestId: requestId(req) });
};

/** Wraps a route so neither a throw nor a rejection can escape without being logged and answered. */
export const apiRoute =
  (event: string, handler: (req: Request, res: Response) => Promise<void> | void): RequestHandler =>
  (req, res) => {
    void (async () => {
      try {
        await handler(req, res);
      } catch (error) {
        respondWithError(event, req, res, error);
      }
    })();
  };

const clientLogLevel = z.enum(['debug', 'info', 'warn', 'error']);
const clientLogEventSchema = z.object({
  level: clientLogLevel,
  scope: z.string().trim().min(1).max(120),
  message: z.string().trim().min(1).max(2000),
  timestamp: z.string().trim().max(40).optional(),
  stack: z.string().max(maxStackLength).optional(),
  componentStack: z.string().max(maxStackLength).optional(),
  url: z.string().max(2000).optional(),
  context: z.record(z.string(), z.unknown()).optional(),
});
export const clientLogBatchSchema = z.object({
  sessionId: z.string().trim().max(64).optional(),
  events: z.array(clientLogEventSchema).min(1).max(50),
});
type ClientLogBatch = z.infer<typeof clientLogBatchSchema>;

/** Re-emit browser events into the app logs so UI failures are diagnosable server side. */
export const logClientEvents = (req: Request, batch: ClientLogBatch) => {
  for (const event of batch.events) {
    logger[event.level](`client.${event.scope}`, {
      source: 'client',
      requestId: requestId(req),
      sessionId: batch.sessionId,
      user: headerValue(req, 'x-forwarded-email') ?? headerValue(req, 'x-forwarded-user'),
      message: event.message,
      clientTimestamp: event.timestamp,
      url: event.url,
      userAgent: headerValue(req, 'user-agent'),
      context: event.context,
      stack: event.stack,
      componentStack: event.componentStack,
    });
  }
};

/** Node terminates on these by default; log the reason first so the crash is not silent. */
export const installProcessLogging = () => {
  process.on('uncaughtException', (error) => {
    logger.error('process.uncaught_exception', { error: serializeError(error) });
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    logger.error('process.unhandled_rejection', { error: serializeError(reason) });
    process.exit(1);
  });
};
