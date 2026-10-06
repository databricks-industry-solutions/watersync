import { useEffect } from 'react';
import { ApiError } from './api';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LogContext = Record<string, unknown>;

type ClientLogEvent = {
  level: LogLevel;
  scope: string;
  message: string;
  timestamp: string;
  url: string;
  stack?: string;
  componentStack?: string;
  context?: LogContext;
};

const endpoint = '/api/client-logs';
const maxQueued = 50;
const maxPerRequest = 20;
const flushDelayMs = 1000;
const repeatWindowMs = 10_000;
const maxSignatures = 200;

const newSessionId = () => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
};

/** Stable for the lifetime of the tab, so a user can quote it and we can group their events. */
const sessionId = newSessionId();
export const logSessionId = () => sessionId;

const queue: ClientLogEvent[] = [];
const lastSentAt = new Map<string, number>();
let flushTimer: ReturnType<typeof setTimeout> | undefined;

const send = (events: ClientLogEvent[], beacon: boolean) => {
  const body = JSON.stringify({ sessionId, events });
  if (beacon && typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    if (navigator.sendBeacon(endpoint, new Blob([body], { type: 'application/json' }))) return;
  }
  // Reporting must never surface its own failure: a rejected report would otherwise be reported again.
  void fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
    keepalive: true,
  }).catch(() => undefined);
};

const flush = (beacon = false) => {
  if (flushTimer !== undefined) {
    clearTimeout(flushTimer);
    flushTimer = undefined;
  }
  while (queue.length) send(queue.splice(0, maxPerRequest), beacon);
};

const enqueue = (event: ClientLogEvent) => {
  const signature = `${event.level}|${event.scope}|${event.message}`;
  const now = Date.now();
  const previous = lastSentAt.get(signature);
  // A repeating failure (a render loop, a dead endpoint polled on a timer) must not flood the log.
  if (previous !== undefined && now - previous < repeatWindowMs) return;
  if (lastSentAt.size >= maxSignatures) lastSentAt.clear();
  lastSentAt.set(signature, now);

  if (queue.length >= maxQueued) queue.shift();
  queue.push(event);
  if (event.level === 'error') {
    flush();
    return;
  }
  flushTimer ??= setTimeout(() => flush(), flushDelayMs);
};

export const errorMessage = (value: unknown) => {
  if (value instanceof Error) return value.message;
  return typeof value === 'string' && value ? value : 'Unexpected error';
};

const apiContext = (error: unknown): LogContext | undefined =>
  error instanceof ApiError
    ? { status: error.status, requestMethod: error.method, requestPath: error.path, requestId: error.requestId }
    : undefined;

const report = (
  level: LogLevel,
  scope: string,
  message: string,
  options: { error?: unknown; componentStack?: string; context?: LogContext } = {}
) => {
  const context = { ...apiContext(options.error), ...options.context };
  const consoleArgs = [`[watersync] ${scope}: ${message}`, ...(options.error === undefined ? [] : [options.error])];
  if (level === 'error') console.error(...consoleArgs);
  else if (level === 'warn') console.warn(...consoleArgs);
  else console.info(...consoleArgs);

  enqueue({
    level,
    scope,
    message,
    timestamp: new Date().toISOString(),
    url: window.location.pathname + window.location.search,
    stack: options.error instanceof Error ? options.error.stack : undefined,
    componentStack: options.componentStack,
    context: Object.keys(context).length ? context : undefined,
  });
};

/**
 * Send an exception to the app logs and return its user-facing message, so a handler can do
 * `setError(reportError('config.save', error))` and never drop the failure on the floor.
 */
export const reportError = (scope: string, error: unknown, context?: LogContext) => {
  const message = errorMessage(error);
  report('error', scope, message, { error, context });
  return message;
};

/** Report a failure that only ever arrives as a message, such as an analytics query error. */
export const reportFailure = (scope: string, message: string, context?: LogContext) => {
  report('error', scope, message, { context });
  return message;
};

export const reportWarning = (scope: string, message: string, context?: LogContext) => {
  report('warn', scope, message, { context });
};

export const reportComponentError = (
  scope: string,
  error: unknown,
  componentStack: string | null | undefined,
  context?: LogContext
) => {
  report('error', scope, errorMessage(error), { error, componentStack: componentStack ?? undefined, context });
};

let handlersInstalled = false;

/** Catches everything that never reaches a component: render-time throws, stray rejections, script errors. */
export const installGlobalErrorHandlers = () => {
  if (handlersInstalled) return;
  handlersInstalled = true;

  window.addEventListener('error', (event) => {
    const error: unknown = event.error;
    report(
      'error',
      'window.error',
      error instanceof Error ? error.message : event.message || 'Unhandled script error',
      {
        error,
        context: { source: event.filename, line: event.lineno, column: event.colno },
      }
    );
  });

  window.addEventListener('unhandledrejection', (event) => {
    const reason: unknown = event.reason;
    report('error', 'window.unhandled_rejection', errorMessage(reason), {
      error: reason,
      context: reason instanceof Error ? undefined : { reason: String(reason).slice(0, 500) },
    });
  });

  // A hidden or unloading page may never get another chance to send what is queued.
  window.addEventListener('pagehide', () => flush(true));
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flush(true);
  });
};

/**
 * Reports a query or fetch error string that a component renders, once per distinct message.
 * Analytics hooks surface failures as state rather than throwing, so nothing else would log them.
 */
export const useReportedFailure = (scope: string, message: string | null | undefined, context?: LogContext) => {
  // Serialized so an inline context object does not re-trigger the effect on every render.
  const contextKey = context ? JSON.stringify(context) : '';
  useEffect(() => {
    if (message) reportFailure(scope, message, contextKey ? (JSON.parse(contextKey) as LogContext) : undefined);
  }, [scope, message, contextKey]);
};
