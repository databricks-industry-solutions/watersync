import { reportWarning } from './logging';

/**
 * A JSON column of a query result. The analytics plugin already parses STRING values that start with
 * { or [, so the value is usually an object; a string (if that behaviour changes) is parsed here.
 */
export function jsonColumn<T extends object>(value: unknown, scope: string): T | null {
  if (value == null || value === '') return null;
  if (typeof value === 'object') return value as T;
  if (typeof value === 'string') {
    try {
      return JSON.parse(value) as T;
    } catch (x) {
      reportWarning(scope, 'Column is not valid JSON', { detail: String(x) });
      return null;
    }
  }
  reportWarning(scope, 'Column has an unexpected type', { detail: typeof value });
  return null;
}
