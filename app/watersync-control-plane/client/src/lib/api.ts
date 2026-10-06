/** Error thrown for any non-2xx API response, carrying the fields needed to correlate with app logs. */
export class ApiError extends Error {
  readonly status: number;
  readonly method: string;
  readonly path: string;
  /** Identifier the server logged this failure under, when it reported one. */
  readonly requestId?: string;

  constructor(message: string, details: { status: number; method: string; path: string; requestId?: string }) {
    super(message);
    this.name = 'ApiError';
    this.status = details.status;
    this.method = details.method;
    this.path = details.path;
    this.requestId = details.requestId;
  }
}

const stringField = (body: unknown, field: string) => {
  if (typeof body !== 'object' || body === null || !(field in body)) return undefined;
  const value = (body as Record<string, unknown>)[field];
  return typeof value === 'string' && value ? value : undefined;
};

export const api = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const method = init?.method ?? 'GET';
  const response = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...init });
  const body: unknown = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new ApiError(stringField(body, 'error') ?? `Request failed (${response.status})`, {
      status: response.status,
      method,
      path,
      requestId: stringField(body, 'requestId') ?? response.headers.get('x-request-id') ?? undefined,
    });
  }
  return body as T;
};
