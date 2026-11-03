import { describe, expect, it, vi } from 'vitest';

const reportWarning = vi.fn();
vi.mock('./logging', () => ({ reportWarning }));
const { jsonColumn } = await import('./jsonColumn');

describe('jsonColumn', () => {
  it('keeps a value the analytics plugin already parsed', () => {
    const parsed = { chunking: 'numeric', chunks: [] };
    expect(jsonColumn(parsed, 'test')).toBe(parsed);
  });

  it('parses a JSON string', () => {
    expect(jsonColumn('{"mismatched":[{"id":"1"}]}', 'test')).toEqual({ mismatched: [{ id: '1' }] });
  });

  it('treats missing values as no data', () => {
    expect(jsonColumn(null, 'test')).toBeNull();
    expect(jsonColumn(undefined, 'test')).toBeNull();
    expect(jsonColumn('', 'test')).toBeNull();
  });

  it('reports values it cannot read', () => {
    expect(jsonColumn('not json', 'test.scope')).toBeNull();
    expect(jsonColumn(42, 'test.scope')).toBeNull();
    expect(reportWarning).toHaveBeenCalledTimes(2);
    expect(reportWarning).toHaveBeenCalledWith('test.scope', 'Column is not valid JSON', expect.anything());
  });
});
