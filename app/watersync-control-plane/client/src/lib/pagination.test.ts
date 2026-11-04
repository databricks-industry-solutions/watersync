import { describe, expect, it } from 'vitest';
import { pageInfo } from './pagination';

describe('pageInfo', () => {
  it('describes a middle page', () => {
    expect(pageInfo(1_000_000, 2, 50)).toEqual({ pageCount: 20_000, page: 2, first: 101, last: 150 });
  });

  it('ends on a partial last page', () => {
    expect(pageInfo(103, 10, 10)).toEqual({ pageCount: 11, page: 10, first: 101, last: 103 });
  });

  it('clamps a page past the end, e.g. after a filter shrank the list', () => {
    expect(pageInfo(3, 7, 50)).toEqual({ pageCount: 1, page: 0, first: 1, last: 3 });
  });

  it('has one empty page when nothing matches', () => {
    expect(pageInfo(0, 0, 10)).toEqual({ pageCount: 1, page: 0, first: 0, last: 0 });
  });
});
