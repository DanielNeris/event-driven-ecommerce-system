import { getOpenSearchNode } from '@app/opensearch/opensearch.config';

describe('opensearch.config', () => {
  it('should return env.OPENSEARCH_NODE from getOpenSearchNode', () => {
    const node = getOpenSearchNode();
    expect(typeof node).toBe('string');
    expect(node).toContain('http');
  });
});
