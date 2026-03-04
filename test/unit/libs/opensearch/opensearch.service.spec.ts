import { Test, TestingModule } from '@nestjs/testing';
import { OpenSearchService } from '@app/opensearch';

const mockExists = jest.fn();
const mockCreate = jest.fn();
const mockBulk = jest.fn();

const mockClient = {
  indices: {
    exists: mockExists,
    create: mockCreate,
  },
  bulk: mockBulk,
};

jest.mock('@opensearch-project/opensearch', () => ({
  Client: jest.fn().mockImplementation(() => mockClient),
}));

describe('OpenSearchService', () => {
  let service: OpenSearchService;

  beforeEach(async () => {
    jest.clearAllMocks();
    mockExists.mockResolvedValue({ body: false });
    mockCreate.mockResolvedValue({});
    mockBulk.mockResolvedValue({ body: { errors: false, items: [] } });

    const module: TestingModule = await Test.createTestingModule({
      providers: [OpenSearchService],
    }).compile();

    service = module.get(OpenSearchService);
  });

  afterEach(() => {
    service.onModuleDestroy();
  });

  it('should return same client instance on getClient', () => {
    const c1 = service.getClient();
    const c2 = service.getClient();
    expect(c1).toBe(c2);
  });

  it('should create index when it does not exist on ensureIndex', async () => {
    mockExists.mockResolvedValueOnce({ body: false });
    await service.ensureIndex('my-index');
    expect(mockExists).toHaveBeenCalledWith({ index: 'my-index' });
    expect(mockCreate).toHaveBeenCalledWith({ index: 'my-index' });
  });

  it('should not create when index exists on ensureIndex', async () => {
    mockExists.mockResolvedValueOnce({ body: true });
    await service.ensureIndex('existing');
    expect(mockCreate).not.toHaveBeenCalled();
  });

  it('should return count 0 and errors false for empty items on bulkIndex', async () => {
    const result = await service.bulkIndex('idx', []);
    expect(result).toEqual({ count: 0, errors: false });
    expect(mockBulk).not.toHaveBeenCalled();
  });

  it('should send body and return count and errors on bulkIndex', async () => {
    const id = 'doc-id-1';
    const source = { title: 'Test document' };
    mockBulk.mockResolvedValueOnce({
      body: { errors: false, items: [{ index: {} }, { index: {} }] },
    });

    const result = await service.bulkIndex('idx', [{ id, source }]);

    expect(mockBulk).toHaveBeenCalledWith(
      expect.objectContaining({
        body: [{ index: { _index: 'idx', _id: id } }, source],
      }),
    );
    expect(result).toEqual({ count: 2, errors: false });
  });

  it('should return errors true when body.errors is true on bulkIndex', async () => {
    mockBulk.mockResolvedValueOnce({
      body: { errors: true, items: [{ index: { error: 'x' } }] },
    });
    const result = await service.bulkIndex('idx', [
      { id: 'doc-id-2', source: {} },
    ]);
    expect(result).toEqual({ count: 1, errors: true });
  });

  it('should return count 0 when body.items is undefined on bulkIndex', async () => {
    mockBulk.mockResolvedValueOnce({ body: { errors: false } });
    const result = await service.bulkIndex('idx', [
      { id: 'doc-id-3', source: { x: 1 } },
    ]);
    expect(result).toEqual({ count: 0, errors: false });
  });

  it('should clear client on module destroy', () => {
    service.getClient();
    service.onModuleDestroy();
    const client = service.getClient();
    expect(client).toBe(mockClient);
  });
});
