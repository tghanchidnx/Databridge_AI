import { Test, TestingModule } from '@nestjs/testing';
import { SchemaMatcherService } from './schema-matcher.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { PrismaService } from '../../database/prisma/prisma.service';

describe('SchemaMatcherService', () => {
  let service: SchemaMatcherService;

  const mockSnowflakeService = {
    getTables: jest.fn(),
    getTableColumns: jest.fn(),
  };

  const mockPrismaService = {
    schemaComparisonJob: {
      create: jest.fn(),
    },
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SchemaMatcherService,
        {
          provide: SnowflakeService,
          useValue: mockSnowflakeService,
        },
        {
          provide: PrismaService,
          useValue: mockPrismaService,
        },
      ],
    }).compile();

    service = module.get<SchemaMatcherService>(SchemaMatcherService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('getTables', () => {
    it('should return tables from Snowflake', async () => {
      const mockTables = [{ TABLE_NAME: 'USERS' }, { TABLE_NAME: 'ORDERS' }];

      mockSnowflakeService.getTables.mockResolvedValue(mockTables);

      const result = await service.getTables({
        connectionId: 'conn-123',
        database: 'TEST_DB',
        schema: 'PUBLIC',
      });

      expect(result).toEqual(mockTables);
    });
  });

  describe('getTableColumns', () => {
    it('should return columns for a table', async () => {
      const mockColumns = [{ COLUMN_NAME: 'ID', DATA_TYPE: 'NUMBER' }];

      mockSnowflakeService.getTableColumns.mockResolvedValue(mockColumns);

      const result = await service.getTableColumns('conn-123', 'DB', 'SCHEMA', 'TABLE');

      expect(result).toEqual(mockColumns);
    });
  });

  describe('compareSchemas', () => {
    it('should create comparison job', async () => {
      const mockJob = {
        id: 'job-123',
        status: 'PENDING',
      };

      mockPrismaService.schemaComparisonJob.create.mockResolvedValue(mockJob);

      const result = await service.compareSchemas('user-123', {
        sourceConnectionId: 'source',
        targetConnectionId: 'target',
        sourceDatabase: 'DB1',
        sourceSchema: 'PUBLIC',
        targetDatabase: 'DB2',
        targetSchema: 'PUBLIC',
      });

      expect(result).toHaveProperty('jobId', 'job-123');
    });
  });
});
