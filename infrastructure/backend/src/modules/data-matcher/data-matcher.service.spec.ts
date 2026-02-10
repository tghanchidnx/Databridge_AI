import { Test, TestingModule } from '@nestjs/testing';
import { DataMatcherService } from './data-matcher.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { PrismaService } from '../../database/prisma/prisma.service';

describe('DataMatcherService', () => {
  let service: DataMatcherService;

  const mockSnowflakeService = {
    executeQuery: jest.fn(),
  };

  const mockPrismaService = {
    connection: {
      findUnique: jest.fn(),
    },
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DataMatcherService,
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

    service = module.get<DataMatcherService>(DataMatcherService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('compareData', () => {
    it('should compare data between tables', async () => {
      const mockConnection = {
        id: 'conn-123',
        credentials: '{"account":"test"}',
      };

      mockPrismaService.connection.findUnique.mockResolvedValue(mockConnection);
      mockSnowflakeService.executeQuery.mockResolvedValue([{ COUNT: 100 }]);

      const result = await service.compareData({
        sourceConnectionId: 'source',
        targetConnectionId: 'target',
        sourceTable: 'TABLE1',
        targetTable: 'TABLE2',
      });

      expect(result).toHaveProperty('sourceRowCount');
      expect(result).toHaveProperty('targetRowCount');
      expect(result).toHaveProperty('matchPercentage');
    });
  });
});
