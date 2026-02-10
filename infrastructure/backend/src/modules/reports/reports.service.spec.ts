import { Test, TestingModule } from '@nestjs/testing';
import { ReportsService } from './reports.service';
import { PrismaService } from '../../database/prisma/prisma.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';

describe('ReportsService', () => {
  let service: ReportsService;

  const mockPrismaService = {
    connection: {
      findUnique: jest.fn(),
    },
    auditLog: {
      create: jest.fn(),
    },
  };

  const mockSnowflakeService = {
    executeQuery: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReportsService,
        {
          provide: PrismaService,
          useValue: mockPrismaService,
        },
        {
          provide: SnowflakeService,
          useValue: mockSnowflakeService,
        },
      ],
    }).compile();

    service = module.get<ReportsService>(ReportsService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('generateReport', () => {
    it('should generate a hierarchy report', async () => {
      const mockConnection = {
        id: 'conn-123',
        connectionName: 'Test',
        connectionType: 'SNOWFLAKE',
        credentials: JSON.stringify({ account: 'test-account', username: 'test-user' }),
      };

      mockPrismaService.connection.findUnique.mockResolvedValue(mockConnection);
      mockSnowflakeService.executeQuery.mockResolvedValue([]);
      mockPrismaService.auditLog.create.mockResolvedValue({});

      const result = await service.generateReport('user-123', {
        connectionId: 'conn-123',
        database: 'TEST_DB',
      });

      expect(result).toHaveProperty('database');
      expect(result).toHaveProperty('schemas');
      expect(result).toHaveProperty('statistics');
    });
  });
});
