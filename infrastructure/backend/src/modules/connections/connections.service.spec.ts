import { Test, TestingModule } from '@nestjs/testing';
import { ConnectionsService } from './connections.service';
import { PrismaService } from '../../database/prisma/prisma.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { NotFoundException } from '@nestjs/common';

describe('ConnectionsService', () => {
  let service: ConnectionsService;

  const mockPrismaService = {
    connection: {
      findMany: jest.fn(),
      findFirst: jest.fn(),
      create: jest.fn(),
      update: jest.fn(),
      delete: jest.fn(),
    },
  };

  const mockSnowflakeService = {
    testConnection: jest.fn(),
  };

  const mockConnection = {
    id: 'conn-123',
    userId: 'user-123',
    connectionName: 'Test Connection',
    connectionType: 'SNOWFLAKE',
    credentials: '{"account":"test-account"}',
    isActive: true,
    createdAt: new Date(),
    updatedAt: new Date(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ConnectionsService,
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

    service = module.get<ConnectionsService>(ConnectionsService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('findAll', () => {
    it('should return sanitized connections', async () => {
      mockPrismaService.connection.findMany.mockResolvedValue([mockConnection]);

      const result = await service.findAll('user-123');

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('hasCredentials', true);
      expect(result[0]).not.toHaveProperty('credentials');
    });

    it('should return empty array when no connections', async () => {
      mockPrismaService.connection.findMany.mockResolvedValue([]);

      const result = await service.findAll('user-123');

      expect(result).toEqual([]);
    });
  });

  describe('findOne', () => {
    it('should return a sanitized connection', async () => {
      mockPrismaService.connection.findFirst.mockResolvedValue(mockConnection);

      const result = await service.findOne('conn-123', 'user-123');

      expect(result).toBeDefined();
      expect(result).toHaveProperty('hasCredentials', true);
      expect(result).not.toHaveProperty('credentials');
    });

    it('should throw NotFoundException when not found', async () => {
      mockPrismaService.connection.findFirst.mockResolvedValue(null);

      await expect(service.findOne('invalid', 'user-123')).rejects.toThrow(NotFoundException);
    });
  });

  describe('remove', () => {
    it('should delete connection successfully', async () => {
      mockPrismaService.connection.findFirst.mockResolvedValue(mockConnection);
      mockPrismaService.connection.delete.mockResolvedValue(mockConnection);

      const result = await service.remove('conn-123', 'user-123');

      expect(result).toEqual({ message: 'Connection deleted successfully' });
    });
  });
});
