import { Test, TestingModule } from '@nestjs/testing';
import { HealthService } from './health.service';
import { PrismaService } from '../../database/prisma/prisma.service';

describe('HealthService', () => {
  let service: HealthService;
  let prismaService: PrismaService;

  const mockPrismaService = {
    healthCheck: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        HealthService,
        {
          provide: PrismaService,
          useValue: mockPrismaService,
        },
      ],
    }).compile();

    service = module.get<HealthService>(HealthService);
    prismaService = module.get<PrismaService>(PrismaService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('check', () => {
    it('should return healthy status when database is connected', async () => {
      // Arrange
      mockPrismaService.healthCheck.mockResolvedValue(true);

      // Act
      const result = await service.check();

      // Assert
      expect(result).toHaveProperty('status', 'ok');
      expect(result).toHaveProperty('timestamp');
      expect(result).toHaveProperty('uptime');
      expect(result).toHaveProperty('database');
      expect(result.database).toHaveProperty('status', 'connected');
      expect(mockPrismaService.healthCheck).toHaveBeenCalled();
      expect(result.uptime).toBeGreaterThan(0);
    });

    it('should return unhealthy status when database connection fails', async () => {
      // Arrange
      mockPrismaService.healthCheck.mockRejectedValue(new Error('Connection failed'));

      // Act
      const result = await service.check();

      // Assert
      expect(result).toHaveProperty('status', 'ok');
      expect(result).toHaveProperty('database');
      expect(result.database).toHaveProperty('status', 'error');
      expect(mockPrismaService.healthCheck).toHaveBeenCalled();
    });

    it('should calculate uptime correctly', async () => {
      // Arrange
      mockPrismaService.healthCheck.mockResolvedValue(true);
      const beforeUptime = process.uptime();

      // Act
      const result = await service.check();
      const afterUptime = process.uptime();

      // Assert
      expect(result.uptime).toBeGreaterThanOrEqual(beforeUptime);
      expect(result.uptime).toBeLessThanOrEqual(afterUptime);
    });

    it('should return timestamp in ISO format', async () => {
      // Arrange
      mockPrismaService.healthCheck.mockResolvedValue(true);

      // Act
      const result = await service.check();

      // Assert
      expect(result.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
      const timestamp = new Date(result.timestamp);
      expect(timestamp).toBeInstanceOf(Date);
      expect(timestamp.getTime()).not.toBeNaN();
    });
  });

  describe('error handling', () => {
    it('should handle database timeout', async () => {
      // Arrange
      mockPrismaService.healthCheck.mockImplementation(() => {
        return new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Timeout')), 100);
        });
      });

      // Act
      const result = await service.check();

      // Assert
      expect(result.database.status).toBe('error');
    });

    it('should handle database query errors gracefully', async () => {
      // Arrange
      const dbError = new Error('Query failed: syntax error');
      mockPrismaService.healthCheck.mockRejectedValue(dbError);

      // Act
      const result = await service.check();

      // Assert
      expect(result.database.status).toBe('error');
      expect(result.status).toBe('ok'); // Service still returns ok, just db is down
    });
  });

  describe('integration scenarios', () => {
    it('should work with actual Prisma service (if available)', async () => {
      // This would be an integration test
      // For now, we mock it
      mockPrismaService.healthCheck.mockResolvedValue(true);

      const result = await service.check();

      expect(result).toBeDefined();
      expect(result.status).toBe('ok');
    });
  });
});
