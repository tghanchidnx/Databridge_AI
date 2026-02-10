import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../database/prisma/prisma.service';

@Injectable()
export class HealthService {
  private readonly logger = new Logger(HealthService.name);

  constructor(private prisma: PrismaService) {}

  async check() {
    const health = {
      status: 'ok',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      database: {
        status: 'disconnected',
      },
    };

    try {
      const isHealthy = await this.prisma.healthCheck();
      health.database.status = isHealthy ? 'connected' : 'disconnected';
    } catch (error) {
      this.logger.error('Database health check failed', error);
      health.database.status = 'error';
    }

    return health;
  }
}
