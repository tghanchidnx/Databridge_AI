import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PrismaService.name);

  constructor() {
    super({
      log: [
        { emit: 'event', level: 'query' },
        { emit: 'event', level: 'error' },
        { emit: 'event', level: 'warn' },
      ],
    });
  }

  async onModuleInit() {
    try {
      await this.$connect();
      this.logger.log('✅ Database connected successfully');

      // Log queries in development
      if (process.env.NODE_ENV === 'development') {
        this.$on('query' as never, (e: any) => {
          this.logger.debug(`Query: ${e.query}`);
          this.logger.debug(`Duration: ${e.duration}ms`);
        });
      }

      // Log errors
      this.$on('error' as never, (e: any) => {
        this.logger.error(`Database error: ${e.message}`);
      });

      // Log warnings
      this.$on('warn' as never, (e: any) => {
        this.logger.warn(`Database warning: ${e.message}`);
      });
    } catch (error) {
      this.logger.error('❌ Failed to connect to database', error);
      throw error;
    }
  }

  async onModuleDestroy() {
    await this.$disconnect();
    this.logger.log('Database disconnected');
  }

  async healthCheck(): Promise<boolean> {
    try {
      await this.$queryRaw`SELECT 1`;
      return true;
    } catch (error) {
      this.logger.error('Health check failed', error);
      return false;
    }
  }
}
