import { NestFactory } from '@nestjs/core';
import { ValidationPipe, VersioningType } from '@nestjs/common';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { ConfigService } from '@nestjs/config';
import { NestExpressApplication } from '@nestjs/platform-express';
import helmet from 'helmet';
import * as compression from 'compression';
import * as bodyParser from 'body-parser';
import { join } from 'path';
import { AppModule } from './app.module';
import { HttpExceptionFilter } from './common/filters/http-exception.filter';
import { TransformInterceptor } from './common/interceptors/transform.interceptor';
import { LoggingInterceptor } from './common/interceptors/logging.interceptor';
import { WinstonModule } from 'nest-winston';
import { winstonConfig } from './config/winston.config';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: WinstonModule.createLogger(winstonConfig),
  });

  // Increase body parser limits for large CSV imports (50MB)
  app.use(bodyParser.json({ limit: '50mb' }));
  app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

  const configService = app.get(ConfigService);

  // Security - MUST be before static assets
  app.use(
    helmet({
      crossOriginResourcePolicy: { policy: 'cross-origin' },
      contentSecurityPolicy: false, // Allow loading images from same origin
    }),
  );

  // Serve static files for uploads
  const uploadsPath = join(process.cwd(), 'uploads');
  console.log('Serving static files from:', uploadsPath);
  app.useStaticAssets(uploadsPath, {
    prefix: '/uploads',
  });

  app.enableCors({
    origin: configService.get('CORS_ORIGINS')?.split(',') || '*',
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-session-type', 'x-access-token', 'X-API-Key', 'x-api-key'],
  });

  app.use(compression());

  // Validation
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );

  // Global filters and interceptors
  app.useGlobalFilters(new HttpExceptionFilter());
  app.useGlobalInterceptors(new LoggingInterceptor(), new TransformInterceptor());

  // Global prefix - BEFORE Swagger to ensure routes are registered correctly
  const apiPrefix = configService.get('API_PREFIX') || 'api';
  app.setGlobalPrefix(apiPrefix, {
    exclude: ['/uploads/(.*)'], // Exclude uploads from API prefix
  });

  // Swagger documentation
  const swaggerConfig = new DocumentBuilder()
    .setTitle('Data Amplifier API')
    .setDescription('Enterprise Data Warehouse Schema Comparison & Hierarchy Management System')
    .setVersion('2.0.0')
    .addBearerAuth(
      {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
        name: 'JWT',
        description: 'Enter JWT token',
        in: 'header',
      },
      'JWT-auth',
    )
    .addApiKey(
      {
        type: 'apiKey',
        name: 'x-api-key',
        in: 'header',
        description: 'API Key for external integrations',
      },
      'API-Key',
    )
    .addTag('Authentication', 'User authentication and authorization endpoints')
    .addTag('Users', 'User management')
    .addTag('Connections', 'Database connection management')
    .addTag('Schema Matcher', 'Schema comparison and synchronization')
    .addTag('Data Matcher', 'Data comparison and validation')
    .addTag('Reports', 'Hierarchy and report management')
    .addTag('Health', 'Application health checks and monitoring')
    .build();

  const document = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup(`api/docs`, app, document, {
    swaggerOptions: {
      persistAuthorization: true,
      docExpansion: 'none',
      filter: true,
      showRequestDuration: true,
    },
    customSiteTitle: 'Data Amplifier API Documentation',
    customCss: '.swagger-ui .topbar { display: none }',
  });

  const port = configService.get('PORT', 3000);
  await app.listen(port, '0.0.0.0');

  console.log(`
╔════════════════════════════════════════════════════════════╗
║                                                            ║
║       🚀  Data Amplifier API Server Started  🚀           ║
║                                                            ║
║  Application:  http://localhost:${port}/api                 ║
║  Documentation: http://localhost:${port}/api/docs           ║
║  Health Check:  http://localhost:${port}/api/health         ║
║                                                            ║
║  Environment:   ${configService.get('NODE_ENV')}                          ║
║  Database:      MySQL (dataamplifier)                     ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
  `);
}

bootstrap();

// Hot Module Replacement - DISABLED to prevent double server startup
// declare const module: any;

// if (module.hot) {
//   module.hot.accept();
//   module.hot.dispose(() => console.log('Module disposed.'));
// }
