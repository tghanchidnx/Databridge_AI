import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from '../src/app.module';

describe('AppController (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();

    // Apply same configurations as main.ts
    app.setGlobalPrefix('api/v1');

    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );

    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  describe('Health Check', () => {
    it('/api/v1/health (GET) should return health status', () => {
      return request(app.getHttpServer())
        .get('/api/v1/health')
        .expect(200)
        .expect((res) => {
          expect(res.body).toHaveProperty('status', 'ok');
          expect(res.body).toHaveProperty('timestamp');
          expect(res.body).toHaveProperty('uptime');
          expect(res.body).toHaveProperty('database');
        });
    });

    it('/api/v1/health (GET) should include database status', () => {
      return request(app.getHttpServer())
        .get('/api/v1/health')
        .expect(200)
        .expect((res) => {
          expect(res.body.database).toHaveProperty('status');
          expect(['connected', 'disconnected']).toContain(res.body.database.status);
        });
    });
  });

  describe('Authentication', () => {
    const validMicrosoftToken =
      Buffer.from(
        JSON.stringify({
          header: { typ: 'JWT', alg: 'RS256' },
        }),
      ).toString('base64') +
      '.' +
      Buffer.from(
        JSON.stringify({
          name: 'Test User',
          email: 'test@example.com',
          upn: 'test@example.com',
        }),
      ).toString('base64') +
      '.signature';

    it('/api/v1/auth/login (POST) should accept Microsoft login', () => {
      return request(app.getHttpServer())
        .post('/api/v1/auth/login')
        .send({
          accessToken: validMicrosoftToken,
          authType: 'microsoft',
        })
        .expect(201)
        .expect((res) => {
          expect(res.body).toHaveProperty('accessToken');
          expect(res.body).toHaveProperty('tokenType', 'Bearer');
          expect(res.body).toHaveProperty('user');
          expect(res.body.user).toHaveProperty('email');
        });
    });

    it('/api/v1/auth/login (POST) should reject invalid token', () => {
      return request(app.getHttpServer())
        .post('/api/v1/auth/login')
        .send({
          accessToken: 'invalid-token',
          authType: 'microsoft',
        })
        .expect(401);
    });

    it('/api/v1/auth/login (POST) should validate request body', () => {
      return request(app.getHttpServer())
        .post('/api/v1/auth/login')
        .send({
          accessToken: validMicrosoftToken,
          // Missing authType
        })
        .expect(400);
    });
  });

  describe('Protected Routes', () => {
    it('/api/v1/users (GET) should require authentication', () => {
      return request(app.getHttpServer()).get('/api/v1/users').expect(401);
    });

    it('/api/v1/connections (GET) should require authentication', () => {
      return request(app.getHttpServer()).get('/api/v1/connections').expect(401);
    });

    it('/api/v1/schema-matcher/tables (GET) should require authentication', () => {
      return request(app.getHttpServer()).get('/api/v1/schema-matcher/tables').expect(401);
    });
  });

  describe('Authenticated Requests', () => {
    let jwtToken: string;

    beforeAll(async () => {
      const validMicrosoftToken =
        Buffer.from(
          JSON.stringify({
            header: { typ: 'JWT', alg: 'RS256' },
          }),
        ).toString('base64') +
        '.' +
        Buffer.from(
          JSON.stringify({
            name: 'E2E Test User',
            email: 'e2e-test@example.com',
            upn: 'e2e-test@example.com',
          }),
        ).toString('base64') +
        '.signature';

      const response = await request(app.getHttpServer()).post('/api/v1/auth/login').send({
        accessToken: validMicrosoftToken,
        authType: 'microsoft',
      });

      jwtToken = response.body.accessToken;
    });

    it('/api/v1/users (GET) should work with valid token', () => {
      return request(app.getHttpServer())
        .get('/api/v1/users')
        .set('Authorization', `Bearer ${jwtToken}`)
        .expect(200)
        .expect((res) => {
          expect(Array.isArray(res.body)).toBe(true);
        });
    });

    it('/api/v1/connections (GET) should work with valid token', () => {
      return request(app.getHttpServer())
        .get('/api/v1/connections')
        .set('Authorization', `Bearer ${jwtToken}`)
        .expect(200)
        .expect((res) => {
          expect(Array.isArray(res.body)).toBe(true);
        });
    });

    it('/api/v1/connections (POST) should create connection', () => {
      return request(app.getHttpServer())
        .post('/api/v1/connections')
        .set('Authorization', `Bearer ${jwtToken}`)
        .send({
          connectionName: 'Test Connection',
          connectionType: 'SNOWFLAKE',
          credentials: {
            account: 'test-account',
            username: 'test-user',
            password: 'test-password',
            warehouse: 'TEST_WH',
            database: 'TEST_DB',
            schema: 'PUBLIC',
          },
        })
        .expect(201)
        .expect((res) => {
          expect(res.body).toHaveProperty('id');
          expect(res.body).toHaveProperty('connectionName', 'Test Connection');
          expect(res.body).toHaveProperty('connectionType', 'SNOWFLAKE');
        });
    });
  });

  describe('Error Handling', () => {
    it('should return 404 for non-existent routes', () => {
      return request(app.getHttpServer()).get('/api/v1/non-existent-route').expect(404);
    });

    it('should handle malformed JSON', () => {
      return request(app.getHttpServer())
        .post('/api/v1/auth/login')
        .set('Content-Type', 'application/json')
        .send('{"invalid-json":')
        .expect(400);
    });

    it('should validate request body with proper error messages', () => {
      return request(app.getHttpServer())
        .post('/api/v1/auth/login')
        .send({
          accessToken: '', // Empty token
          authType: 'invalid-type',
        })
        .expect(400)
        .expect((res) => {
          expect(res.body).toHaveProperty('message');
          expect(Array.isArray(res.body.message)).toBe(true);
        });
    });
  });

  describe('CORS', () => {
    it('should include CORS headers', () => {
      return request(app.getHttpServer())
        .get('/api/v1/health')
        .expect(200)
        .expect((res) => {
          expect(res.headers).toHaveProperty('access-control-allow-origin');
        });
    });
  });

  describe('Rate Limiting', () => {
    it('should apply rate limiting (this test may be slow)', async () => {
      // Make multiple requests quickly
      const requests = Array.from({ length: 150 }, () =>
        request(app.getHttpServer()).get('/api/v1/health'),
      );

      const responses = await Promise.all(requests);
      const tooManyRequests = responses.filter((res) => res.status === 429);

      // Should have some rate limited responses
      expect(tooManyRequests.length).toBeGreaterThan(0);
    }, 30000); // 30 second timeout
  });
});
