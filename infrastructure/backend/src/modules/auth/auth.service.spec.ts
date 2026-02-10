import { Test, TestingModule } from '@nestjs/testing';
import { AuthService } from './auth.service';
import { PrismaService } from '../../database/prisma/prisma.service';
import { JwtService } from '@nestjs/jwt';
import { ConfigService } from '@nestjs/config';
import { UnauthorizedException } from '@nestjs/common';
import { AuthType } from './dto/login.dto';

describe('AuthService', () => {
  let service: AuthService;
  let prismaService: PrismaService;
  let jwtService: JwtService;
  let configService: ConfigService;

  const mockPrismaService = {
    user: {
      findUnique: jest.fn(),
      create: jest.fn(),
    },
  };

  const mockJwtService = {
    sign: jest.fn(),
  };

  const mockConfigService = {
    get: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AuthService,
        {
          provide: PrismaService,
          useValue: mockPrismaService,
        },
        {
          provide: JwtService,
          useValue: mockJwtService,
        },
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
      ],
    }).compile();

    service = module.get<AuthService>(AuthService);
    prismaService = module.get<PrismaService>(PrismaService);
    jwtService = module.get<JwtService>(JwtService);
    configService = module.get<ConfigService>(ConfigService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('login', () => {
    const mockUser = {
      id: '123e4567-e89b-12d3-a456-426614174000',
      name: 'Shahid Ali',
      email: 'shahid.ali@datanexum.com',
      authType: 'microsoft' as const,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const validMicrosoftToken =
      'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJuYW1lIjoiU2hhaGlkIEFsaSIsImVtYWlsIjoic2hhaGlkLmFsaUBkYXRhbmV4dW0uY29tIiwidXBuIjoic2hhaGlkLmFsaUBkYXRhbmV4dW0uY29tIn0.signature';

    it('should login existing user with Microsoft SSO', async () => {
      // Arrange
      mockPrismaService.user.findUnique.mockResolvedValue(mockUser);
      mockJwtService.sign.mockReturnValue('generated-jwt-token');

      // Act
      const result = await service.login({
        accessToken: validMicrosoftToken,
        authType: AuthType.MICROSOFT_SSO,
      });

      // Assert
      expect(result).toHaveProperty('accessToken', 'generated-jwt-token');
      expect(result).toHaveProperty('tokenType', 'Bearer');
      expect(result).toHaveProperty('expiresIn', 3600);
      expect(result.user).toMatchObject({
        id: mockUser.id,
        name: mockUser.name,
        email: mockUser.email,
        authType: mockUser.authType,
      });
      expect(mockPrismaService.user.findUnique).toHaveBeenCalledWith({
        where: { email: mockUser.email },
      });
    });

    it('should create new user on first login', async () => {
      // Arrange
      mockPrismaService.user.findUnique.mockResolvedValue(null);
      mockPrismaService.user.create.mockResolvedValue(mockUser);
      mockJwtService.sign.mockReturnValue('generated-jwt-token');

      // Act
      const result = await service.login({
        accessToken: validMicrosoftToken,
        authType: AuthType.MICROSOFT_SSO,
      });

      // Assert
      expect(mockPrismaService.user.create).toHaveBeenCalledWith({
        data: {
          name: expect.any(String),
          email: expect.any(String),
          authType: AuthType.MICROSOFT_SSO,
        },
      });
      expect(result.accessToken).toBeDefined();
    });

    it('should throw UnauthorizedException for invalid token', async () => {
      // Arrange
      const invalidToken = 'invalid.token.here';

      // Act & Assert
      await expect(
        service.login({
          accessToken: invalidToken,
          authType: AuthType.MICROSOFT_SSO,
        }),
      ).rejects.toThrow(UnauthorizedException);
    });

    it('should generate JWT with correct payload', async () => {
      // Arrange
      mockPrismaService.user.findUnique.mockResolvedValue(mockUser);
      mockJwtService.sign.mockReturnValue('jwt-token');

      // Act
      await service.login({
        accessToken: validMicrosoftToken,
        authType: AuthType.MICROSOFT_SSO,
      });

      // Assert
      expect(mockJwtService.sign).toHaveBeenCalledWith({
        sub: mockUser.id,
        email: mockUser.email,
        name: mockUser.name,
        authType: mockUser.authType,
      });
    });

    it('should handle Snowflake authentication', async () => {
      // Arrange
      const snowflakeUser = { ...mockUser, authType: 'snowflake' as const };
      mockPrismaService.user.findUnique.mockResolvedValue(snowflakeUser);
      mockJwtService.sign.mockReturnValue('jwt-token');

      // Act
      const result = await service.login({
        accessToken: validMicrosoftToken,
        authType: AuthType.SNOWFLAKE,
      });

      // Assert
      expect(result).toBeDefined();
      expect(result.user.authType).toBe('snowflake');
    });

    it('should throw for unsupported auth type', async () => {
      // Act & Assert
      await expect(
        service.login({
          accessToken: validMicrosoftToken,
          authType: 'unsupported' as any,
        }),
      ).rejects.toThrow(UnauthorizedException);
    });
  });

  describe('validateMicrosoftToken', () => {
    it('should decode valid Microsoft token', async () => {
      // Arrange
      const token =
        Buffer.from(
          JSON.stringify({
            header: { typ: 'JWT', alg: 'RS256' },
          }),
        ).toString('base64') +
        '.' +
        Buffer.from(
          JSON.stringify({
            name: 'Shahid Ali',
            email: 'shahid.ali@datanexum.com',
            upn: 'shahid.ali@datanexum.com',
          }),
        ).toString('base64') +
        '.signature';

      // Act
      const result = await service['validateMicrosoftToken'](token);

      // Assert
      expect(result).toHaveProperty('name', 'Shahid Ali');
      expect(result).toHaveProperty('email', 'shahid.ali@datanexum.com');
    });

    it('should use upn when email is not present', async () => {
      // Arrange
      const token =
        Buffer.from(
          JSON.stringify({
            header: { typ: 'JWT', alg: 'RS256' },
          }),
        ).toString('base64') +
        '.' +
        Buffer.from(
          JSON.stringify({
            name: 'Test User',
            upn: 'test@example.com',
          }),
        ).toString('base64') +
        '.signature';

      // Act
      const result = await service['validateMicrosoftToken'](token);

      // Assert
      expect(result).toHaveProperty('email', 'test@example.com');
    });

    it('should throw for malformed token', async () => {
      // Act & Assert
      await expect(service['validateMicrosoftToken']('malformed-token')).rejects.toThrow(
        UnauthorizedException,
      );
    });
  });

  describe('validateUser', () => {
    it('should return user when exists', async () => {
      // Arrange
      const mockUser = {
        id: 'user-id',
        name: 'Test User',
        email: 'test@example.com',
        authType: 'microsoft',
      };
      mockPrismaService.user.findUnique.mockResolvedValue(mockUser);

      // Act
      const result = await service.validateUser('user-id');

      // Assert
      expect(result).toEqual(mockUser);
      expect(mockPrismaService.user.findUnique).toHaveBeenCalledWith({
        where: { id: 'user-id' },
      });
    });

    it('should throw UnauthorizedException when user not found', async () => {
      // Arrange
      mockPrismaService.user.findUnique.mockResolvedValue(null);

      // Act & Assert
      await expect(service.validateUser('non-existent-id')).rejects.toThrow(UnauthorizedException);
    });
  });

  describe('Snowflake OAuth', () => {
    it('should throw for validateSnowflakeCode (not implemented)', async () => {
      // Act & Assert
      await expect(service.validateSnowflakeCode({ code: 'test-code' })).rejects.toThrow(
        UnauthorizedException,
      );
    });

    it('should throw for refreshSnowflakeToken (not implemented)', async () => {
      // Act & Assert
      await expect(service.refreshSnowflakeToken({ refreshToken: 'test-token' })).rejects.toThrow(
        UnauthorizedException,
      );
    });
  });
});
