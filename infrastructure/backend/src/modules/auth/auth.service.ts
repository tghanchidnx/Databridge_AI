import { Injectable, UnauthorizedException, Logger } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../database/prisma/prisma.service';
import { LoginDto, AuthType } from './dto/login.dto';
import { SignupDto } from './dto/signup.dto';
import { EmailLoginDto } from './dto/email-login.dto';
import { AuthResponseDto } from './dto/auth-response.dto';
import { SnowflakeValidateDto, SnowflakeRefreshDto } from './dto/snowflake-validate.dto';
import { SnowflakeSSOInitDto, SnowflakeSSOCallbackDto } from './dto/snowflake-sso-login.dto';
import { SnowflakeService } from '../connections/services/snowflake.service';
import * as crypto from 'crypto';
import * as bcrypt from 'bcrypt';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private prisma: PrismaService,
    private jwtService: JwtService,
    private configService: ConfigService,
    private snowflakeService: SnowflakeService,
  ) {}

  async login(loginDto: LoginDto): Promise<AuthResponseDto> {
    this.logger.log(`Login attempt with ${loginDto.authType}`);

    let userInfo;

    if (loginDto.authType === AuthType.MICROSOFT_SSO || loginDto.authType === AuthType.MICROSOFT) {
      userInfo = await this.validateMicrosoftToken(loginDto.accessToken);
    } else if (loginDto.authType === AuthType.SNOWFLAKE) {
      userInfo = await this.validateSnowflakeToken(loginDto.accessToken);
    } else {
      throw new UnauthorizedException('Invalid authentication type');
    }

    // Find or create user
    let user = await this.prisma.user.findUnique({
      where: { email: userInfo.email },
      include: {
        organization: true,
      },
    });

    if (!user) {
      user = await this.prisma.user.create({
        data: {
          name: userInfo.name,
          email: userInfo.email,
          authType: loginDto.authType,
        },
        include: {
          organization: true,
        },
      });
      this.logger.log(`New user created: ${user.email}`);
    }

    // Generate JWT token
    const payload = {
      sub: user.id,
      email: user.email,
      name: user.name,
      authType: user.authType,
      organizationId: user.organizationId,
    };

    const accessToken = this.jwtService.sign(payload);

    // Check if user is organization owner
    const isOrganizationOwner = user.organizationId && user.organization?.ownerId === user.id;

    const response: any = {
      token: accessToken,
      accessToken,
      tokenType: 'Bearer',
      expiresIn: 86400, // 1 day
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        authType: user.authType,
        organizationId: user.organizationId,
        onboardingCompleted: user.onboardingCompleted,
        isOrganizationOwner,
        avatarUrl: user.avatarUrl,
        createdAt: user.createdAt.toISOString(),
      },
    };

    // Include organization if user has one
    if (user.organization) {
      response.organization = {
        id: user.organization.id,
        name: user.organization.name,
        description: user.organization.description,
        plan: user.organization.plan,
        status: user.organization.status,
        ownerId: user.organization.ownerId,
        createdAt: user.organization.createdAt.toISOString(),
      };
    }

    return response;
  }

  async signupWithEmail(signupDto: SignupDto): Promise<AuthResponseDto> {
    this.logger.log(`Email signup attempt for: ${signupDto.email}`);

    // Check if user already exists
    const existingUser = await this.prisma.user.findUnique({
      where: { email: signupDto.email },
    });

    if (existingUser) {
      throw new UnauthorizedException('User with this email already exists');
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(signupDto.password, 10);

    let organizationId: string | null = null;
    let isOrganizationOwner = false;

    // Handle organization key (join existing organization)
    if (signupDto.organizationKey) {
      // Verify organization exists
      const organization = await this.prisma.organization.findUnique({
        where: { id: signupDto.organizationKey },
      });

      if (!organization) {
        throw new UnauthorizedException('Invalid organization key');
      }

      organizationId = organization.id;
      this.logger.log(`User joining organization: ${organization.name}`);
    } else if (signupDto.teamSize || signupDto.primaryUseCase) {
      // User is creating a new organization (owner)
      const organization = await this.prisma.organization.create({
        data: {
          name: `${signupDto.name}'s Organization`,
          description: `Organization for ${signupDto.name}`,
          teamSize: signupDto.teamSize,
          primaryUseCase: signupDto.primaryUseCase,
          ownerId: 'temp', // Will update after user creation
        },
      });

      organizationId = organization.id;
      isOrganizationOwner = true;
      this.logger.log(`Created new organization: ${organization.name}`);
    }

    // Create user
    const user = await this.prisma.user.create({
      data: {
        name: signupDto.name,
        email: signupDto.email,
        password: hashedPassword,
        authType: AuthType.EMAIL,
        bio: signupDto.bio,
        organizationId,
        // Set onboardingCompleted to true if user has organizationId (either creating or joining)
        onboardingCompleted: !!organizationId,
      },
      include: {
        organization: true,
      },
    });

    // If user is organization owner, update the organization
    if (isOrganizationOwner && organizationId) {
      await this.prisma.organization.update({
        where: { id: organizationId },
        data: { ownerId: user.id },
      });
    }

    // If joining existing organization, add as member
    if (organizationId && !isOrganizationOwner) {
      await this.prisma.organizationMember.create({
        data: {
          organizationId,
          userId: user.id,
          role: 'member',
        },
      });
      this.logger.log(`Added user as organization member`);
    }

    this.logger.log(`New user created with email: ${user.email}`);

    // Generate JWT token
    const payload = {
      sub: user.id,
      email: user.email,
      name: user.name,
      authType: user.authType,
      organizationId: user.organizationId,
    };

    const accessToken = this.jwtService.sign(payload);

    return {
      token: accessToken,
      accessToken,
      tokenType: 'Bearer',
      expiresIn: 86400,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        authType: user.authType,
        organizationId: user.organizationId,
        onboardingCompleted: user.onboardingCompleted,
        isOrganizationOwner,
        avatarUrl: user.avatarUrl,
        createdAt: user.createdAt.toISOString(),
      },
    };
  }

  async loginWithEmail(emailLoginDto: EmailLoginDto): Promise<AuthResponseDto> {
    this.logger.log(`Email login attempt for: ${emailLoginDto.email}`);

    // Find user
    const user = await this.prisma.user.findUnique({
      where: { email: emailLoginDto.email },
      include: {
        organization: true,
      },
    });

    if (!user) {
      throw new UnauthorizedException('Invalid email or password');
    }

    if (!user.password) {
      throw new UnauthorizedException('Password not set for this account. Please use SSO login.');
    }

    // Verify password
    const isPasswordValid = await bcrypt.compare(emailLoginDto.password, user.password);

    if (!isPasswordValid) {
      throw new UnauthorizedException('Invalid email or password');
    }

    // Generate JWT token
    const payload = {
      sub: user.id,
      email: user.email,
      name: user.name,
      authType: user.authType,
      organizationId: user.organizationId,
    };

    const accessToken = this.jwtService.sign(payload);

    // Check if user is organization owner
    const isOrganizationOwner = user.organizationId && user.organization?.ownerId === user.id;

    const response: any = {
      token: accessToken,
      accessToken,
      tokenType: 'Bearer',
      expiresIn: 86400,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        authType: user.authType,
        organizationId: user.organizationId,
        onboardingCompleted: user.onboardingCompleted,
        isOrganizationOwner,
        avatarUrl: user.avatarUrl,
        createdAt: user.createdAt.toISOString(),
      },
    };

    // Include organization if user has one
    if (user.organization) {
      response.organization = {
        id: user.organization.id,
        name: user.organization.name,
        description: user.organization.description,
        plan: user.organization.plan,
        status: user.organization.status,
        ownerId: user.organization.ownerId,
        createdAt: user.organization.createdAt.toISOString(),
      };
    }

    return response;
  }

  async validateMicrosoftToken(token: string): Promise<any> {
    this.logger.log('Validating Microsoft SSO token');

    try {
      // Option 1: Validate with Microsoft Graph API
      const graphUrl = 'https://graph.microsoft.com/v1.0/me';
      const response = await fetch(graphUrl, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        this.logger.error(`Microsoft Graph API error: ${response.status} ${response.statusText}`);
        throw new UnauthorizedException('Invalid Microsoft token - Graph API validation failed');
      }

      const userInfo = await response.json();

      this.logger.log(`Microsoft user validated: ${userInfo.userPrincipalName}`);

      return {
        name: userInfo.displayName || userInfo.givenName || 'Microsoft User',
        email: userInfo.userPrincipalName || userInfo.mail,
        microsoftId: userInfo.id,
      };
    } catch (error) {
      this.logger.error('Microsoft token validation failed', error.stack);

      // Fallback: Try to decode JWT (for testing)
      try {
        const decoded = this.decodeToken(token);
        this.logger.warn('Using fallback JWT decode for Microsoft token');
        return {
          name: decoded.name || 'Microsoft User',
          email: decoded.email || decoded.upn || decoded.unique_name || decoded.preferred_username,
          microsoftId: decoded.oid || decoded.sub,
        };
      } catch (decodeError) {
        throw new UnauthorizedException('Invalid Microsoft token');
      }
    }
  }

  async validateSnowflakeToken(token: string): Promise<any> {
    this.logger.log('Validating Snowflake OAuth token via introspection');

    try {
      const snowflakeAccount = this.configService.get('snowflakeAccount');
      const clientId = this.configService.get('snowflakeClientId');
      const clientSecret = this.configService.get('snowflakeClientSecret');

      if (!snowflakeAccount || !clientId || !clientSecret) {
        this.logger.warn('Snowflake OAuth config incomplete, falling back to JWT decode');
        return this.fallbackSnowflakeTokenDecode(token);
      }

      const tokenUrl = `https://${snowflakeAccount}.snowflakecomputing.com/oauth/token-request`;
      const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

      const params = new URLSearchParams();
      params.append('grant_type', 'urn:ietf:params:oauth:grant-type:token-exchange');
      params.append('subject_token', token);
      params.append('subject_token_type', 'urn:ietf:params:oauth:token-type:access_token');

      const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${credentials}`,
        },
        body: params.toString(),
      });

      if (!response.ok) {
        const errorText = await response.text();
        this.logger.error(`Snowflake token introspection failed: ${response.status} - ${errorText}`);
        this.logger.warn('Falling back to JWT decode for Snowflake token');
        return this.fallbackSnowflakeTokenDecode(token);
      }

      const tokenData = await response.json();
      this.logger.log(`Snowflake token validated successfully for user: ${tokenData.username}`);

      return {
        name: tokenData.user_first_name && tokenData.user_last_name
          ? `${tokenData.user_first_name} ${tokenData.user_last_name}`
          : tokenData.username || 'Snowflake User',
        email: tokenData.username?.includes('@')
          ? tokenData.username
          : `${tokenData.username}@snowflake.local`,
        snowflakeId: tokenData.username,
      };
    } catch (error) {
      this.logger.error('Snowflake token validation failed', error);

      try {
        return this.fallbackSnowflakeTokenDecode(token);
      } catch (decodeError) {
        throw new UnauthorizedException('Invalid Snowflake token');
      }
    }
  }

  private fallbackSnowflakeTokenDecode(token: string): any {
    this.logger.warn('Using fallback JWT decode for Snowflake token');
    const decoded = this.decodeToken(token);
    return {
      name: decoded.name || decoded.given_name || 'Snowflake User',
      email: decoded.email || decoded.upn || `${decoded.sub}@snowflake.local`,
      snowflakeId: decoded.sub,
    };
  }

  async validateSnowflakeCode(validateDto: SnowflakeValidateDto): Promise<any> {
    this.logger.log('Validating Snowflake authorization code');

    try {
      const tokenUrl = `https://${this.configService.get('snowflakeAccount')}/oauth/token-request`;

      const params = new URLSearchParams();
      params.append('grant_type', 'authorization_code');
      params.append('code', validateDto.code);
      params.append('redirect_uri', this.configService.get('snowflakeRedirectUri'));

      const credentials = Buffer.from(
        `${this.configService.get('snowflakeClientId')}:${this.configService.get('snowflakeClientSecret')}`,
      ).toString('base64');

      const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${credentials}`,
        },
        body: params.toString(),
      });

      if (!response.ok) {
        const errorText = await response.text();
        this.logger.error(`Snowflake OAuth error: ${response.status} - ${errorText}`);
        throw new UnauthorizedException('Snowflake authorization code validation failed');
      }

      const tokenData = await response.json();

      this.logger.log('Snowflake authorization code validated successfully');

      return {
        accessToken: tokenData.access_token,
        refreshToken: tokenData.refresh_token,
        expiresIn: tokenData.expires_in,
        tokenType: tokenData.token_type,
      };
    } catch (error) {
      this.logger.error('Snowflake code validation failed', error.stack);
      throw new UnauthorizedException(`Snowflake OAuth error: ${error.message}`);
    }
  }

  async refreshSnowflakeToken(refreshDto: SnowflakeRefreshDto): Promise<any> {
    this.logger.log('Refreshing Snowflake token');

    try {
      const tokenUrl = `https://${this.configService.get('snowflakeAccount')}/oauth/token-request`;

      const params = new URLSearchParams();
      params.append('grant_type', 'refresh_token');
      params.append('refresh_token', refreshDto.refreshToken);

      const credentials = Buffer.from(
        `${this.configService.get('snowflakeClientId')}:${this.configService.get('snowflakeClientSecret')}`,
      ).toString('base64');

      const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${credentials}`,
        },
        body: params.toString(),
      });

      if (!response.ok) {
        const errorText = await response.text();
        this.logger.error(`Snowflake token refresh error: ${response.status} - ${errorText}`);
        throw new UnauthorizedException('Snowflake token refresh failed');
      }

      const tokenData = await response.json();

      this.logger.log('Snowflake token refreshed successfully');

      return {
        accessToken: tokenData.access_token,
        refreshToken: tokenData.refresh_token,
        expiresIn: tokenData.expires_in,
        tokenType: tokenData.token_type,
      };
    } catch (error) {
      this.logger.error('Snowflake token refresh failed', error.stack);
      throw new UnauthorizedException(`Snowflake token refresh error: ${error.message}`);
    }
  }

  async validateUser(userId: string): Promise<any> {
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
    });

    if (!user) {
      throw new UnauthorizedException('User not found');
    }

    return user;
  }

  /**
   * Initialize Snowflake SSO login
   */
  async initSnowflakeSSO(initDto: SnowflakeSSOInitDto) {
    try {
      const authUrl = this.snowflakeService.generateAuthUrl(
        this.configService.get('SNOWFLAKE_CLIENT_ID'),
        initDto.snowflakeAccount,
        this.configService.get('SNOWFLAKE_REDIRECT_URI'),
      );

      return {
        authorizationUrl: authUrl,
        message: 'Redirect user to this URL to initiate Snowflake SSO',
      };
    } catch (error) {
      this.logger.error('Failed to initialize Snowflake SSO', error);
      throw new UnauthorizedException('Failed to initialize Snowflake SSO');
    }
  }

  /**
   * Handle Snowflake SSO callback and login user
   */
  async snowflakeSSOCallback(callbackDto: SnowflakeSSOCallbackDto): Promise<AuthResponseDto> {
    try {
      const snowflakeAccount =
        callbackDto.snowflakeAccount || this.configService.get('SNOWFLAKE_ACCOUNT');

      // Exchange code for tokens
      const tokens = await this.snowflakeService.exchangeCodeForTokens(
        callbackDto.code,
        this.configService.get('SNOWFLAKE_CLIENT_ID'),
        this.configService.get('SNOWFLAKE_CLIENT_SECRET'),
        snowflakeAccount,
        this.configService.get('SNOWFLAKE_REDIRECT_URI'),
      );

      // Find or create user based on Snowflake username (email)
      const email = tokens.username.toLowerCase();
      const name =
        tokens.userFirstName && tokens.userLastName
          ? `${tokens.userFirstName} ${tokens.userLastName}`
          : tokens.username;

      let user = await this.prisma.user.findUnique({
        where: { email },
        include: { organization: true },
      });

      const accessExpiryTime = new Date(Date.now() + tokens.expiresIn * 1000);
      const refreshExpiryTime = new Date(Date.now() + tokens.refreshTokenExpiresIn * 1000);

      if (!user) {
        // Create new user with Snowflake auth
        user = await this.prisma.user.create({
          data: {
            name,
            email,
            authType: AuthType.SNOWFLAKE,
            accessToken: tokens.accessToken,
            refreshToken: tokens.refreshToken,
            accessExpiryTime,
            refreshExpiryTime,
          },
          include: { organization: true },
        });
        this.logger.log(`New user created via Snowflake SSO: ${user.email}`);
      } else {
        // Update existing user with new tokens
        user = await this.prisma.user.update({
          where: { id: user.id },
          data: {
            authType: AuthType.SNOWFLAKE,
            accessToken: tokens.accessToken,
            refreshToken: tokens.refreshToken,
            accessExpiryTime,
            refreshExpiryTime,
          },
          include: { organization: true },
        });
        this.logger.log(`Existing user logged in via Snowflake SSO: ${user.email}`);
      }

      // Generate JWT token for our application
      const payload = {
        sub: user.id,
        email: user.email,
        name: user.name,
        authType: user.authType,
        organizationId: user.organizationId,
      };

      const accessToken = this.jwtService.sign(payload);

      const response: any = {
        token: accessToken,
        accessToken,
        tokenType: 'Bearer',
        expiresIn: 86400,
        user: {
          id: user.id,
          name: user.name,
          email: user.email,
          authType: user.authType,
          organizationId: user.organizationId,
          onboardingCompleted: user.onboardingCompleted,
          avatarUrl: user.avatarUrl,
          createdAt: user.createdAt.toISOString(),
        },
        snowflake: {
          username: tokens.username,
          scope: tokens.scope,
        },
      };

      if (user.organization) {
        response.organization = {
          id: user.organization.id,
          name: user.organization.name,
          description: user.organization.description,
          plan: user.organization.plan,
          status: user.organization.status,
          ownerId: user.organization.ownerId,
          createdAt: user.organization.createdAt.toISOString(),
        };
      }

      return response;
    } catch (error) {
      this.logger.error('Snowflake SSO callback failed', error);
      throw new UnauthorizedException('Snowflake SSO login failed: ' + error.message);
    }
  }

  private decodeToken(token: string): any {
    try {
      const parts = token.split('.');
      if (parts.length !== 3) {
        throw new Error('Invalid token format');
      }

      const payload = Buffer.from(parts[1], 'base64').toString('utf-8');
      return JSON.parse(payload);
    } catch (error) {
      throw new UnauthorizedException('Invalid token');
    }
  }
}
