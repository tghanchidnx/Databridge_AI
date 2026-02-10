import { Controller, Post, Body } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';
import { AuthService } from './auth.service';
import { LoginDto } from './dto/login.dto';
import { SignupDto } from './dto/signup.dto';
import { EmailLoginDto } from './dto/email-login.dto';
import { AuthResponseDto } from './dto/auth-response.dto';
import { SnowflakeValidateDto, SnowflakeRefreshDto } from './dto/snowflake-validate.dto';
import { SnowflakeSSOInitDto, SnowflakeSSOCallbackDto } from './dto/snowflake-sso-login.dto';
import { Public } from '../../common/decorators/public.decorator';

@ApiTags('Authentication')
@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('login')
  @Public()
  @ApiOperation({ summary: 'User login with email/password or SSO (auto-detect)' })
  @ApiResponse({ status: 200, description: 'Login successful', type: AuthResponseDto })
  @ApiResponse({ status: 401, description: 'Unauthorized' })
  async login(@Body() body: any): Promise<AuthResponseDto> {
    // Auto-detect login type based on body content
    if (body.email && body.password) {
      // Email/password login
      const emailLoginDto: EmailLoginDto = {
        email: body.email,
        password: body.password,
      };
      return this.authService.loginWithEmail(emailLoginDto);
    } else if (body.authType && body.accessToken) {
      // SSO login
      const loginDto: LoginDto = body;
      return this.authService.login(loginDto);
    } else {
      throw new Error(
        'Invalid login request. Provide either (email, password) for email login or (authType, accessToken) for SSO login.',
      );
    }
  }

  @Post('signup')
  @Public()
  @ApiOperation({ summary: 'User signup (auto-detects email or SSO)' })
  @ApiResponse({ status: 200, description: 'Signup successful', type: AuthResponseDto })
  @ApiResponse({ status: 401, description: 'Unauthorized' })
  async signup(@Body() body: any): Promise<AuthResponseDto> {
    // Auto-detect signup type based on body content
    if (body.email && body.password && body.name) {
      // Email signup
      const signupDto: SignupDto = {
        name: body.name,
        email: body.email,
        password: body.password,
        organizationKey: body.organizationKey,
        bio: body.bio,
        teamSize: body.teamSize,
        primaryUseCase: body.primaryUseCase,
      };
      return this.authService.signupWithEmail(signupDto);
    } else if (body.authType && body.accessToken) {
      // SSO signup
      const loginDto: LoginDto = body;
      return this.authService.login(loginDto);
    } else {
      throw new Error(
        'Invalid signup request. Provide either (name, email, password) for email signup or (authType, accessToken) for SSO signup.',
      );
    }
  }

  @Post('signup/email')
  @Public()
  @ApiOperation({ summary: 'Signup with email and password' })
  @ApiResponse({ status: 200, description: 'Signup successful', type: AuthResponseDto })
  @ApiResponse({ status: 400, description: 'User already exists' })
  async signupWithEmail(@Body() signupDto: SignupDto): Promise<AuthResponseDto> {
    return this.authService.signupWithEmail(signupDto);
  }

  @Post('login/email')
  @Public()
  @ApiOperation({ summary: 'Login with email and password' })
  @ApiResponse({ status: 200, description: 'Login successful', type: AuthResponseDto })
  @ApiResponse({ status: 401, description: 'Invalid credentials' })
  async loginWithEmail(@Body() emailLoginDto: EmailLoginDto): Promise<AuthResponseDto> {
    return this.authService.loginWithEmail(emailLoginDto);
  }

  @Post('snowflake/validate')
  @Public()
  @ApiOperation({ summary: 'Validate Snowflake authorization code' })
  @ApiResponse({ status: 200, description: 'Token validated successfully' })
  @ApiResponse({ status: 401, description: 'Invalid authorization code' })
  async validateSnowflake(@Body() validateDto: SnowflakeValidateDto) {
    return this.authService.validateSnowflakeCode(validateDto);
  }

  @Post('snowflake/refresh')
  @Public()
  @ApiOperation({ summary: 'Refresh Snowflake access token' })
  @ApiResponse({ status: 200, description: 'Token refreshed successfully' })
  @ApiResponse({ status: 401, description: 'Invalid refresh token' })
  async refreshSnowflake(@Body() refreshDto: SnowflakeRefreshDto) {
    return this.authService.refreshSnowflakeToken(refreshDto);
  }

  @Post('snowflake/sso/init')
  @Public()
  @ApiOperation({ summary: 'Initialize Snowflake SSO login' })
  @ApiResponse({ status: 200, description: 'Authorization URL generated' })
  async initSnowflakeSSO(@Body() initDto: SnowflakeSSOInitDto) {
    return this.authService.initSnowflakeSSO(initDto);
  }

  @Post('snowflake/sso/callback')
  @Public()
  @ApiOperation({ summary: 'Handle Snowflake SSO callback and login user' })
  @ApiResponse({ status: 200, description: 'Login successful', type: AuthResponseDto })
  @ApiResponse({ status: 401, description: 'Authentication failed' })
  async snowflakeSSOCallback(
    @Body() callbackDto: SnowflakeSSOCallbackDto,
  ): Promise<AuthResponseDto> {
    return this.authService.snowflakeSSOCallback(callbackDto);
  }
}
