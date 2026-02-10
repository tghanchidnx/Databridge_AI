import { Injectable, ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import { Reflector } from '@nestjs/core';

export const IS_PUBLIC_KEY = 'isPublic';

@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {
  constructor(private reflector: Reflector) {
    super();
  }

  canActivate(context: ExecutionContext) {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    if (isPublic) {
      return true;
    }

    // Check for API Key authentication (for MCP integration)
    const request = context.switchToHttp().getRequest();
    const apiKey = request.headers['x-api-key'];

    if (apiKey) {
      const validKeys = (process.env.API_KEYS || '').split(',').map(k => k.trim()).filter(Boolean);
      if (validKeys.includes(apiKey)) {
        // Set the MCP system user for API key requests
        // This user ID must exist in the database - run: mysql -u root -padmin dataamplifier < prisma/seed-mcp-user.sql
        // Using a fixed ID ensures MCP-created projects are accessible by this user in the UI
        const mcpUserId = process.env.MCP_USER_ID || 'mcp-system-user-001';
        request.user = {
          id: mcpUserId,
          email: 'mcp@dataamplifier.local',
          name: 'MCP System',
          isApiKeyAuth: true,
        };
        console.log('JwtAuthGuard: API Key authentication successful for MCP user:', mcpUserId);
        return true;
      } else {
        console.error('JwtAuthGuard: Invalid API Key provided');
        throw new UnauthorizedException('Invalid API Key');
      }
    }

    // Call parent AuthGuard which will use JwtStrategy
    return super.canActivate(context);
  }

  handleRequest(err: any, user: any, info: any) {
    console.log('JwtAuthGuard: handleRequest called', { err, user, info });

    if (err || !user) {
      console.error('JwtAuthGuard: Authentication failed', { err, info });
      throw err || new UnauthorizedException('Invalid token or user not found');
    }

    console.log('JwtAuthGuard: Authentication successful, user:', user);
    return user;
  }
}
