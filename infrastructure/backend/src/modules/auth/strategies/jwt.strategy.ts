import { Injectable, UnauthorizedException } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { ConfigService } from '@nestjs/config';
import { AuthService } from '../auth.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    private configService: ConfigService,
    private authService: AuthService,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: configService.get<string>('jwt.secret'),
    });
  }

  async validate(payload: any) {
    console.log('JWT Strategy: validate called with payload:', payload);

    const user = await this.authService.validateUser(payload.sub);

    if (!user) {
      console.log('JWT Strategy: User not found for sub:', payload.sub);
      throw new UnauthorizedException();
    }

    const result = {
      sub: payload.sub, // Changed from 'id' to 'sub' to match controller usage
      id: payload.sub, // Keep id as alias for backwards compatibility
      email: payload.email,
      name: payload.name,
      authType: payload.authType,
    };

    console.log('JWT Strategy: Returning user object:', result);
    return result;
  }
}
