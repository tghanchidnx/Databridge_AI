import { ApiProperty } from '@nestjs/swagger';

export class AuthResponseDto {
  @ApiProperty({ description: 'JWT access token' })
  accessToken: string;

  @ApiProperty({ description: 'JWT access token (alias)' })
  token?: string;

  @ApiProperty({ description: 'Token type', example: 'Bearer' })
  tokenType: string;

  @ApiProperty({ description: 'Token expiration in seconds', example: 3600 })
  expiresIn: number;

  @ApiProperty({ description: 'User information' })
  user: {
    id: string;
    name: string;
    email: string;
    authType: string;
    organizationId?: string;
    onboardingCompleted?: boolean;
    isOrganizationOwner?: boolean;
    avatarUrl?: string;
    createdAt?: string;
  };
}
