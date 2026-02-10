import { IsString, IsNotEmpty, IsEnum } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export enum AuthType {
  MICROSOFT_SSO = 'MICROSOFT_SSO',
  MICROSOFT = 'MICROSOFT',
  SNOWFLAKE = 'Snowflake',
  EMAIL = 'EMAIL',
}

export class LoginDto {
  @ApiProperty({
    description: 'Authentication type',
    enum: AuthType,
    example: AuthType.MICROSOFT_SSO,
  })
  @IsEnum(AuthType)
  @IsNotEmpty()
  authType: AuthType;

  @ApiProperty({
    description: 'Access token from authentication provider',
    example: 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...',
  })
  @IsString()
  @IsNotEmpty()
  accessToken: string;
}
