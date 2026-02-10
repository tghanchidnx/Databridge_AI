import { IsString, IsNotEmpty, IsOptional } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class SnowflakeSSOInitDto {
  @ApiProperty({
    description: 'Snowflake Account Identifier',
    example: 'zf07542.south-central-us.azure',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeAccount: string;
}

export class SnowflakeSSOCallbackDto {
  @ApiProperty({
    description: 'Authorization code from Snowflake OAuth callback',
    example: 'auth_code_xyz123',
  })
  @IsString()
  @IsNotEmpty()
  code: string;

  @ApiProperty({
    description: 'Snowflake Account Identifier',
    example: 'zf07542.south-central-us.azure',
    required: false,
  })
  @IsString()
  @IsOptional()
  snowflakeAccount?: string;
}

export class SnowflakeTokenResponseDto {
  @ApiProperty()
  accessToken: string;

  @ApiProperty()
  refreshToken: string;

  @ApiProperty()
  expiresIn: number;

  @ApiProperty()
  refreshTokenExpiresIn: number;

  @ApiProperty()
  tokenType: string;

  @ApiProperty()
  username: string;

  @ApiProperty({ required: false })
  userFirstName?: string;

  @ApiProperty({ required: false })
  userLastName?: string;

  @ApiProperty({ required: false })
  scope?: string;
}
