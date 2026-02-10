import { IsString, IsNotEmpty } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class SnowflakeValidateDto {
  @ApiProperty({
    description: 'Authorization code from Snowflake OAuth',
    example: 'auth_code_123456',
  })
  @IsString()
  @IsNotEmpty()
  code: string;
}

export class SnowflakeRefreshDto {
  @ApiProperty({
    description: 'Refresh token from Snowflake',
    example: 'refresh_token_123456',
  })
  @IsString()
  @IsNotEmpty()
  refreshToken: string;
}
