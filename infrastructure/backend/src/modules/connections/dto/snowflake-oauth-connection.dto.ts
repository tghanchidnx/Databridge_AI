import { IsString, IsNotEmpty, IsOptional } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class SnowflakeOAuthInitDto {
  @ApiProperty({
    description: 'Snowflake Client ID',
    example: 'YOUR_CLIENT_ID',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeClientId: string;

  @ApiProperty({
    description: 'Snowflake Client Secret',
    example: 'YOUR_CLIENT_SECRET',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeClientSecret: string;

  @ApiProperty({
    description: 'Snowflake Account Identifier',
    example: 'zf07542.south-central-us.azure',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeAccount: string;

  @ApiProperty({
    description: 'Connection name for this Snowflake connection',
    example: 'My Snowflake Prod',
  })
  @IsString()
  @IsNotEmpty()
  connectionName: string;
}

export class SnowflakeOAuthCallbackDto {
  @ApiProperty({
    description: 'Authorization code from Snowflake OAuth callback',
    example: 'auth_code_xyz123',
  })
  @IsString()
  @IsNotEmpty()
  code: string;

  @ApiProperty({
    description: 'Connection name for this Snowflake connection',
    example: 'My Snowflake Prod',
  })
  @IsString()
  @IsNotEmpty()
  connectionName: string;

  @ApiProperty({
    description: 'Snowflake Client ID',
    example: 'YOUR_CLIENT_ID',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeClientId: string;

  @ApiProperty({
    description: 'Snowflake Client Secret',
    example: 'YOUR_CLIENT_SECRET',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeClientSecret: string;

  @ApiProperty({
    description: 'Snowflake Account Identifier',
    example: 'zf07542.south-central-us.azure',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeAccount: string;

  @ApiProperty({
    description: 'Snowflake Warehouse',
    example: 'COMPUTE_WH',
  })
  @IsString()
  @IsOptional()
  snowflakeWarehouse?: string;

  @ApiProperty({
    description: 'Snowflake Database',
    example: 'TRANSFORMATION',
  })
  @IsString()
  @IsOptional()
  databaseName?: string;

  @ApiProperty({
    description: 'Snowflake Schema',
    example: 'PUBLIC',
  })
  @IsString()
  @IsOptional()
  schemaName?: string;
}

export class SnowflakePasswordConnectionDto {
  @ApiProperty({
    description: 'Connection name for this Snowflake connection',
    example: 'My Snowflake Dev',
  })
  @IsString()
  @IsNotEmpty()
  connectionName: string;

  @ApiProperty({
    description: 'Snowflake username',
    example: 'john_doe',
  })
  @IsString()
  @IsNotEmpty()
  username: string;

  @ApiProperty({
    description: 'Snowflake password',
    example: 'MySecurePassword123!',
  })
  @IsString()
  @IsNotEmpty()
  password: string;

  @ApiProperty({
    description: 'Snowflake Account Identifier',
    example: 'zf07542.south-central-us.azure',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeAccount: string;

  @ApiProperty({
    description: 'Snowflake Warehouse',
    example: 'COMPUTE_WH',
  })
  @IsString()
  @IsNotEmpty()
  snowflakeWarehouse: string;

  @ApiProperty({
    description: 'Snowflake Database',
    example: 'TRANSFORMATION',
  })
  @IsString()
  @IsNotEmpty()
  databaseName: string;

  @ApiProperty({
    description: 'Snowflake Schema',
    example: 'PUBLIC',
  })
  @IsString()
  @IsOptional()
  schemaName?: string;
}

export class RefreshSnowflakeTokenDto {
  @ApiProperty({
    description: 'Connection ID to refresh token for',
    example: 'uuid-string',
  })
  @IsString()
  @IsNotEmpty()
  connectionId: string;
}
