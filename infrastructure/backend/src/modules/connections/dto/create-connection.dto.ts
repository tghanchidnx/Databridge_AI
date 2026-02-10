import { IsString, IsNotEmpty, IsObject, IsOptional } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CreateConnectionDto {
  @ApiProperty({ description: 'Connection name', example: 'Production Snowflake' })
  @IsString()
  @IsNotEmpty()
  connectionName: string;

  @ApiProperty({ description: 'Connection type', example: 'Snowflake' })
  @IsString()
  @IsNotEmpty()
  connectionType: string;

  @ApiProperty({
    description: 'Connection credentials (encrypted)',
    example: { account: 'account', username: 'user', password: 'pass' },
  })
  @IsObject()
  @IsNotEmpty()
  credentials: Record<string, any>;

  @ApiProperty({ description: 'Connection description', required: false })
  @IsString()
  @IsOptional()
  description?: string;
}
