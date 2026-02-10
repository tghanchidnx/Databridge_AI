import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNotEmpty, IsOptional, IsArray } from 'class-validator';

export class ChatMessageDto {
  @ApiProperty({ example: 'How do I connect to Snowflake?' })
  @IsString()
  @IsNotEmpty()
  message: string;

  @ApiProperty({ required: false, example: 'workspace_123' })
  @IsString()
  @IsOptional()
  workspaceId?: string;

  @ApiProperty({ required: false, type: [Object] })
  @IsArray()
  @IsOptional()
  context?: Array<{ role: string; content: string }>;
}

export class GenerateSqlDto {
  @ApiProperty({ example: 'Get all customers who made purchases last month' })
  @IsString()
  @IsNotEmpty()
  query: string;

  @ApiProperty({ required: false, example: 'snowflake' })
  @IsString()
  @IsOptional()
  databaseType?: string;

  @ApiProperty({ required: false, type: [String] })
  @IsArray()
  @IsOptional()
  tableSchemas?: string[];
}

export class ExplainQueryDto {
  @ApiProperty({ example: 'SELECT * FROM customers WHERE created_at > NOW() - INTERVAL 30 DAY' })
  @IsString()
  @IsNotEmpty()
  query: string;
}
