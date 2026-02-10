import { IsString, IsNotEmpty, IsOptional, IsArray, IsBoolean } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';

export class MergeTablesDto {
  @ApiProperty({ description: 'Connection ID' })
  @IsString()
  @IsNotEmpty()
  connectionId: string;

  @ApiProperty({ description: 'Source table (fully qualified: database.schema.table)' })
  @IsString()
  @IsNotEmpty()
  sourceTable: string;

  @ApiProperty({ description: 'Target table (fully qualified: database.schema.table)' })
  @IsString()
  @IsNotEmpty()
  targetTable: string;

  @ApiProperty({
    description:
      'Columns to use for merge (optional - will auto-detect common columns if not provided)',
    required: false,
    type: [String],
  })
  @IsArray()
  @IsOptional()
  @Type(() => String)
  mergeColumns?: string[];

  @ApiProperty({
    description:
      'Columns to use as join keys (optional - will use first common column if not provided)',
    required: false,
    type: [String],
  })
  @IsArray()
  @IsOptional()
  @Type(() => String)
  joinKeys?: string[];

  @ApiProperty({
    description: 'Execute the merge immediately (default: false - returns SQL only)',
    required: false,
    default: false,
  })
  @IsBoolean()
  @IsOptional()
  executeImmediately?: boolean;
}
