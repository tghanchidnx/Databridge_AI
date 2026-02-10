import { IsString, IsOptional, IsBoolean } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class GenerateReportDto {
  @ApiProperty({
    description: 'Connection ID to generate report for',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @IsString()
  connectionId: string;

  @ApiProperty({
    description: 'Database name',
    example: 'ANALYTICS_DB',
  })
  @IsString()
  database: string;

  @ApiProperty({
    description: 'Specific schema to report on (optional - reports all schemas if not specified)',
    example: 'PUBLIC',
    required: false,
  })
  @IsOptional()
  @IsString()
  schema?: string;

  @ApiProperty({
    description: 'Include row counts for each table',
    example: true,
    default: false,
    required: false,
  })
  @IsOptional()
  @IsBoolean()
  includeRowCounts?: boolean;

  @ApiProperty({
    description: 'Include detailed column information',
    example: true,
    default: true,
    required: false,
  })
  @IsOptional()
  @IsBoolean()
  includeColumnDetails?: boolean;

  @ApiProperty({
    description: 'Include sample data from tables',
    example: false,
    default: false,
    required: false,
  })
  @IsOptional()
  @IsBoolean()
  includeSampleData?: boolean;
}
