import { IsString, IsOptional, IsArray, IsNumber, Min, Max } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CompareDataDto {
  @ApiProperty({
    description: 'Source connection ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @IsString()
  sourceConnectionId: string;

  @ApiProperty({
    description: 'Target connection ID',
    example: '123e4567-e89b-12d3-a456-426614174001',
  })
  @IsString()
  targetConnectionId: string;

  @ApiProperty({
    description: 'Fully qualified source table name (DATABASE.SCHEMA.TABLE)',
    example: 'ANALYTICS_DB.PUBLIC.CUSTOMERS',
  })
  @IsString()
  sourceTable: string;

  @ApiProperty({
    description: 'Fully qualified target table name (DATABASE.SCHEMA.TABLE)',
    example: 'ANALYTICS_DB.PUBLIC.CUSTOMERS_COPY',
  })
  @IsString()
  targetTable: string;

  @ApiProperty({
    description:
      'Specific columns to compare (optional - compares all common columns if not specified)',
    example: ['CUSTOMER_ID', 'NAME', 'EMAIL'],
    required: false,
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  columns?: string[];

  @ApiProperty({
    description: 'Number of rows to sample for comparison',
    example: 1000,
    default: 1000,
    minimum: 1,
    maximum: 10000,
    required: false,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(10000)
  sampleSize?: number;

  @ApiProperty({
    description: 'Tolerance for numeric value differences',
    example: 0.01,
    default: 0,
    minimum: 0,
    required: false,
  })
  @IsOptional()
  @IsNumber()
  @Min(0)
  tolerance?: number;
}
