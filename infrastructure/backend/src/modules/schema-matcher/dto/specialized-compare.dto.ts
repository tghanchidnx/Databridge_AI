import { IsString, IsNotEmpty, IsOptional, IsEnum } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export enum ComparisonType {
  DATABASE_TO_DATABASE = 'D2D',
  DATABASE_TO_SNOWFLAKE = 'D2S',
  SNOWFLAKE_TO_DATABASE = 'S2D',
  SNOWFLAKE_TO_SNOWFLAKE = 'S2S',
}

export class SpecializedCompareDto {
  @ApiProperty({ description: 'Type of comparison', enum: ComparisonType })
  @IsEnum(ComparisonType)
  @IsNotEmpty()
  comparisonType: ComparisonType;

  @ApiProperty({ description: 'Source connection ID' })
  @IsString()
  @IsNotEmpty()
  sourceConnectionId: string;

  @ApiProperty({ description: 'Target connection ID' })
  @IsString()
  @IsNotEmpty()
  targetConnectionId: string;

  @ApiProperty({ description: 'Source database' })
  @IsString()
  @IsNotEmpty()
  sourceDatabase: string;

  @ApiProperty({ description: 'Source schema' })
  @IsString()
  @IsNotEmpty()
  sourceSchema: string;

  @ApiProperty({ description: 'Target database' })
  @IsString()
  @IsNotEmpty()
  targetDatabase: string;

  @ApiProperty({ description: 'Target schema' })
  @IsString()
  @IsNotEmpty()
  targetSchema: string;

  @ApiProperty({ description: 'Job name', required: false })
  @IsString()
  @IsOptional()
  jobName?: string;

  @ApiProperty({ description: 'Include DDL in comparison', required: false, default: true })
  @IsOptional()
  includeDdl?: boolean;

  @ApiProperty({ description: 'Include column details', required: false, default: true })
  @IsOptional()
  includeColumns?: boolean;
}
