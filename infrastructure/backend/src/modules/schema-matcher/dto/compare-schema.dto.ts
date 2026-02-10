import { IsString, IsNotEmpty, IsOptional, IsArray } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CompareSchemaDto {
  @ApiProperty({ description: 'Source connection ID' })
  @IsString()
  @IsNotEmpty()
  sourceConnectionId: string;

  @ApiProperty({ description: 'Source database name' })
  @IsString()
  @IsNotEmpty()
  sourceDatabase: string;

  @ApiProperty({ description: 'Source schema name' })
  @IsString()
  @IsNotEmpty()
  sourceSchema: string;

  @ApiProperty({ description: 'Target connection ID' })
  @IsString()
  @IsNotEmpty()
  targetConnectionId: string;

  @ApiProperty({ description: 'Target database name' })
  @IsString()
  @IsNotEmpty()
  targetDatabase: string;

  @ApiProperty({ description: 'Target schema name' })
  @IsString()
  @IsNotEmpty()
  targetSchema: string;

  @ApiProperty({ description: 'Tables to compare (optional)', required: false })
  @IsArray()
  @IsOptional()
  tables?: string[];
}
