import { IsString, IsNotEmpty } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class GetTablesDto {
  @ApiProperty({ description: 'Connection ID' })
  @IsString()
  @IsNotEmpty()
  connectionId: string;

  @ApiProperty({ description: 'Database name' })
  @IsString()
  @IsNotEmpty()
  database: string;

  @ApiProperty({ description: 'Schema name' })
  @IsString()
  @IsNotEmpty()
  schema: string;
}
