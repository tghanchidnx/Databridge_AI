import { IsString, IsEmail, IsNotEmpty, IsOptional } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CreateUserDto {
  @ApiProperty({ description: 'User name', example: 'John Doe' })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiProperty({ description: 'User email', example: 'john@example.com' })
  @IsEmail()
  @IsNotEmpty()
  email: string;

  @ApiProperty({
    description: 'User avatar URL',
    example: 'https://example.com/avatar.jpg',
    required: false,
  })
  @IsString()
  @IsOptional()
  avatarUrl?: string;

  @ApiProperty({ description: 'User bio', example: 'Data engineer', required: false })
  @IsString()
  @IsOptional()
  bio?: string;

  @ApiProperty({ description: 'Team size', example: '5-10 people', required: false })
  @IsString()
  @IsOptional()
  teamSize?: string;

  @ApiProperty({ description: 'Primary use case', example: 'Schema comparison', required: false })
  @IsString()
  @IsOptional()
  primaryUseCase?: string;

  @ApiProperty({
    description: 'Authentication type',
    example: 'Microsoft SSO',
    required: false,
  })
  @IsString()
  @IsOptional()
  authType?: string;
}
