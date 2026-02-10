import { IsString, IsEmail, IsNotEmpty, MinLength, IsOptional } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class SignupDto {
  @ApiProperty({
    description: 'User name',
    example: 'John Doe',
  })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiProperty({
    description: 'User email',
    example: 'john@example.com',
  })
  @IsEmail()
  @IsNotEmpty()
  email: string;

  @ApiProperty({
    description: 'User password',
    example: 'SecurePassword123',
  })
  @IsString()
  @IsNotEmpty()
  @MinLength(8)
  password: string;

  @ApiPropertyOptional({
    description: 'Organization ID to join existing organization',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @IsString()
  @IsOptional()
  organizationKey?: string;

  @ApiPropertyOptional({
    description: 'User bio',
    example: 'Data engineer with 5 years of experience',
  })
  @IsString()
  @IsOptional()
  bio?: string;

  @ApiPropertyOptional({
    description: 'Team size (for organization owners)',
    example: '10-50',
  })
  @IsString()
  @IsOptional()
  teamSize?: string;

  @ApiPropertyOptional({
    description: 'Primary use case (for organization owners)',
    example: 'Data Warehousing',
  })
  @IsString()
  @IsOptional()
  primaryUseCase?: string;
}
