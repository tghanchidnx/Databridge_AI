import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNotEmpty, IsEnum, IsOptional, IsUUID } from 'class-validator';

export class CreateOrganizationDto {
  @ApiProperty({ example: 'Acme Corp' })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiProperty({ example: 'free', enum: ['free', 'pro', 'enterprise'] })
  @IsEnum(['free', 'pro', 'enterprise'])
  plan: 'free' | 'pro' | 'enterprise';

  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  description?: string;
}

export class UpdateOrganizationDto {
  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  name?: string;

  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  description?: string;

  @ApiProperty({ required: false, enum: ['free', 'pro', 'enterprise'] })
  @IsEnum(['free', 'pro', 'enterprise'])
  @IsOptional()
  plan?: 'free' | 'pro' | 'enterprise';
}

export class AddMemberDto {
  @ApiProperty()
  @IsUUID()
  userId: string;

  @ApiProperty({ enum: ['owner', 'admin', 'member'] })
  @IsEnum(['owner', 'admin', 'member'])
  role: 'owner' | 'admin' | 'member';
}
