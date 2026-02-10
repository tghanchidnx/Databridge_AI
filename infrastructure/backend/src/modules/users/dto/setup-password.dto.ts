import { IsString, IsNotEmpty, MinLength } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class SetupPasswordDto {
  @ApiProperty({ description: 'New password for SSO users' })
  @IsString()
  @IsNotEmpty()
  @MinLength(8)
  newPassword: string;
}
