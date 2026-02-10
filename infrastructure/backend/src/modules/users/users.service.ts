import {
  Injectable,
  NotFoundException,
  ConflictException,
  UnauthorizedException,
  Logger,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../database/prisma/prisma.service';
import { CreateUserDto } from './dto/create-user.dto';
import { UpdateUserDto } from './dto/update-user.dto';
import { ChangePasswordDto } from './dto/change-password.dto';
import { SetupPasswordDto } from './dto/setup-password.dto';
import * as bcrypt from 'bcrypt';

@Injectable()
export class UsersService {
  private readonly logger = new Logger(UsersService.name);

  constructor(
    private prisma: PrismaService,
    private configService: ConfigService,
  ) {}

  async create(createUserDto: CreateUserDto) {
    const existingUser = await this.prisma.user.findUnique({
      where: { email: createUserDto.email },
    });

    if (existingUser) {
      throw new ConflictException('User with this email already exists');
    }

    const user = await this.prisma.user.create({
      data: {
        name: createUserDto.name,
        email: createUserDto.email,
        authType: createUserDto.authType || 'Microsoft SSO',
      },
    });

    this.logger.log(`User created: ${user.email}`);
    return user;
  }

  async findAll() {
    return this.prisma.user.findMany({
      select: {
        id: true,
        name: true,
        email: true,
        authType: true,
        createdAt: true,
        updatedAt: true,
      },
    });
  }

  async findOne(id: string) {
    const user = await this.prisma.user.findUnique({
      where: { id },
      include: {
        connections: {
          select: {
            id: true,
            connectionName: true,
            connectionType: true,
            createdAt: true,
          },
        },
      },
    });

    if (!user) {
      throw new NotFoundException(`User with ID ${id} not found`);
    }

    return user;
  }

  async update(id: string, updateUserDto: UpdateUserDto) {
    await this.findOne(id); // Check if user exists

    const user = await this.prisma.user.update({
      where: { id },
      data: updateUserDto,
    });

    this.logger.log(`User updated: ${user.email}`);
    return user;
  }

  async remove(id: string) {
    await this.findOne(id); // Check if user exists

    await this.prisma.user.delete({
      where: { id },
    });

    this.logger.log(`User deleted: ${id}`);
    return { message: 'User deleted successfully' };
  }

  async changePassword(id: string, changePasswordDto: ChangePasswordDto) {
    const user = await this.prisma.user.findUnique({
      where: { id },
      select: { id: true, email: true, password: true },
    });

    if (!user) {
      throw new NotFoundException(`User with ID ${id} not found`);
    }

    if (!user.password) {
      throw new UnauthorizedException('Password not set for this account');
    }

    // Verify current password
    const isPasswordValid = await bcrypt.compare(changePasswordDto.currentPassword, user.password);

    if (!isPasswordValid) {
      throw new UnauthorizedException('Current password is incorrect');
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(changePasswordDto.newPassword, 10);

    // Update password
    await this.prisma.user.update({
      where: { id },
      data: { password: hashedPassword },
    });

    this.logger.log(`Password changed for user: ${user.email}`);
    return { message: 'Password changed successfully' };
  }

  async setupPassword(id: string, setupPasswordDto: SetupPasswordDto) {
    const user = await this.prisma.user.findUnique({
      where: { id },
      select: { id: true, email: true, password: true, authType: true },
    });

    if (!user) {
      throw new NotFoundException(`User with ID ${id} not found`);
    }

    // Optional: Check if password already exists
    if (user.password) {
      throw new UnauthorizedException('Password already set. Use change password instead.');
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(setupPasswordDto.newPassword, 10);

    // Update password
    await this.prisma.user.update({
      where: { id },
      data: { password: hashedPassword },
    });

    this.logger.log(`Password set up for SSO user: ${user.email}`);
    return { message: 'Password set successfully' };
  }

  async updateAvatar(id: string, avatarUrl: string) {
    const user = await this.prisma.user.findUnique({
      where: { id },
    });

    if (!user) {
      throw new NotFoundException(`User with ID ${id} not found`);
    }

    // Store only relative path in database, NOT full URL
    // Frontend will construct full URL using VITE_BACKEND_URL env variable
    const updatedUser = await this.prisma.user.update({
      where: { id },
      data: { avatarUrl },
      select: {
        id: true,
        name: true,
        email: true,
        avatarUrl: true,
        bio: true,
        teamSize: true,
        primaryUseCase: true,
        authType: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    this.logger.log(`Avatar updated for user: ${user.email}`);
    return updatedUser;
  }
}
