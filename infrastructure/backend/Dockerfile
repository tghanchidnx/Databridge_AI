# ============================================================================
# Multi-stage Dockerfile for NestJS Backend
# ============================================================================

# ============================================================================
# Stage 1: Base - Dependencies Installation
# ============================================================================
FROM node:20-alpine AS base

# Install build dependencies
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    libc6-compat \
    openssl

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./
COPY prisma ./prisma/

# ============================================================================
# Stage 2: Dependencies - Install production dependencies
# ============================================================================
FROM base AS dependencies

# Install production dependencies
RUN npm ci --only=production && \
    npm cache clean --force

# Copy production node_modules for later
RUN cp -R node_modules /prod_node_modules

# Install all dependencies (including dev)
RUN npm ci

# Install NestJS CLI globally for build
RUN npm install -g @nestjs/cli

# Generate Prisma Client
RUN npx prisma generate

# ============================================================================
# Stage 3: Build - Compile TypeScript
# ============================================================================
FROM base AS build

# Copy all dependencies from dependencies stage
COPY --from=dependencies /app/node_modules ./node_modules

# Copy source code
COPY . .

# Build the application
RUN npm run build && \
    echo "Build completed. Checking dist directory:" && \
    ls -la && \
    if [ -d "dist" ]; then ls -la dist/; else echo "ERROR: dist directory not found!"; exit 1; fi

# ============================================================================
# Stage 4: Production - Final optimized image
# ============================================================================
FROM node:20-alpine AS production

# Install production runtime dependencies
RUN apk add --no-cache \
    dumb-init \
    openssl \
    ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nestjs -u 1001

# Set working directory
WORKDIR /app

# Copy production node_modules
COPY --from=dependencies /prod_node_modules ./node_modules

# Copy Prisma client and generate again in production
COPY --from=build /app/node_modules/.prisma ./node_modules/.prisma
COPY --from=build /app/node_modules/@prisma ./node_modules/@prisma

# Copy built application (with verification)
COPY --from=build /app/dist ./dist

# Verify dist exists
RUN ls -la dist/ && test -f dist/main.js

# Copy Prisma schema for migrations
COPY --chown=nestjs:nodejs prisma ./prisma

# Copy package.json for reference
COPY --chown=nestjs:nodejs package*.json ./

# Create necessary directories
RUN mkdir -p uploads logs && \
    chown -R nestjs:nodejs uploads logs

# Switch to non-root user
USER nestjs

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Use dumb-init to handle signals properly
ENTRYPOINT ["dumb-init", "--"]

# Start command
CMD ["sh", "-c", "npx prisma migrate deploy && node dist/main"]

# ============================================================================
# Stage 5: Development - Hot reload enabled
# ============================================================================
FROM base AS development

# Copy all dependencies
COPY --from=dependencies /app/node_modules ./node_modules

# Copy source code
COPY . .

# Generate Prisma Client
RUN npx prisma generate

# Expose port
EXPOSE 3000

# Development command
CMD ["npm", "run", "start:dev"]
