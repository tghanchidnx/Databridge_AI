# ============================================================================
# Multi-stage Dockerfile for React + Vite Frontend
# ============================================================================

# ============================================================================
# Stage 1: Base - Dependencies Installation
# ============================================================================
FROM node:20-alpine AS base

# Install build dependencies
RUN apk add --no-cache libc6-compat

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# ============================================================================
# Stage 2: Dependencies - Install dependencies
# ============================================================================
FROM base AS dependencies

# Install dependencies
RUN npm ci && \
    npm cache clean --force

# ============================================================================
# Stage 3: Build - Build the application
# ============================================================================
FROM base AS build

# Copy dependencies
COPY --from=dependencies /app/node_modules ./node_modules

# Copy source code
COPY . .

# Build arguments for environment variables
ARG VITE_API_URL
ARG VITE_APP_NAME
ARG VITE_APP_VERSION
ARG VITE_MICROSOFT_CLIENT_ID
ARG VITE_MICROSOFT_TENANT_ID
ARG VITE_ENABLE_ANALYTICS

# Set environment variables for build
ENV VITE_API_URL=${VITE_API_URL}
ENV VITE_APP_NAME=${VITE_APP_NAME}
ENV VITE_APP_VERSION=${VITE_APP_VERSION}
ENV VITE_MICROSOFT_CLIENT_ID=${VITE_MICROSOFT_CLIENT_ID}
ENV VITE_MICROSOFT_TENANT_ID=${VITE_MICROSOFT_TENANT_ID}
ENV VITE_ENABLE_ANALYTICS=${VITE_ENABLE_ANALYTICS}

# Build the application
RUN npm run build

# ============================================================================
# Stage 4: Production - Nginx server
# ============================================================================
FROM nginx:1.25-alpine AS production

# Install runtime dependencies including openssl for certificate generation
RUN apk add --no-cache \
    curl \
    ca-certificates \
    openssl

# Generate self-signed SSL certificate
RUN mkdir -p /etc/nginx/ssl && \
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/nginx.key \
    -out /etc/nginx/ssl/nginx.crt \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=IT/CN=localhost"

# Copy custom nginx config
COPY nginx.conf /etc/nginx/nginx.conf

# Copy built application from build stage
COPY --from=build /app/dist /usr/share/nginx/html

# Create non-root user
RUN addgroup -g 1001 -S nginx-group && \
    adduser -S nginx-user -u 1001 -G nginx-group && \
    chown -R nginx-user:nginx-group /usr/share/nginx/html && \
    chown -R nginx-user:nginx-group /var/cache/nginx && \
    chown -R nginx-user:nginx-group /var/log/nginx && \
    chown -R nginx-user:nginx-group /etc/nginx/ssl && \
    touch /var/run/nginx.pid && \
    chown -R nginx-user:nginx-group /var/run/nginx.pid

# Create necessary directories
RUN mkdir -p /var/cache/nginx/client_temp \
    /var/cache/nginx/proxy_temp \
    /var/cache/nginx/fastcgi_temp \
    /var/cache/nginx/uwsgi_temp \
    /var/cache/nginx/scgi_temp && \
    chown -R nginx-user:nginx-group /var/cache/nginx

# Switch to non-root user
USER nginx-user

# Expose ports
EXPOSE 80 443

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:80/health || exit 1

# Start nginx
CMD ["nginx", "-g", "daemon off;"]

# ============================================================================
# Stage 5: Development - Vite dev server with Nginx proxy
# ============================================================================
FROM nginx:1.25-alpine AS development

# Install Node.js and dependencies
RUN apk add --no-cache \
    nodejs \
    npm \
    wget \
    curl \
    openssl \
    ca-certificates

# Generate self-signed SSL certificate
RUN mkdir -p /etc/nginx/ssl && \
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/nginx.key \
    -out /etc/nginx/ssl/nginx.crt \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=IT/CN=localhost"

# Copy nginx config
COPY nginx-dev.conf /etc/nginx/nginx.conf

# Set working directory
WORKDIR /app

# Copy dependencies from dependencies stage
COPY --from=dependencies /app/node_modules ./node_modules

# Copy source code
COPY . .

# Expose ports
EXPOSE 80 443 5173

# Health check for development (check vite on port 5173)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:5173 || exit 1

# Start script that runs both nginx and vite
RUN echo '#!/bin/sh' > /start.sh && \
    echo 'set -e' >> /start.sh && \
    echo 'echo "Starting Nginx..."' >> /start.sh && \
    echo 'nginx' >> /start.sh && \
    echo 'echo "Nginx started"' >> /start.sh && \
    echo 'echo "Starting Vite dev server..."' >> /start.sh && \
    echo 'cd /app' >> /start.sh && \
    echo 'exec npm run dev -- --host 0.0.0.0 --port 5173' >> /start.sh && \
    chmod +x /start.sh

# Development command - run both nginx and vite
CMD ["/start.sh"]
