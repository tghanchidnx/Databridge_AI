# Data Amplifier - NestJS Backend

Enterprise-grade Data Warehouse Schema Comparison & Hierarchy Management System built with NestJS, TypeScript, and MySQL.

## 🚀 Features

- ✅ **User Authentication** - Microsoft SSO & Snowflake OAuth
- ✅ **Connection Management** - Multi-database connection handling
- ✅ **Schema Matcher** - Compare schemas across Snowflake accounts
- ✅ **Data Matcher** - Row-by-row data comparison
- ✅ **Hierarchy Manager** - Complex hierarchy mapping
- ✅ **RESTful APIs** - Complete API coverage
- ✅ **Swagger Documentation** - Auto-generated API docs
- ✅ **Advanced Logging** - Winston with daily file rotation
- ✅ **Security** - Helmet, CORS, Rate limiting, JWT
- ✅ **Type Safety** - Full TypeScript implementation
- ✅ **Prisma ORM** - Type-safe database access

---

## 📋 Prerequisites

Before you begin, ensure you have installed:

- **Node.js** 20.x LTS or higher
- **npm** or **yarn**
- **MySQL** 8.0 or higher
- **Git**

---

## 🛠️ Installation & Setup

### Step 1: Clone or Navigate to Project

```bash
cd "e:\DataNexumProjects\backend DataAmplifier\hierarchybuilder-nestjs"
```

### Step 2: Install Dependencies

```bash
npm install
```

### Step 3: Configure Environment

The `.env` file is already created with default values. Update the following:

```env
# Update these with your actual credentials
AZURE_TENANT_ID=your-actual-tenant-id
AZURE_CLIENT_ID=your-actual-client-id
AZURE_CLIENT_SECRET=your-actual-client-secret

SNOWFLAKE_ACCOUNT=your-actual-account
SNOWFLAKE_CLIENT_ID=your-actual-snowflake-client-id
SNOWFLAKE_CLIENT_SECRET=your-actual-snowflake-client-secret
```

### Step 4: Setup MySQL Database

Ensure MySQL is running and the `dataamplifier` database exists:

```bash
# Connect to MySQL
mysql -u root -p

# Create database
CREATE DATABASE IF NOT EXISTS dataamplifier CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
EXIT;
```

### Step 5: Initialize Prisma

```bash
# Generate Prisma Client
npm run prisma:generate

# Create database tables
npm run prisma:migrate

# (Optional) Pull existing schema if database already has tables
npm run prisma:pull
```

### Step 6: Start the Application

```bash
# Development mode with hot reload
npm run start:dev

# Production mode
npm run build
npm run start:prod
```

The server will start on `http://localhost:3000`

---

## 📚 API Documentation

Once the server is running, access the Swagger documentation:

```
http://localhost:3000/api/v1/docs
```

---

## 🗂️ Project Structure

```
hierarchybuilder-nestjs/
├── src/
│   ├── main.ts                      # Application entry point
│   ├── app.module.ts                # Root module
│   │
│   ├── config/                      # Configuration
│   │   ├── configuration.ts         # Environment config
│   │   └── winston.config.ts        # Logging config
│   │
│   ├── common/                      # Shared resources
│   │   ├── decorators/              # Custom decorators
│   │   ├── guards/                  # Auth guards
│   │   ├── interceptors/            # Interceptors
│   │   ├── filters/                 # Exception filters
│   │   ├── pipes/                   # Validation pipes
│   │   └── utils/                   # Utility functions
│   │
│   ├── database/                    # Database layer
│   │   ├── prisma/                  # Prisma service
│   │   └── snowflake/               # Snowflake service
│   │
│   ├── modules/                     # Feature modules
│   │   ├── auth/                    # Authentication
│   │   ├── users/                   # User management
│   │   ├── connections/             # DB connections
│   │   ├── schema-matcher/          # Schema comparison
│   │   ├── data-matcher/            # Data comparison
│   │   ├── reports/                 # Reports & hierarchy
│   │   └── health/                  # Health checks
│   │
│   └── types/                       # TypeScript types
│
├── prisma/
│   └── schema.prisma                # Database schema
│
├── logs/                            # Application logs (auto-created)
│   ├── application-YYYY-MM-DD.log
│   ├── error-YYYY-MM-DD.log
│   └── combined-YYYY-MM-DD.log
│
├── test/                            # E2E tests
├── .env                             # Environment variables
├── package.json                     # Dependencies
└── README.md                        # This file
```

---

## 🔧 Available Scripts

```bash
# Development
npm run start:dev          # Start with hot reload
npm run start:debug        # Start in debug mode

# Build & Production
npm run build              # Build for production
npm run start:prod         # Run production build

# Code Quality
npm run lint               # Lint code
npm run format             # Format code with Prettier

# Testing
npm run test               # Run unit tests
npm run test:watch         # Run tests in watch mode
npm run test:cov           # Run tests with coverage
npm run test:e2e           # Run E2E tests

# Database
npm run prisma:generate    # Generate Prisma client
npm run prisma:migrate     # Run migrations
npm run prisma:studio      # Open Prisma Studio
npm run prisma:pull        # Pull schema from existing DB
npm run prisma:seed        # Seed database
```

---

## 🔐 Authentication

### Microsoft SSO Login

```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "authType": "Microsoft SSO",
  "accessToken": "your-microsoft-jwt-token"
}
```

### Snowflake OAuth

```http
POST /api/v1/auth/snowflake/validate
Content-Type: application/json

{
  "code": "authorization-code-from-snowflake"
}
```

---

## 📝 Key API Endpoints

### Authentication

- `POST /api/v1/auth/login` - User login
- `POST /api/v1/auth/snowflake/validate` - Validate Snowflake token
- `POST /api/v1/auth/snowflake/refresh` - Refresh Snowflake token

### Users

- `GET /api/v1/users` - Get all users
- `GET /api/v1/users/:id` - Get user by ID
- `PUT /api/v1/users/:id` - Update user
- `DELETE /api/v1/users/:id` - Delete user

### Connections

- `GET /api/v1/connections` - List all connections
- `POST /api/v1/connections` - Create connection
- `PUT /api/v1/connections/:id` - Update connection
- `DELETE /api/v1/connections/:id` - Delete connection

### Schema Matcher

- `GET /api/v1/schema-matcher/tables` - Get all tables
- `GET /api/v1/schema-matcher/table/columns` - Get table columns
- `POST /api/v1/schema-matcher/compare` - Compare schemas
- `GET /api/v1/schema-matcher/jobs/:id` - Get comparison results

### Data Matcher

- `POST /api/v1/data-matcher/compare` - Compare data

### Reports

- `GET /api/v1/reports/hierarchy` - Get hierarchy mapping
- `POST /api/v1/reports/generate` - Generate report

### Health

- `GET /api/v1/health` - Health check

---

## 📊 Logging

Logs are automatically rotated daily and stored in the `logs/` directory:

- **application-\*.log** - All application logs
- **error-\*.log** - Error logs only
- **combined-\*.log** - Combined logs
- **exceptions-\*.log** - Uncaught exceptions
- **rejections-\*.log** - Unhandled promise rejections

Logs are:

- Rotated daily
- Compressed after rotation
- Kept for 14-30 days
- Max size: 20MB per file

---

## 🔒 Security Features

- ✅ **Helmet** - HTTP security headers
- ✅ **CORS** - Configurable cross-origin requests
- ✅ **Rate Limiting** - DDoS protection
- ✅ **JWT Authentication** - Token-based auth
- ✅ **Input Validation** - class-validator
- ✅ **SQL Injection Protection** - Prisma parameterized queries
- ✅ **Password Encryption** - bcrypt hashing
- ✅ **Sensitive Data Encryption** - AES-256

---

## 🚀 Deployment

### Docker (Coming Soon)

```bash
docker-compose up -d
```

### Manual Deployment

```bash
# Build
npm run build

# Start with PM2
pm2 start dist/main.js --name "data-amplifier"
pm2 save
pm2 startup
```

---

## 🔄 Migration from Python/Flask

This NestJS application is a complete rewrite with:

1. **Better Type Safety** - TypeScript vs Python
2. **Improved Architecture** - Modular design
3. **Enhanced Performance** - Node.js async/await
4. **Better Developer Experience** - Hot reload, CLI tools
5. **Production Ready** - Built-in monitoring, logging, health checks

### API Compatibility

All existing Python/Flask API endpoints have been migrated with the same:

- Request/Response formats
- Authentication mechanisms
- Business logic
- Database schemas

---

## 🐛 Troubleshooting

### Database Connection Issues

```bash
# Test MySQL connection
mysql -u root -p -e "SELECT 1;"

# Check database exists
mysql -u root -p -e "SHOW DATABASES LIKE 'dataamplifier';"
```

### Prisma Issues

```bash
# Reset Prisma
rm -rf node_modules/.prisma
npm run prisma:generate
```

### Port Already in Use

```bash
# Change port in .env
PORT=3001
```

---

## 📞 Support

For issues or questions:

- Check Swagger docs: http://localhost:3000/api/v1/docs
- Review logs in `logs/` directory
- Check `.env` configuration

---

## 📄 License

Copyright © 2025 DataNexum. All rights reserved.

---

## 🎯 Next Steps

1. ✅ Install dependencies: `npm install`
2. ✅ Configure `.env` file
3. ✅ Setup MySQL database
4. ✅ Run Prisma migrations: `npm run prisma:migrate`
5. ✅ Start development server: `npm run start:dev`
6. ✅ Access API docs: http://localhost:3000/api/v1/docs
7. ✅ Test endpoints with your frontend

---

**Built with ❤️ using NestJS, TypeScript, and MySQL**
