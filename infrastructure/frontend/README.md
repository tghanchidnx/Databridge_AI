# Data Amplifier - Database Management Platform

A comprehensive multi-tenant SaaS platform for data engineering teams to manage Snowflake and multi-database operations with intelligent automation, comparison tools, and AI-powered assistance.

## 📚 Documentation Quick Links

> **🗂️ NEW USER? Start here:** **[Complete Documentation Index](DOC_INDEX.md)** - Your guide to all documentation

### Essential Guides

- **🚀 [Quick Start Guide](QUICK_START.md)** - Get up and running with backend integration
- **🔗 [GitHub Setup Guide](GITHUB_SETUP.md)** - How to push this project to GitHub and run it on your system
- **📍 [Project Locations](PROJECT_LOCATIONS.md)** - Where to find package.json and all important files
- **📦 [Package Guide](PACKAGE_GUIDE.md)** - Understanding package.json and npm dependencies
- **⚡ [Setup Guide](SETUP.md)** - Installation and configuration

### Backend Integration (NEW! ✨)

- **🔌 [API Documentation](API_DOCUMENTATION.md)** - Complete API reference for all 42 endpoints
- **📋 [Backend Integration Summary](BACKEND_INTEGRATION_SUMMARY.md)** - Implementation details
- **💡 [Integration Examples](src/examples/)** - Code examples for OAuth, connections, and more

### Project Details

- **📝 [Product Requirements](PRD.md)** - Detailed feature specifications and design guidelines
- **🔐 [Backend Integration](BACKEND_INTEGRATION.md)** - Legacy API integration guide
- **🛡️ [Security](SECURITY.md)** - Security best practices

## 🚀 Quick Start

### Prerequisites

- **Node.js** (v18 or higher recommended)
- **npm** (v9 or higher)

### Installation

1. **Clone or navigate to the project directory**

   ```bash
   cd /path/to/dataamp-ai
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Configure environment variables**

   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Start the development server**

   ```bash
   npm run dev
   ```

5. **Open your browser**

   The application will automatically start at `http://localhost:5000`

## 🔌 Backend Integration

The application now includes **full backend API integration** with 42 endpoints:

- ✅ **Authentication** - Microsoft OAuth, Snowflake OAuth, token management
- ✅ **Connections** - CRUD operations for database connections
- ✅ **Schema Matcher** - Compare schemas, generate deployment scripts
- ✅ **Hierarchy & Mapping** - Manage data hierarchies and mappings
- ✅ **Data Matcher** - Compare data across databases

**See [API_DOCUMENTATION.md](API_DOCUMENTATION.md) for complete API reference.**

### Quick API Usage

```typescript
import { apiService } from "@/lib/api-service";
import { useAuth } from "@/contexts/AuthContext";

// Login with Microsoft
await loginWithMicrosoft(accessToken);

// Fetch connections
const connections = await apiService.fetchConnections(user.id);

// Compare schemas
const result = await apiService.compareSchemas({
  userId: user.id,
  sourceConnectionId: "conn1",
  targetConnectionId: "conn2",
  sourceDatabase: "db1",
  targetDatabase: "db2",
});
```

## 📦 Available Scripts

- **`npm run dev`** - Start the development server with hot reload
- **`npm run build`** - Build the application for production
- **`npm run preview`** - Preview the production build locally
- **`npm run lint`** - Run ESLint to check code quality
- **`npm run optimize`** - Optimize dependencies with Vite

## 🏗️ Project Structure

```
spark-template/
├── src/
│   ├── components/        # React components
│   │   ├── layout/       # Layout components (Sidebar, TopBar)
│   │   ├── ui/           # Shadcn UI components
│   │   └── views/        # Page/view components
│   ├── contexts/         # React context providers
│   ├── hooks/            # Custom React hooks
│   ├── lib/              # Utility libraries
│   ├── types/            # TypeScript type definitions
│   ├── App.tsx           # Main app component
│   ├── index.css         # Global styles and theme
│   └── main.tsx          # Application entry point
├── public/               # Static assets
├── index.html            # HTML entry point
├── package.json          # Dependencies and scripts
└── vite.config.ts        # Vite configuration
```

## 🎨 Tech Stack

- **React 19** - UI library
- **TypeScript** - Type safety
- **Vite** - Build tool and dev server
- **Tailwind CSS v4** - Utility-first styling
- **Shadcn UI v4** - Component library
- **Framer Motion** - Animations
- **Phosphor Icons** - Icon library
- **React Hook Form** - Form management
- **Sonner** - Toast notifications

## 🔑 Key Features

- **Dashboard** - Overview of database operations and metrics
- **Database Connections** - Manage connections to Snowflake, PostgreSQL, MySQL, and more
- **Schema Matcher** - Compare database schemas and generate deployment scripts
- **Report Matcher** - Compare data reports with cell-level difference highlighting
- **Query Builder** - Build SQL queries with AI assistance
- **AI Assistant** - Context-aware chatbot for database operations
- **Version Control** - Git integration for tracking schema changes
- **Settings** - User preferences and workspace configuration

## 🎯 Default Credentials (Demo Mode)

For demo purposes, you can log in with:

- **Email**: demo@example.com
- **Password**: password

## 🛠️ Development

### Running in Development Mode

The dev server includes:

- Hot module replacement (HMR)
- Fast refresh for React components
- TypeScript type checking
- ESLint integration

### Building for Production

```bash
npm run build
```

This creates an optimized production build in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## 🌈 Theming

The application uses a dark-primary theme with Snowflake-inspired colors. You can customize the theme by editing `src/index.css`:

- Colors are defined using OKLCH color space
- Theme variables are in CSS custom properties
- Supports both light and dark modes (currently set to dark by default)

## 📝 License

The Spark Template files and resources from GitHub are licensed under the terms of the MIT license, Copyright GitHub, Inc.

## 🆘 Troubleshooting

### Port Already in Use

If port 5173 is already in use:

```bash
npm run kill  # Kill process on port 5000
# Or manually specify a different port in vite.config.ts
```

### Dependencies Issues

If you encounter dependency issues:

```bash
rm -rf node_modules package-lock.json
npm install
```

### Build Errors

Make sure you're using Node.js v18 or higher:

```bash
node --version
```

## 📚 Additional Resources

- See `PRD.md` for detailed product requirements
- Check `BACKEND_INTEGRATION.md` for API integration details
- Review `SECURITY.md` for security best practices
