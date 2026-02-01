# DataBridge AI - Development Log

## Session: January 22, 2026

### Issue: "Cannot connect to server" error in Hierarchy Builder UI

**Problem:** User was getting a connection error when trying to log into the Hierarchy Builder UI app.

**Root Cause:** Required services were not running:
- MySQL Server (port 3306)
- NestJS Backend (port 3001)
- Vite Frontend (port 5173)

**Solution Applied:**
1. Initialized and started MySQL Server 8.4
2. Ran Prisma migrations to create database tables
3. Started NestJS backend
4. Started Vite frontend

**Commands Used (for future reference):**
```bash
# MySQL Setup (Admin prompt - first time only)
cd "C:\Program Files\MySQL\MySQL Server 8.4\bin"
mysqld --initialize-insecure --console
mysqld --install MySQL84
net start MySQL84
mysql -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'admin'; CREATE DATABASE IF NOT EXISTS dataamplifier CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; FLUSH PRIVILEGES;"

# Quick Start (after initial setup)
net start MySQL84
cd "C:\Users\telha\Databridge_AI\current application\extracted_app\HIERARCHY_BUILDER_APP\hierarchybuilder-nestjs" && npm run start:dev
cd "C:\Users\telha\Databridge_AI\current application\extracted_app\HIERARCHY_BUILDER_APP\dataamp-ai" && npm run dev
```

---

### Issue: MCP-created hierarchies not visible in UI

**Problem:** When Claude creates hierarchies via MCP tools, they weren't visible to users in the Web UI.

**Root Cause:**
- MCP uses API key authentication (`X-API-Key: dev-key-1`)
- API key auth was setting `userId: 'mcp-system'` (a fake ID)
- Projects are user-scoped - UI users only see their own projects
- The 'mcp-system' user didn't exist in the database

**Solution Applied:**

1. **Created MCP System User in Database:**
   - **ID:** `mcp-system-user-001`
   - **Email:** `mcp@dataamplifier.local`
   - **Name:** `MCP System`
   - **Auth Type:** `API Key`

   SQL used:
   ```sql
   INSERT INTO users (id, user_name, user_email, password, auth_type, is_active, onboarding_completed, created_at, updated_at)
   VALUES ('mcp-system-user-001', 'MCP System', 'mcp@dataamplifier.local', NULL, 'API Key', 1, 1, NOW(), NOW())
   ON DUPLICATE KEY UPDATE user_name = 'MCP System', is_active = 1, updated_at = NOW();
   ```

2. **Updated JWT Auth Guard** (`hierarchybuilder-nestjs/src/common/guards/jwt-auth.guard.ts`):
   - Changed from hardcoded `mcp-system` to `mcp-system-user-001`
   - Added `MCP_USER_ID` environment variable support
   - Now MCP-created projects are owned by a real database user

3. **Created Seed SQL File:**
   - Location: `hierarchybuilder-nestjs/prisma/seed-mcp-user.sql`
   - Can be re-run if database is recreated

**Files Modified:**
- `hierarchybuilder-nestjs/src/common/guards/jwt-auth.guard.ts` - Updated MCP user ID
- `hierarchybuilder-nestjs/prisma/seed-mcp-user.sql` - New file for seeding MCP user

---

### How to Access MCP-Created Projects in UI

**Option 1: Add yourself as project member (Recommended)**

To see MCP-created projects in the UI, add your UI user as a project member:

```sql
-- First, find your user ID (login to UI first, then query)
SELECT id, user_email FROM users WHERE user_email = 'your-email@domain.com';

-- Find MCP-created projects
SELECT id, name FROM hierarchy_projects WHERE user_id = 'mcp-system-user-001';

-- Add yourself as a member to a project
INSERT INTO hierarchy_project_members (id, project_id, user_id, role, invitation_status, is_active, created_at, updated_at)
VALUES (UUID(), '<project-id>', '<your-user-id>', 'editor', 'accepted', 1, NOW(), NOW());
```

**Option 2: Query MCP projects directly**
```sql
SELECT * FROM hierarchy_projects WHERE user_id = 'mcp-system-user-001';
SELECT * FROM smart_hierarchy_master WHERE project_id IN (
  SELECT id FROM hierarchy_projects WHERE user_id = 'mcp-system-user-001'
);
```

---

### Documentation Updated

Added startup instructions to:
1. `tutorial/STEP_BY_STEP_GUIDE.html` - New "Hierarchy Builder UI App - Start Services" section
2. `control-center.html` - Added MySQL Setup and Quick Start sections with copy buttons

---

### Services Status (End of Session)

| Service | Status | Port |
|---------|--------|------|
| MySQL | Running | 3306 |
| NestJS Backend | Running | 3001 |
| Vite Frontend | Running | 5173 |

**URLs:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:3001/api
- Swagger Docs: http://localhost:3001/api/docs
- Health Check: http://localhost:3001/api/health

---

### Next Steps / TODO

1. **Test shared access:** Create a project via MCP, then verify it's accessible in UI after adding yourself as member
2. **Consider adding MCP tools** for project sharing (invite_project_member, etc.)
3. **Optional:** Create a dedicated "shared" organization for MCP projects

---

### Quick Reference

**Database Credentials:**
- Host: localhost:3306
- Database: dataamplifier
- User: root
- Password: admin

**API Key for MCP:**
- Key: `dev-key-1`
- Header: `X-API-Key: dev-key-1`

**MCP User in Database:**
- ID: `mcp-system-user-001`
- Email: `mcp@dataamplifier.local`

**Config Files:**
- MCP Server: `C:\Users\telha\Databridge_AI\.env`
- NestJS Backend: `hierarchybuilder-nestjs\.env`
- Frontend: `dataamp-ai\.env`
