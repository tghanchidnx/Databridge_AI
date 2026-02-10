# Data Amplifier - Product Requirements Document

Data Amplifier is a comprehensive multi-tenant SaaS platform designed for data engineering teams to manage Snowflake and multi-database operations with intelligent automation, comparison tools, and AI-powered assistance.

**Experience Qualities**:
1. **Professional** - Enterprise-grade reliability and polish that instills confidence in mission-critical database operations
2. **Intelligent** - AI-powered suggestions and self-healing capabilities that reduce manual work and prevent errors
3. **Collaborative** - Seamless workspace-based team coordination with shared resources and transparent activity tracking

**Complexity Level**: Complex Application (advanced functionality, accounts)
- This is a full-featured enterprise platform requiring sophisticated workspace management, database connection handling, real-time collaboration features, and AI integration across multiple independent modules working together as a unified system.

## Essential Features

### Authentication & User Management
- **Functionality**: Secure OAuth2.0 authentication with GitHub, Microsoft, and Google providers, plus traditional email/password signup. New users are automatically directed through onboarding flow.
- **Purpose**: Provide flexible, secure authentication options that meet enterprise requirements while ensuring smooth first-time user experience
- **Trigger**: User visits application or clicks sign up/login
- **Progression**: 
  - **Login**: Choose auth method → OAuth redirect/email login → Authenticate → Dashboard (existing users) OR Onboarding (new users)
  - **Signup**: Choose auth method → OAuth redirect/email signup → Profile creation → Onboarding flow → Dashboard
- **Success criteria**: Users can authenticate via any supported method, new users complete onboarding before accessing main app, returning users skip onboarding, authentication state persists across sessions

### User Onboarding
- **Functionality**: Multi-step onboarding flow that collects user profile (bio), workspace information (name, team size, use case), and billing plan preference (Free/Pro/Enterprise). All data is persisted to user profile and editable in settings.
- **Purpose**: Guide new users through initial setup and plan selection to ensure proper workspace configuration and personalized experience
- **Trigger**: First-time user login after authentication (detected via isNewUser flag)
- **Progression**: Welcome screen → Profile completion → Workspace setup (name, team size, use case) → Billing plan selection with feature comparison → Feature overview → Quick start guide → Dashboard
- **Success criteria**: 90%+ of new users complete onboarding, all onboarding data is saved to user profile in JSON format, users can update all information in Settings, only new users see onboarding

### Workspace Management
- **Functionality**: Multi-workspace support where each user can belong to multiple workspaces and switch between them. Each workspace has team member management, role-based access control, and subscription plan management (Free, Pro, Enterprise). Users can create new workspaces, update workspace settings, and manage team members. When users have more than one workspace, a workspace switcher is displayed in the sidebar.
- **Purpose**: Enable teams to collaborate securely with proper permission boundaries across multiple projects or departments, with transparent billing and editable configuration per workspace
- **Trigger**: Workspace automatically created during signup/onboarding, or user clicks "Create New Workspace" in workspace switcher
- **Progression**: 
  - **First Workspace**: Onboarding → Workspace creation → Configure settings in Settings tab → Invite team members → Set roles
  - **Additional Workspaces**: Click workspace switcher → Create new workspace → Set name and plan → Invite members → Switch between workspaces via sidebar dropdown
  - **Switching**: Click workspace switcher → Select workspace from dropdown → Context switches to selected workspace → All data scoped to current workspace
- **Success criteria**: Each user can have multiple workspaces, workspace switcher only shows when user has 2+ workspaces, workspace name and plan are editable in Settings, all changes persist to JSON storage, current workspace displays correctly in navigation and topbar, users only see data for their current workspace

### Database Connection Management
- **Functionality**: Secure storage and management of database credentials with support for Snowflake, PostgreSQL, MySQL, and other major databases
- **Purpose**: Centralized, encrypted credential storage that eliminates manual configuration and reduces security risks
- **Trigger**: User needs to connect to a database for comparison or query operations
- **Progression**: Navigate to connections → Add connection → Select database type → Enter credentials → Test connection → Save encrypted credentials → Use in modules
- **Success criteria**: Connections can be established within 60 seconds, credentials are never exposed, and connections are shareable across team members

### Schema Matcher ✅ IMPLEMENTED
- **Functionality**: Automated schema comparison between two database environments with GitHub-style diff visualization, dependency graph analysis, and deployment script generation with rollback support
- **Purpose**: Reduce manual database migration errors and accelerate deployment cycles with confidence
- **Trigger**: Developer needs to sync schemas between environments or validate deployment changes
- **Progression**: Select source/target connections → Configure comparison options → Run comparison → Review differences in results/dependencies/script tabs → Generate and download deployment/rollback scripts → Execute or save for later
- **Success criteria**: Accurately identifies all schema differences within 30 seconds for 1000+ objects and generates syntactically correct migration scripts with dependency-aware ordering
- **Implementation Status**: Complete with comparison setup, results visualization, dependency graph (canvas-based), script generator with options, and comparison history

### Report Matcher
- **Functionality**: Data-level comparison between CSV files, Excel reports, or query results with cell-by-cell difference highlighting
- **Purpose**: Validate data consistency across environments and quickly identify discrepancies in reports
- **Trigger**: User needs to validate data migration or compare report outputs
- **Progression**: Upload/select source data → Upload/select target data → Configure comparison rules → Execute comparison → Visualize differences → Export results
- **Success criteria**: Processes 100K rows in under 10 seconds with clear visual indicators of matching vs. mismatched records

### AI Assistant
- **Functionality**: Context-aware chatbot with fixed input at bottom and scrollable message area, integrates with OpenAI API (configurable), generates SQL queries, explains results, suggests optimizations, and provides self-healing recommendations
- **Purpose**: Reduce cognitive load and accelerate problem-solving for database tasks with intelligent, real-time assistance
- **Trigger**: User clicks AI assistant button or types natural language query
- **Progression**: Click AI button → Chat panel opens with fixed input → Describe task → AI generates SQL/suggestion → Review response in scrollable area → Execute if needed → Continue conversation
- **Success criteria**: 80%+ of generated queries execute successfully on first attempt, UI remains usable with long conversations, users save average 15+ minutes per task, fallback responses work when API key not configured

### Version Control Integration
- **Functionality**: Git integration for tracking schema configurations, comparison results, and deployment history
- **Purpose**: Enable audit trails, rollback capabilities, and team collaboration on database changes
- **Trigger**: User completes a schema comparison or wants to track changes over time
- **Progression**: Configure GitHub connection → Select repository → Commit comparison results → Track history → Create pull requests for approvals
- **Success criteria**: All configuration changes are versioned automatically with meaningful commit messages and diff views

## Edge Case Handling

- **Connection Failures** - Implement exponential backoff retry logic with clear error messages and suggested fixes
- **Large Dataset Handling** - Use streaming and pagination for datasets exceeding memory limits; provide progress indicators
- **Concurrent Edits** - Implement optimistic locking with conflict resolution UI for workspace settings and shared resources
- **Session Expiry** - Auto-save work in progress and gracefully restore state after re-authentication
- **Unsupported Database Features** - Gracefully degrade with warnings when encountering database-specific syntax or objects
- **API Rate Limits** - Queue requests and display wait times; provide workspace-level quotas to prevent abuse
- **Network Interruptions** - Cache recent data and enable offline viewing of historical results; resume operations when reconnected

## Design Direction

The design should evoke trust, precision, and technological sophistication - drawing inspiration from Snowflake's clean data-centric aesthetic while incorporating the fluidity and intelligence of modern AI-powered tools. The interface should feel professional and capable without being overwhelming, using a dark-primary theme that reduces eye strain during extended analysis sessions. The design should favor clarity over decoration, with purposeful animations that guide attention and reinforce actions rather than distract. A minimal yet rich interface serves the core purpose - enabling power users to work efficiently while remaining approachable to new team members.

## Color Selection

Custom palette - Snowflake-inspired color scheme that balances cool data-focused blues with warm accent colors for emphasis and alerts. The palette creates a professional, technology-forward feeling that aligns with enterprise database tooling while maintaining sufficient contrast and visual hierarchy.

- **Primary Color**: Snowflake Cyan (oklch(0.73 0.15 210)) - Conveys innovation, trust, and technical precision as the main brand color for key actions and focal points
- **Secondary Colors**: Deep Navy (oklch(0.18 0.02 240)) for backgrounds creating depth; Slate Blue (oklch(0.35 0.03 240)) for elevated surfaces and cards
- **Accent Color**: Bright Cyan (oklch(0.78 0.18 205)) - Energetic highlight for CTAs, active states, and important interactive elements
- **Foreground/Background Pairings**:
  - Background (Deep Navy #0E1420 - oklch(0.18 0.02 240)): White text (oklch(1 0 0)) - Ratio 14.2:1 ✓
  - Card (Slate Blue - oklch(0.35 0.03 240)): White text (oklch(1 0 0)) - Ratio 9.5:1 ✓
  - Primary (Snowflake Cyan - oklch(0.73 0.15 210)): Deep Navy text (oklch(0.18 0.02 240)) - Ratio 8.1:1 ✓
  - Secondary (Light Slate - oklch(0.92 0.01 240)): Deep Navy text (oklch(0.18 0.02 240)) - Ratio 12.8:1 ✓
  - Accent (Bright Cyan - oklch(0.78 0.18 205)): Deep Navy text (oklch(0.18 0.02 240)) - Ratio 9.2:1 ✓
  - Muted (Dark Slate - oklch(0.28 0.02 240)): Light Gray text (oklch(0.85 0.01 240)) - Ratio 7.1:1 ✓

## Font Selection

Typography should convey technical precision and modern professionalism through clean, highly legible sans-serif fonts that perform well in data-dense interfaces. Inter is selected for its exceptional clarity at small sizes and neutral geometric construction that pairs perfectly with technical content.

- **Typographic Hierarchy**:
  - H1 (Page Titles): Inter Bold/32px/tight letter-spacing(-0.02em)/line-height 1.2
  - H2 (Section Headers): Inter SemiBold/24px/tight letter-spacing(-0.01em)/line-height 1.3
  - H3 (Subsection Headers): Inter SemiBold/18px/normal letter-spacing/line-height 1.4
  - Body (Primary Text): Inter Regular/14px/normal letter-spacing/line-height 1.6
  - Small (Secondary Text): Inter Regular/12px/normal letter-spacing/line-height 1.5
  - Code/Monospace: JetBrains Mono Regular/13px/normal letter-spacing/line-height 1.5

## Animations

Animations should feel purposeful and technically precise - subtle micro-interactions that provide feedback without delay, reinforcing the professional and responsive nature of the platform. Movement should be quick and decisive (200-300ms) for most interactions, with slightly longer transitions (400-500ms) reserved for spatial changes like panel slides or page transitions. The balance favors functionality over delight, with occasional moments of polish in success states and AI interactions to humanize the technical experience.

- **Purposeful Meaning**: Motion communicates system status, guides attention to important changes, and reinforces the platform's intelligence through smooth, confident transitions
- **Hierarchy of Movement**: Highest priority animations for loading states and error feedback; medium priority for hover states and menu transitions; lowest priority for decorative embellishments

## Component Selection

- **Components**: 
  - Dialog/Sheet for connection configuration forms and settings
  - Card for connection items, comparison results, and dashboard metrics
  - Table with sorting/filtering for schema object lists and comparison results
  - Tabs for switching between different comparison views and module sections
  - Button with multiple variants (primary, secondary, ghost, destructive)
  - Input, Select, Textarea for form fields with consistent styling
  - Badge for status indicators (connected, failed, running)
  - Separator for visual section breaks
  - Tooltip for contextual help and truncated text
  - Scroll Area for handling long lists and data tables
  - Skeleton for loading states
  - Alert for notifications and warnings

- **Customizations**: 
  - Custom Monaco Editor wrapper for SQL editing with syntax highlighting
  - Custom split-pane layout for resizable AI chat panel
  - Custom diff viewer component for schema and data comparisons
  - Custom workspace switcher dropdown with search
  - Custom connection status indicator with real-time health checks

- **States**: 
  - Buttons: default, hover (brightness increase), active (scale down 0.98), disabled (opacity 0.5), loading (spinner)
  - Inputs: default, focus (ring + border color), error (red border + shake), success (green border)
  - Cards: default, hover (subtle lift with shadow), selected (border highlight)

- **Icon Selection**: Phosphor Icons for consistent line-weight and modern aesthetic
  - Database for connections
  - GitBranch for version control
  - ChartBar for reports
  - Robot for AI features
  - Gear for settings
  - Lightning for quick actions
  - Warning/CheckCircle for status indicators

- **Spacing**: Consistent spacing scale using Tailwind
  - Component padding: p-4 (16px) for cards, p-6 (24px) for dialogs
  - Section gaps: gap-6 (24px) between major sections, gap-4 (16px) for form fields
  - Page margins: px-8 (32px) for desktop, px-4 (16px) for mobile

- **Mobile**: Mobile-first responsive design with progressive enhancement
  - Navigation: Sidebar collapses to bottom sheet or hamburger menu on mobile
  - Tables: Horizontal scroll with sticky columns for large data tables
  - Forms: Stack vertically with full-width inputs on mobile
  - AI Chat: Converts to full-screen overlay on mobile instead of side panel
  - Cards: Grid layouts collapse from 3-column to 1-column at breakpoints
