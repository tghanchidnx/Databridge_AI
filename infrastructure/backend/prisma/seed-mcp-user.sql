-- Seed script to create MCP System User
-- This user is used by Claude/MCP when creating hierarchies via API key authentication
-- Run this script to ensure MCP-created projects are accessible in the UI

-- Create the MCP System User (or update if exists)
INSERT INTO users (
    id,
    user_name,
    user_email,
    password,
    auth_type,
    is_active,
    onboarding_completed,
    created_at,
    updated_at
) VALUES (
    'mcp-system-user-001',
    'MCP System',
    'mcp@dataamplifier.local',
    NULL,
    'API Key',
    1,
    1,
    NOW(),
    NOW()
) ON DUPLICATE KEY UPDATE
    user_name = 'MCP System',
    is_active = 1,
    updated_at = NOW();

-- Verify the user was created
SELECT id, user_name, user_email, auth_type, is_active FROM users WHERE user_email = 'mcp@dataamplifier.local';
