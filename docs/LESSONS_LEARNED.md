# DataBridge AI - Lessons Learned

> Development notes and common issues to review before making changes.
> Last updated: 2026-01-28

---

## Architecture Lessons

### 1. Docker vs Local Development

**Issue**: Changes made locally don't affect Docker containers.

**Solution**:
- Frontend runs in Docker container `databridge-frontend`
- After local code changes, must rebuild: `docker-compose build frontend --no-cache`
- Then restart: `docker-compose up -d frontend`
- The Vite dev server runs INSIDE the container, not on host

### 2. Parent ID References

**Issue**: Hierarchy tree only showing root nodes, children not linked.

**Root Cause**: Backend uses `hierarchyId` for parent references, not `id`:
```json
{
  "id": "uuid-1234",
  "hierarchyId": "HIER_AUTO_1000001",
  "parentId": "HIER_AUTO_1000000"  // References hierarchyId, not id!
}
```

**Solution**: Build tree maps using BOTH `id` and `hierarchyId`:
```typescript
const nodeMapById = new Map<string, HierarchyNode>();
const nodeMapByHierarchyId = new Map<string, HierarchyNode>();
// ... index by both
const parent = nodeMapByHierarchyId.get(node.parentId) || nodeMapById.get(node.parentId);
```

### 3. forwardRef and useImperativeHandle

**Issue**: `sendMessage` function signature changed, broke onClick handler.

**Root Cause**: When modifying function to accept optional parameter `(externalMessage?: string)`, existing `onClick={sendMessage}` passes the event as first arg.

**Solution**: Always use arrow function for onClick: `onClick={() => sendMessage()}`

### 4. Tailwind group-hover

**Issue**: Hover effects not showing on tree nodes.

**Checklist**:
1. Parent must have `group` class
2. Children use `group-hover:opacity-100` etc.
3. Docker container must be rebuilt after CSS changes
4. Hard refresh browser (Ctrl+Shift+R)

---

## API Lessons

### 5. Backend Response Parsing

**Issue**: AI chat returning changes but frontend showing "0 changes parsed".

**Root Cause #1**: Backend already parses JSON and removes it from message.

**Root Cause #2**: Backend wraps response in `{data: {...}, statusCode, message, timestamp}`:
```json
{
  "data": {
    "message": "...",
    "response": "...",
    "changes": [...]
  },
  "statusCode": 201,
  "message": "...",
  "timestamp": "..."
}
```

**Solution**: Unwrap the response first:
```typescript
const responseData = await response.json();
const data = responseData.data || responseData;  // Unwrap!

if (data.changes && Array.isArray(data.changes) && data.changes.length > 0) {
  changes = data.changes;
} else {
  const parsed = parseAIResponse(rawResponse);
  changes = parsed.changes;
}
```

### 6. V1 vs V2 Ports

| Service | V1 Port | V2 Port |
|---------|---------|---------|
| Backend | 3001 | 3002 |
| Frontend HTTP | - | 8080 |
| Frontend HTTPS | - | 8443 |
| MySQL | 3306 | 3308 |
| Redis | 6379 | 6381 |

**API Keys**: `dev-key-1`, `dev-key-2`

---

## Frontend Lessons

### 7. Component State in Tree Nodes

**Issue**: useState/useRef in TreeNode function component.

**Note**: This works fine because TreeNode is a proper function component, but be aware:
- Each node gets its own state
- Don't add state that should be shared (use parent or context)

### 8. Animation Classes

**CSS classes for AI changes**:
- `animate-hierarchy-rename` - Purple flash
- `animate-hierarchy-update` - Blue pulse
- `animate-hierarchy-move` - Amber slide
- `animate-hierarchy-create` - Green fade-in
- `animate-hierarchy-delete` - Red fade-out

**Location**: `infrastructure/frontend/src/index.css`

### 9. AI Hierarchy Lookup Table Limit

**Issue**: AI chat saying it can't find hierarchies that exist (e.g., "Revenue" not found).

**Root Cause**: The AI system prompt only includes the first N hierarchies in its lookup table. If the project has many hierarchies, some may not be visible to the AI.

**Location**: `infrastructure/backend/src/modules/ai/chat.service.ts` - `buildSystemPrompt()` function

**Previous limit**: 100 hierarchies
**Updated limit**: 300 hierarchies (sorted alphabetically for easier lookup)

**Debugging steps**:
1. Check the browser DevTools Console - look for "Context Sent" logs showing which hierarchies were sent
2. Verify the correct project is selected
3. Check if the hierarchy name matches exactly (case-insensitive search is supported)
4. If hierarchy not found, AI should list similar available names

**Solution**: The AI now receives up to 300 hierarchies sorted alphabetically, with search tips for partial matching.

### 10. Separation of Viewing and Editing

**Decision**: Hierarchy Viewer is now read-only, while Hierarchy Knowledge Base handles all editing.

**Rationale**:
- **Hierarchy Viewer** - Focused on visualization with mappings as child nodes and activity logging
- **Hierarchy Knowledge Base** - Full editing with drag-drop, AI chat, and CRUD operations

**Benefits**:
1. Simpler code - each page has a single responsibility
2. Better UX - users know where to go for viewing vs editing
3. Reduced complexity - no need to sync AI chat with tree state in viewer
4. Performance - viewer loads faster without editing infrastructure

**Mapping Display**: Viewer shows mappings as child nodes with color-coded tags:
- Database (purple), Schema (blue), Table (green), Column (amber), ID Value (rose)

---

## Debugging Checklist

Before making changes, verify:

1. [ ] Which port is the backend running on? (3001 vs 3002)
2. [ ] Is the code in Docker or running locally?
3. [ ] After changes, did you rebuild Docker? (`docker-compose build --no-cache`)
4. [ ] Did you restart the container? (`docker-compose up -d`)
5. [ ] Did you hard refresh browser? (Ctrl+Shift+R)
6. [ ] Check browser DevTools Console for errors
7. [ ] Check Network tab for failed API calls
8. [ ] Check Docker logs: `docker logs databridge-frontend`

---

## MCP Server Configuration

**Main config**: `src/config.py`

```python
nestjs_backend_url = "http://localhost:3002/api"
nestjs_api_key = "dev-key-1"
```

**For V1 (legacy)**:
```python
nestjs_backend_url = "http://localhost:3001/api"
nestjs_api_key = "dev-key-1"
```

---

## Quick Commands

```bash
# Rebuild and restart frontend
cd infrastructure && docker-compose build frontend --no-cache && docker-compose up -d frontend

# Check container logs
docker logs databridge-frontend --tail 50

# Check backend health
curl -s http://localhost:3002/api/health

# Test AI chat API
curl -s -k https://localhost:8443/api/ai/hierarchy-chat -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-1" \
  -d '{"sessionId":"test","message":"list hierarchies","context":{}}'
```
