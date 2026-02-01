const mysql = require('mysql2/promise');
const fs = require('fs');

async function syncHierarchies() {
  const conn = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'admin',
    database: 'dataamplifier'
  });

  const projectId = '7e8c37ef-5f52-48d7-8de5-a6df094da5a8';

  // Read hierarchies from local MCP storage
  const data = JSON.parse(fs.readFileSync('C:/Users/telha/Databridge_AI/data/hierarchies.json', 'utf8'));

  // Filter hierarchies for this project
  const projectHierarchies = Object.values(data.hierarchies || {}).filter(h => h.project_id === projectId);
  console.log('Found', projectHierarchies.length, 'hierarchies for project');

  if (projectHierarchies.length === 0) {
    console.log('No hierarchies to sync');
    await conn.end();
    return;
  }

  // Insert hierarchies
  let inserted = 0;
  for (const h of projectHierarchies) {
    try {
      await conn.execute(
        `INSERT INTO smart_hierarchy_master (
          id, project_id, hierarchy_code, hierarchy_name, parent_id,
          level, sort_order, description, is_active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
          hierarchy_name = VALUES(hierarchy_name),
          parent_id = VALUES(parent_id),
          updated_at = NOW()`,
        [
          h.id,
          projectId,
          h.code || h.hierarchy_code || '',
          h.name || h.hierarchy_name || '',
          h.parent_id || null,
          h.level || 0,
          h.sort_order || 0,
          h.description || ''
        ]
      );
      inserted++;
    } catch (err) {
      console.log('Error inserting hierarchy:', h.id, err.message);
    }
  }

  console.log('Inserted/updated', inserted, 'hierarchies');

  // Verify count
  const [rows] = await conn.execute('SELECT COUNT(*) as count FROM smart_hierarchy_master WHERE project_id = ?', [projectId]);
  console.log('Total hierarchies in DB for project:', rows[0].count);

  await conn.end();
}

syncHierarchies().catch(console.error);
