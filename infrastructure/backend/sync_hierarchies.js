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
  const data = JSON.parse(fs.readFileSync('T:/Users/telha/Databridge_AI_Source/data/hierarchies.json', 'utf8'));

  // Filter hierarchies for this project
  const projectHierarchies = Object.values(data.hierarchies || {}).filter(h => h.project_id === projectId);
  console.log('Found', projectHierarchies.length, 'hierarchies for project');

  if (projectHierarchies.length === 0) {
    console.log('No hierarchies to sync');
    await conn.end();
    return;
  }

  // Disable foreign key checks temporarily
  await conn.execute('SET FOREIGN_KEY_CHECKS = 0');

  // Insert hierarchies - match the actual DB schema
  let inserted = 0;
  for (const h of projectHierarchies) {
    try {
      await conn.execute(
        `INSERT INTO smart_hierarchy_master (
          id, project_id, hierarchy_id, hierarchy_name, description,
          hierarchy_level, flags, mapping, formula_config, filter_config,
          pivot_config, metadata, created_by, updated_by, is_root, parent_id,
          sort_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
          hierarchy_name = VALUES(hierarchy_name),
          parent_id = VALUES(parent_id),
          hierarchy_level = VALUES(hierarchy_level),
          flags = VALUES(flags),
          mapping = VALUES(mapping),
          updated_at = NOW()`,
        [
          h.id,
          projectId,
          h.hierarchy_id || '',
          h.hierarchy_name || '',
          h.description || '',
          JSON.stringify(h.hierarchy_level || {}),
          JSON.stringify(h.flags || {}),
          JSON.stringify(h.mapping || []),
          h.formula_config ? JSON.stringify(h.formula_config) : null,
          h.filter_config ? JSON.stringify(h.filter_config) : null,
          h.pivot_config ? JSON.stringify(h.pivot_config) : null,
          h.metadata ? JSON.stringify(h.metadata) : null,
          h.created_by || null,
          h.updated_by || null,
          h.is_root ? 1 : 0,
          h.parent_id || null,
          h.sort_order || 0
        ]
      );
      inserted++;
    } catch (err) {
      console.log('Error inserting hierarchy:', h.id, err.message);
    }
  }

  console.log('Inserted/updated', inserted, 'hierarchies');

  // Re-enable foreign key checks
  await conn.execute('SET FOREIGN_KEY_CHECKS = 1');

  // Verify count
  const [rows] = await conn.execute('SELECT COUNT(*) as count FROM smart_hierarchy_master WHERE project_id = ?', [projectId]);
  console.log('Total hierarchies in DB for project:', rows[0].count);

  await conn.end();
}

syncHierarchies().catch(console.error);
