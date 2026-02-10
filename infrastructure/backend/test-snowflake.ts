// Test Snowflake Connection
// Run with: npx ts-node test-snowflake.ts

import * as snowflake from 'snowflake-sdk';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';

dotenv.config();

async function testConnection() {
  console.log('🔍 Testing Snowflake connection...\n');

  const useRSA = process.env.SNOWFLAKE_USE_RSA === 'true';
  console.log(`Authentication method: ${useRSA ? 'RSA Key' : 'Password'}`);

  const connectionOptions: any = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  };

  if (useRSA) {
    const privateKeyPath = process.env.SNOWFLAKE_RSA_PRIVATE_KEY_PATH;
    const fullPath = path.resolve(process.cwd(), privateKeyPath);

    console.log(`Reading RSA key from: ${fullPath}`);

    if (fs.existsSync(fullPath)) {
      const privateKey = fs.readFileSync(fullPath, 'utf8');
      connectionOptions.authenticator = 'SNOWFLAKE_JWT';
      connectionOptions.privateKey = privateKey;
      console.log('✅ RSA key loaded');
    } else {
      console.log('❌ RSA key file not found, using password');
      connectionOptions.password = process.env.SNOWFLAKE_PASSWORD;
    }
  } else {
    connectionOptions.password = process.env.SNOWFLAKE_PASSWORD;
  }

  const connection = snowflake.createConnection(connectionOptions);

  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('❌ Connection failed:', err.message);
        reject(err);
        return;
      }

      console.log('✅ Connected to Snowflake!');
      console.log(`   Account: ${process.env.SNOWFLAKE_ACCOUNT}`);
      console.log(`   Database: ${process.env.SNOWFLAKE_DATABASE}`);
      console.log(`   Schema: ${process.env.SNOWFLAKE_SCHEMA}`);
      console.log(`   Warehouse: ${process.env.SNOWFLAKE_WAREHOUSE}\n`);

      // Test query
      console.log('Running test query...');
      connection.execute({
        sqlText:
          'SELECT CURRENT_VERSION() as version, CURRENT_USER() as user, CURRENT_DATABASE() as db',
        complete: (err, stmt, rows) => {
          if (err) {
            console.error('❌ Query failed:', err.message);
            reject(err);
          } else {
            console.log('✅ Query successful!');
            console.log('   Result:', rows[0]);

            // Close connection
            connection.destroy((err) => {
              if (err) {
                console.error('Error closing connection:', err.message);
              } else {
                console.log('\n✅ Connection closed successfully');
              }
              resolve(true);
            });
          }
        },
      });
    });
  });
}

testConnection()
  .then(() => {
    console.log('\n✅ All tests passed!');
    process.exit(0);
  })
  .catch((err) => {
    console.error('\n❌ Tests failed:', err);
    process.exit(1);
  });
