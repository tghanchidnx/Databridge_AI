const crypto = require('crypto');

console.log('=== PRODUCTION SECRETS ===');
console.log('');
console.log('JWT_SECRET=' + crypto.randomBytes(32).toString('hex'));
console.log('ENCRYPTION_KEY=' + crypto.randomBytes(32).toString('hex'));
console.log('');
console.log('=== Copy these to your .env file ===');
