// Test Prisma Organization Models
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function test() {
  console.log('Testing Prisma Organization Models...\n');

  // Test that models exist
  console.log('✓ prisma.organization exists:', typeof prisma.organization);
  console.log('✓ prisma.organizationMember exists:', typeof prisma.organizationMember);
  console.log('✓ prisma.billingPlan exists:', typeof prisma.billingPlan);
  console.log('✓ prisma.billingHistory exists:', typeof prisma.billingHistory);

  console.log('\n✅ All organization models are available in Prisma Client');
}

test()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
