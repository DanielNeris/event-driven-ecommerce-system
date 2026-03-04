import 'dotenv/config';
import { execSync } from 'node:child_process';
import { PrismaClient } from '../src/generated/prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';

const connectionString = process.env.INVENTORY_DATABASE_URL;
const adapter = new PrismaPg({ connectionString: connectionString || '' });
const prisma = new PrismaClient({ adapter });

const INVENTORY_SCHEMA =
  'apps/inventory-consumer-microservice/prisma/schema.prisma';

const PRODUCTS = [
  { productId: 'prod-001', availableStock: 50 },
  { productId: 'prod-002', availableStock: 100 },
  { productId: 'prod-003', availableStock: 25 },
  { productId: 'prod-004', availableStock: 200 },
  { productId: 'prod-005', availableStock: 75 },
  { productId: 'prod-006', availableStock: 30 },
  { productId: 'prod-007', availableStock: 150 },
  { productId: 'prod-008', availableStock: 80 },
  { productId: 'prod-009', availableStock: 60 },
  { productId: 'prod-010', availableStock: 120 },
];

async function main() {
  if (!connectionString) {
    throw new Error(
      'INVENTORY_DATABASE_URL or DATABASE_URL is required. Set it in .env',
    );
  }
  console.log('🌱 Starting inventory seed...');
  console.log(`📡 Database: ${connectionString.replace(/:[^:@]+@/, ':****@')}`);

  try {
    console.log('📦 Ensuring migrations are applied...');
    execSync(`pnpm exec prisma migrate deploy --schema=${INVENTORY_SCHEMA}`, {
      env: { ...process.env, DATABASE_URL: connectionString },
      stdio: 'inherit',
    });

    await prisma.$connect();
    console.log('✅ Database connection established');

    console.log('📝 Seeding products...');
    for (const p of PRODUCTS) {
      const result = await prisma.product.upsert({
        where: { productId: p.productId },
        update: {},
        create: { productId: p.productId, availableStock: p.availableStock },
      });
      console.log(`  ✓ ${result.productId}: stock ${result.availableStock}`);
    }

    const count = await prisma.product.count();
    console.log(`✅ Seeded ${PRODUCTS.length} products`);
    console.log(`📊 Total products in database: ${count}`);
  } catch (error) {
    console.error('❌ Error during seeding:');
    console.error(error);
    throw error;
  }
}

main()
  .catch((e) => {
    console.error('💥 Seed failed:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    console.log('🔌 Database connection closed');
  });
