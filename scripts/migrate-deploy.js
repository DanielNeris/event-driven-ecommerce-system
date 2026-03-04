#!/usr/bin/env node
'use strict';

require('dotenv').config();
const { execSync } = require('child_process');

const migrations = [
  { schema: 'apps/order-producer-microservice/prisma/schema.prisma', urlEnv: 'ORDER_DATABASE_URL' },
  { schema: 'apps/order-consumer-microservice/prisma/schema.prisma', urlEnv: 'ORDER_DATABASE_URL' },
  { schema: 'apps/inventory-consumer-microservice/prisma/schema.prisma', urlEnv: 'INVENTORY_DATABASE_URL' },
  { schema: 'apps/payment-consumer-microservice/prisma/schema.prisma', urlEnv: 'PAYMENT_DATABASE_URL' },
  { schema: 'apps/shipping-consumer-microservice/prisma/schema.prisma', urlEnv: 'SHIPPING_DATABASE_URL' },
  { schema: 'apps/messageria-consumer-microservice/prisma/schema.prisma', urlEnv: 'MESSAGERIA_DATABASE_URL' },
];

for (const { schema, urlEnv } of migrations) {
  const url = process.env[urlEnv];
  if (!url) {
    console.error(`Missing ${urlEnv}; skip migrate for ${schema}`);
    continue;
  }
  console.log(`Deploying migrations: ${schema}`);
  execSync(`pnpm exec prisma migrate deploy --schema=${schema}`, {
    stdio: 'inherit',
    env: { ...process.env, DATABASE_URL: url },
  });
}

console.log('All migrations applied.');
