'use strict';
// Used by Prisma CLI in Docker (migrate deploy). Plain JS so no "prisma/config" module is required.
// Schema path is passed via CLI (--schema=...). Local dev uses prisma.config.ts with defineConfig/env().
module.exports = {
  datasource: {
    url: process.env.DATABASE_URL,
  },
};
