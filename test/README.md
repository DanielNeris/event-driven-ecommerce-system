# Testes

Todos os testes ficam nesta pasta; o código de produção não contém specs.

## Estrutura

```
test/
  unit/       # Testes unitários (Jest) – controllers, services, libs
  functional/ # Testes funcionais (fluxos com mocks, ex.: Kafka → OpenSearch)
  e2e/        # Testes end-to-end (stack real ou por app)
```

## Comandos

```bash
pnpm test          # Roda todos os testes (unit)
pnpm run test:cov  # Testes + relatório de cobertura
pnpm run test:watch # Modo watch
```

## Unit

Os testes em `test/unit/` espelham a estrutura do monorepo:

- `test/unit/apps/<app>/` – testes do app
- `test/unit/libs/<lib>/` – testes da lib

Imports usam os aliases `@app/<lib>` e `@app/<app>` (definidos em `tsconfig.json` e no `moduleNameMapper` do Jest).
