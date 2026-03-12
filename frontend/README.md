# DTC Dashboard Frontend (Vite + GraphQL)

## Run locally

1. Start API server (FastAPI) on port `8001`.
2. Install frontend packages:

```bash
npm install
```

3. Start dev server:

```bash
npm run dev
```

Vite proxies `/graphql` to `http://localhost:8001`.

## Pages

- Fleet Overview
- DTC Level Analytics
- Vehicle Details
- Customer Level
- Maintenance Insights
