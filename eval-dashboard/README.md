# Taabi AI Evaluation Dashboard

Static Vite + React dashboard for evaluation run bundles (`dashboard_bundle.json`).

## Prerequisites

Export bundles from the main project:

```powershell
cd ..
python -m evaluation.analytics export-dashboard --run-id eval_20260520_085108_190cf6 --copy-to-eval-dashboard
```

## Run locally

```powershell
npm install
npm run dev
```

Open http://localhost:5174

## Build static site

```powershell
npm run build
npm run preview
```

Output in `dist/` — can be hosted on any static file server.

## Data files

- `public/runs/manifest.json` — run list for dropdown
- `public/runs/<run_id>.json` — chart-ready bundle per run
