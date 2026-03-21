# DTC Dashboard Frontend (Vite + GraphQL)

## Run locally (dev)

1. Ensure the API server (FastAPI) is running on port `8005` and reachable from your browser.
2. Set the API base for the frontend. The app will default to `http://<current-hostname>:8005`, but you can be explicit:

	Create `.env` in `frontend/`:

	```bash
	VITE_API_URL=http://4.224.101.147:8005
	```

3. Install and start Vite on the standard dev port `5173`:

	```bash
	npm ci
	npm run dev
	# Vite is pinned to 5173 (strictPort) and binds to 0.0.0.0
	```

	Open: `http://<server-ip>:5173` (e.g., `http://4.224.101.147:5173`).

Notes
- The frontend talks directly to the backend using `VITE_API_URL` (no dev proxy).
- Do not use port 5174; the project standard is 5173 only.

## Pages

- Fleet Overview
- DTC Level Analytics
- Vehicle Details
- Customer Level
- Maintenance Insights
