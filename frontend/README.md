# DTC Dashboard Frontend (Vite + GraphQL)

## Run locally (dev)

1. Ensure the API server (FastAPI) is running on port `8001` and reachable from your browser.
2. Set the API base for the frontend. The app will default to `http://<current-hostname>:8001`, but you can be explicit:

	Create `.env` in `frontend/`:

	```bash
	VITE_API_URL=http://4.224.101.147:8001
	```

3. Install and start Vite on the standard dev port `5174`:

	```bash
	npm ci
	npm run dev
	# Vite is pinned to 5174 (strictPort) and binds to 0.0.0.0
	```

	Open: `http://<server-ip>:5174` (e.g., `http://4.224.101.147:5174`).

Notes
- The frontend talks directly to the backend using `VITE_API_URL` (no dev proxy).
- The project standard is port `5174` for the frontend and `8001` for the backend.

## Pages

- Fleet Overview
- DTC Level Analytics
- Vehicle Details
- Customer Level
- Maintenance Insights
