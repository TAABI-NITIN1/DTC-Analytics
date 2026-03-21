import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    // Allow access from other machines (e.g. your host browser hitting the VM IP)
    host: true,
    port: 5173,
    // Don't auto-switch to a different port; we want 5173 specifically
    strictPort: true,
    // NOTE: We no longer proxy API requests via Vite. The frontend talks directly to the backend
    // via a configured VITE_API_URL (default http://127.0.0.1:8005).
  },
});
