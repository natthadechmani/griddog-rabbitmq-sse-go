import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During local `npm run dev`, proxy /api (incl. the SSE endpoint) to the
// gateway-backend so the app uses the same relative paths as in the docker/nginx
// setup. http-proxy streams responses, so Server-Sent-Events work through it.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
