# syntax=docker/dockerfile:1

# Build the React app, then serve the static bundle with nginx (which also
# reverse-proxies /api and the SSE endpoint to the gateway-backend).
# Build context is the repo root.
FROM node:26-alpine AS build
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

FROM nginx:1.27-alpine
COPY deploy/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
