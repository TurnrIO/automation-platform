# ── Stage 1: Build frontend (Vite + React) ────────────────────────────────────
FROM node:22-alpine AS frontend-build

WORKDIR /frontend

# Install deps first (layer cache — only re-runs when package files change)
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

# Copy source and build.
# Override outDir to ./dist so output stays inside /frontend and can be
# cleanly COPY-ed into the next stage (vite.config.js defaults to
# ../app/static/dist which is only correct for local dev).
COPY frontend/ ./
RUN npm run build -- --outDir ./dist --emptyOutDir

# ── Stage 2: Python runtime ───────────────────────────────────────────────────
FROM python:3.11-slim

# Create a non-root user so the process cannot write to the image filesystem
# or escalate privileges even if a dependency is compromised.
RUN groupadd --gid 1001 hiverunr \
 && useradd --uid 1001 --gid hiverunr --shell /bin/bash --create-home hiverunr

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY migrations/ ./migrations/
COPY alembic.ini .

# Copy Vite build output to /app/frontend_dist — deliberately OUTSIDE
# the ./app bind mount so docker-compose hot-reload doesn't clobber it.
# main.py checks this path first, then falls back to app/static/dist/
# for local npm-dev-server workflows.
COPY --from=frontend-build /frontend/dist/ ./frontend_dist/

# Hand ownership of the working directory to the app user
RUN chown -R hiverunr:hiverunr /app

USER hiverunr

CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
