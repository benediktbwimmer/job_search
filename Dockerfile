FROM node:20-alpine AS frontend-build
WORKDIR /workspace/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/backend
WORKDIR /app
COPY backend/ /app/backend/
COPY --from=frontend-build /workspace/frontend/dist /app/frontend/dist
EXPOSE 8787
CMD ["python3", "backend/scripts/serve_api.py", "--host", "0.0.0.0", "--port", "8787", "--frontend-dist", "/app/frontend/dist"]
