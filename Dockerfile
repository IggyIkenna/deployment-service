# Dockerfile for deployment-service
#
# Uses unified-cloud-services base image from Artifact Registry.
#
# Build:
#   docker build --build-arg PROJECT_ID=your-gcp-project-id -t deployment-service .
#
# Run:
#   docker run -e GCP_PROJECT_ID=your-gcp-project-id deployment-service --help

ARG PROJECT_ID
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-cloud-services/unified-cloud-services:latest

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app/deployment-service

RUN pip install --no-cache-dir uv

COPY . .

RUN uv pip install --system --no-cache-dir -e ".[dev]"

ARG PROJECT_ID
ENV GCP_PROJECT_ID=${PROJECT_ID}

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import deployment_service; print('healthy')" || exit 1

ENTRYPOINT ["python", "-m", "deployment_service"]
CMD ["--help"]
