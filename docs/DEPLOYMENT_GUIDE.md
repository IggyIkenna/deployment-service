# deployment-service Deployment Guide

## Build

```bash
docker build --build-arg PROJECT_ID=your-gcp-project-id -t deployment-service .
```

## Run

```bash
docker run -e GCP_PROJECT_ID=your-gcp-project-id deployment-service --help
```

## Cloud Build

Triggered on push to main. Builds image, runs quality gates inside the image, pushes to Artifact Registry on success.
