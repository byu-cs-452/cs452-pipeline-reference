# Container for the Cloud Run job path (Stage 2, Google option).
#
# The same pipeline.ingest module that GitHub Actions runs. Nothing about the
# ingest logic is platform-specific -- only the scheduler and the way the process
# proves who it is differ, which is the point of running both.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first so edits to the pipeline code reuse the cached layer.
# requirements-ingest.txt, not requirements.txt: the ingest path never imports
# pyarrow (only the seed scripts do), and ~100 MB of unused wheel is cold-start
# latency paid on every scheduled run forever.
COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt

COPY pipeline/ ./pipeline/
COPY sql/ ./sql/

# Cloud Run jobs run to completion and exit. A non-zero exit marks the execution
# failed, which is exactly what pipeline.ingest does on error -- so Cloud Run's
# own retry and alerting see the same signal GitHub Actions does.
ENTRYPOINT ["python", "-m", "pipeline.ingest"]
