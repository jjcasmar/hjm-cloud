FROM python:3.13-slim

# openssl: self-signed certificate on first start; tzdata: honour TZ in logs
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY hjm_local_cloud ./hjm_local_cloud
RUN pip install --no-cache-dir .

ENV HJM_DATA_DIR=/data \
    PYTHONUNBUFFERED=1
VOLUME /data
EXPOSE 8443 8080

CMD ["hjm-local-cloud"]
