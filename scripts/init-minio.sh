#!/usr/bin/env bash
# ============================================================
# init-minio.sh — Create data lake bucket and directory prefixes
# ============================================================
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET="${DATALAKE_BUCKET_NAME:-finstreami-datalake}"

echo "Configuring MinIO client..."
mc alias set finstreami "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" --api S3v4

echo "Creating bucket: $BUCKET"
mc mb --ignore-existing "finstreami/$BUCKET"

echo "Creating prefix placeholders..."
# MinIO creates prefixes implicitly on first object write.
# We place .keep files to materialize the hierarchy.
for PREFIX in bronze/market_tick bronze/news_article bronze/social_post bronze/event bronze/quarantine \
              silver/market_tick silver/news_article silver/social_post \
              gold/features gold/signals gold/analytics; do
    echo "  -> $BUCKET/$PREFIX/"
    echo "" | mc pipe "finstreami/$BUCKET/$PREFIX/.keep"
done

echo "Setting bucket policy to download (private)..."
mc anonymous set none "finstreami/$BUCKET"

echo "MinIO initialization complete."
echo "  Bucket:   $BUCKET"
echo "  Console:  ${MINIO_ENDPOINT/9000/9001}"
