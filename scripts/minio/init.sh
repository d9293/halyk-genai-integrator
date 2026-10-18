#!/bin/sh
# Prepares the local MinIO (infra) for running the whole flow locally:
#   bpm-bucket/attstore-mock/       sample source PDFs that BPM tasks point to (plays Attstore)
#   bpm-bucket/register-compare/samples/   sample Word/Excel reports the mock AI links to
# Both prefixes get public read, so they download without keys, like Attstore
# links and AI report links do. The rest of bpm-bucket stays private.
# Safe to run again. Existing objects elsewhere in the bucket are not touched.
set -eu

CONTAINER=${MINIO_CONTAINER:-infra-minio}
BUCKET=${MINIO_BUCKET:-bpm-bucket}
SAMPLES=$(dirname "$0")/samples

mc() { docker exec -i "$CONTAINER" mc "$@"; }

# Log in with the container's own admin credentials.
docker exec "$CONTAINER" sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"' >/dev/null

mc mb --ignore-existing "local/$BUCKET" >/dev/null

upload() { # upload <file> <object path> <content type>
	mc pipe --attr "Content-Type=$3" "local/$BUCKET/$2" <"$SAMPLES/$1" >/dev/null
	echo "  http://localhost:9000/$BUCKET/$2"
}

echo "Sample source documents (fake Attstore):"
upload registry-a.pdf attstore-mock/registry-a.pdf application/pdf
upload registry-b.pdf attstore-mock/registry-b.pdf application/pdf

echo "Sample AI reports:"
upload report.docx register-compare/samples/report.docx application/vnd.openxmlformats-officedocument.wordprocessingml.document
upload report.xlsx register-compare/samples/report.xlsx application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

mc anonymous set download "local/$BUCKET/attstore-mock" >/dev/null
mc anonymous set download "local/$BUCKET/register-compare/samples" >/dev/null
echo "Public read: $BUCKET/attstore-mock/, $BUCKET/register-compare/samples/"
