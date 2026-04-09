#!/usr/bin/env bash
set -euo pipefail

container_name="event-pipeline-postgres"

if docker ps -a --format '{{.Names}}' | grep -qx "${container_name}"; then
  docker rm -f "${container_name}" >/dev/null
fi

docker run -d \
  --name "${container_name}" \
  -e POSTGRES_DB=event_pipeline \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -p 5432:5432 \
  postgres:17-alpine >/dev/null

until docker exec "${container_name}" pg_isready -U postgres -d event_pipeline >/dev/null 2>&1; do
  sleep 1
done

echo "postgres is ready at postgres://postgres:postgres@127.0.0.1:5432/event_pipeline"
