#!/usr/bin/env bash
set -euo pipefail

container_name="event-pipeline-postgres"

if docker ps -a --format '{{.Names}}' | grep -qx "${container_name}"; then
  docker rm -f "${container_name}" >/dev/null
fi
