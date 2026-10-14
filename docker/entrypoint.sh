#!/usr/bin/env bash
set -euo pipefail

if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
  echo "entrypoint: waiting for postgres"
  python docker/wait_for_db.py
  echo "entrypoint: alembic upgrade head"
  alembic upgrade head
fi

exec "$@"
