#!/usr/bin/env bash
set -euo pipefail

if command -v pg_lsclusters >/dev/null 2>&1; then
  ver="$(pg_lsclusters --no-header | awk 'NR==1 { print $1 }')"
  name="$(pg_lsclusters --no-header | awk 'NR==1 { print $2 }')"
  if [ -n "${ver}" ] && [ -n "${name}" ]; then
    sudo pg_ctlcluster "${ver}" "${name}" start || true
  fi
fi
if ! pg_isready -q; then
  sudo service postgresql start
fi

if ! redis-cli ping >/dev/null 2>&1; then
  sudo service redis-server start
fi

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='gol'" | grep -q 1; then
  sudo -u postgres psql -c "CREATE USER gol WITH PASSWORD 'gol' SUPERUSER"
fi
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='gol'" | grep -q 1; then
  sudo -u postgres psql -c "CREATE DATABASE gol OWNER gol"
fi

pg_isready -q
redis-cli ping | grep -q PONG
