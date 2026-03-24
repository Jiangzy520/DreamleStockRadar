#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${ROOT_DIR}/.guanlan/runtime"
PORT="${PORT:-8768}"

show_status() {
  local name="$1"
  local pid_file="$2"
  if [[ ! -f "${pid_file}" ]]; then
    echo "${name}: stopped"
    return
  fi
  local pid
  pid="$(cat "${pid_file}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "${name}: running PID ${pid}"
  else
    echo "${name}: stale PID ${pid}"
  fi
}

show_status "Web" "${RUNTIME_DIR}/web.pid"
show_status "Scanner" "${RUNTIME_DIR}/scan.pid"

if ss -ltn 2>/dev/null | grep -q ":${PORT} "; then
  echo "Port ${PORT}: listening"
else
  echo "Port ${PORT}: not listening"
fi
