#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${ROOT_DIR}/.guanlan/runtime"

stop_by_pid_file() {
  local name="$1"
  local pid_file="$2"
  if [[ ! -f "${pid_file}" ]]; then
    echo "${name}: not running"
    return
  fi
  local pid
  pid="$(cat "${pid_file}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}"
    echo "${name}: stopped PID ${pid}"
  else
    echo "${name}: stale PID file ${pid}"
  fi
  rm -f "${pid_file}"
}

stop_by_pid_file "Web" "${RUNTIME_DIR}/web.pid"
stop_by_pid_file "Scanner" "${RUNTIME_DIR}/scan.pid"
