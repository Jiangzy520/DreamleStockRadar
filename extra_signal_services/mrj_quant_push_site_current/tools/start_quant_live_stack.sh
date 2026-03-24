#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${ROOT_DIR}/.guanlan/runtime"
mkdir -p "${RUNTIME_DIR}"

WEB_PID_FILE="${RUNTIME_DIR}/web.pid"
SCAN_PID_FILE="${RUNTIME_DIR}/scan.pid"
WEB_LOG_FILE="${RUNTIME_DIR}/web.log"
SCAN_LOG_FILE="${RUNTIME_DIR}/scan.log"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8768}"

is_running() {
  local pid_file="$1"
  if [[ ! -f "${pid_file}" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "${pid_file}")"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

start_web() {
  if is_running "${WEB_PID_FILE}"; then
    echo "Web already running: PID $(cat "${WEB_PID_FILE}")"
    return
  fi
  rm -f "${WEB_PID_FILE}"
  nohup env HOST="${HOST}" PORT="${PORT}" "${ROOT_DIR}/start_guanlan_web.sh" >>"${WEB_LOG_FILE}" 2>&1 &
  echo $! >"${WEB_PID_FILE}"
  echo "Web started: PID $(cat "${WEB_PID_FILE}")  URL http://127.0.0.1:${PORT}/push"
}

start_scan() {
  if is_running "${SCAN_PID_FILE}"; then
    echo "Scanner already running: PID $(cat "${SCAN_PID_FILE}")"
    return
  fi
  rm -f "${SCAN_PID_FILE}"
  nohup "${ROOT_DIR}/start_alltick_multi_token_seconds.sh" >>"${SCAN_LOG_FILE}" 2>&1 &
  echo $! >"${SCAN_PID_FILE}"
  echo "Scanner started: PID $(cat "${SCAN_PID_FILE}")"
}

start_web
start_scan

echo "Logs:"
echo "  Web   ${WEB_LOG_FILE}"
echo "  Scan  ${SCAN_LOG_FILE}"
