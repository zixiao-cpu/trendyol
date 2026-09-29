#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE=${ENV_FILE:-.env}
BINARY=${BINARY:-bin/trendyol}
LOG=${LOG:-run_trendyol_go.log}

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
  fi
}

build() {
  mkdir -p bin
  go build -trimpath -o "$BINARY" ./cmd/trendyol
  echo "[build] $BINARY"
}

run() {
  load_env
  [[ -x "$BINARY" ]] || build
  if pgrep -f "${BINARY} run" >/dev/null 2>&1; then
    echo "Go crawler is already running" >&2
    exit 3
  fi
  mkdir -p state out
  nohup "$BINARY" run >>"$LOG" 2>&1 &
  echo $! > state/trendyol.pid
  echo "[run] pid=$! log=$LOG db=${DB_PATH:-state/trendyol.db}"
}

command=${1:-help}
shift || true
case "$command" in
  build) build ;;
  run) run ;;
  init|status|monitor|discover|import-urls|recover|recover-worker|reprioritize|prepare-usd|prepare-content-backfill|reset-candidates|normalize-data|export|validate|help)
    load_env
    [[ -x "$BINARY" ]] || build
    "$BINARY" "$command" "$@"
    ;;
  test)
    gofmt -w cmd internal
    go test ./...
    go vet ./...
    ;;
  *) echo "usage: bash deploy_run.sh [build|init|status|monitor|discover|import-urls|run|recover|recover-worker|reprioritize|prepare-usd|prepare-content-backfill|reset-candidates|normalize-data|export|validate|test|help]" >&2; exit 2 ;;
esac
