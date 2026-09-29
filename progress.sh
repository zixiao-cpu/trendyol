#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE=${ENV_FILE:-.env}
BINARY=${BINARY:-bin/trendyol}
INTERVAL=${INTERVAL:-5}
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi
[[ -x "$BINARY" ]] || go build -trimpath -o "$BINARY" ./cmd/trendyol

exec "$BINARY" monitor --interval "${INTERVAL}s"
