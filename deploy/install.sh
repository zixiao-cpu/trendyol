#!/usr/bin/env bash
set -euo pipefail
ROOT=${1:-/opt/trendyol}
case "$ROOT" in
  /*) ;;
  *) echo "install root must be an absolute path" >&2; exit 2 ;;
esac
if [[ ! "$ROOT" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "install root contains unsupported characters: $ROOT" >&2
  exit 2
fi
cd "$(dirname "$0")/.."
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/trendyol ./cmd/trendyol
if ! getent group trendyol >/dev/null; then
  groupadd --system trendyol
fi
if ! id -u trendyol >/dev/null 2>&1; then
  useradd --system --gid trendyol --home-dir "$ROOT" --shell /usr/sbin/nologin trendyol
fi
install -d -o root -g trendyol -m 0750 "$ROOT" "$ROOT/bin"
install -d -o trendyol -g trendyol -m 0750 "$ROOT/state" "$ROOT/out"
install -m 0755 bin/trendyol "$ROOT/bin/trendyol"
unit_tmp=$(mktemp)
trap 'rm -f "$unit_tmp"' EXIT
sed "s|/opt/trendyol|$ROOT|g" deploy/trendyol.service >"$unit_tmp"
install -m 0644 "$unit_tmp" /etc/systemd/system/trendyol.service
if [[ ! -f "$ROOT/.env" ]]; then
  install -o root -g trendyol -m 0640 .env.example "$ROOT/.env"
else
  chown root:trendyol "$ROOT/.env"
  chmod 0640 "$ROOT/.env"
fi
systemctl daemon-reload
echo "Edit $ROOT/.env, then: systemctl enable --now trendyol"
