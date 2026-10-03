#!/bin/sh
set -eu
delay=2
while true; do
  /usr/local/bin/lyra-index "$@"
  code=$?
  echo "lyra-index exited ${code}, restarting in ${delay}s" >&2
  sleep "${delay}"
  if [ "${delay}" -lt 30 ]; then
    delay=$((delay + 1))
  fi
done
