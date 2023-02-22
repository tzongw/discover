#!/usr/bin/env bash
# -*- coding: utf-8 -*-
set -eo pipefail

key=$1
timeout=$2
len=${#key}
shift 2
while true; do
  value=$(redis-cli "$@" BLPOP "$key" "$timeout")
  if [ -z "$value" ]; then
    exit 0
  fi
  task=${value:len} # skip key string
  py=venv/bin/python
  if [ ! -f "$py" ]; then
    py="/usr/bin/env python"
  fi
  PYTHONPATH="./common/gen-py:." $py user_server/cron.py "+" "$task"
done
