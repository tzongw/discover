#!/usr/bin/env bash
# -*- coding: utf-8 -*-
set -eo pipefail

key=$1
timeout=$2
entry=$3
shift 3
while true; do
  redis-cli "$@" BLMOVE "$key" "$key" LEFT LEFT "$timeout" # no modify
  py=venv/bin/python
  if [ ! -f "$py" ]; then
    py="/usr/bin/env python"
  fi
  TZ="Asia/Shanghai" PYTHONPATH="./common/gen-py:." $py user_server/cron.py "$entry"
done
