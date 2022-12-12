#!/usr/bin/env bash
# -*- coding: utf-8 -*-
set -eo pipefail

key="$1"
timeout="$2"
shift 2
value=$(redis-cli "$@" BLMOVE "$key" "$key" LEFT LEFT "$timeout")
if [ -z "$value" ]; then
    exit 0
fi
py=venv/bin/python
if [ ! -f "$py" ]; then
    py="/usr/bin/env python"
fi
PYTHONPATH="./common/gen-py:." $py user_server/cron.py
