#!/usr/bin/env bash
# -*- coding: utf-8 -*-
set -euo pipefail

db_file="$1"
wal_file="${1}-wal"
remote_file="$2"
echo "$db_file" "$wal_file" "$remote_file"

if [[ $(uname -s) == "Darwin" ]]; then
  stat_arg='-f%Sm'
else
  stat_arg='-c%y'
fi

last_ts=
while true; do
    if [[ ! -f "$wal_file" ]]; then
      sleep 1
      continue
    fi
    ts=$(stat "$stat_arg" "$wal_file")
    if [[ "$ts" == "$last_ts" ]]; then
      sleep 0.1
      continue
    fi
    echo "ts changed: $last_ts -> $ts"
    if sqlite3-rsync "$db_file" "$remote_file"; then
      last_ts="$ts"
    else
      echo "rsync fail: $last_ts -> $ts"
      sleep 1
    fi
done
