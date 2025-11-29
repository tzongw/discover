#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# set -eo pipefail

db_file="$1"
wal_file="${1}-wal"
remote_file="$2"
echo "$db_file" "$wal_file" "$remote_file"

if [[ $(uname -s) == "Darwin" ]]; then
  stat_arg='-f%m'
  date_arg='-r'
else
  stat_arg='-c%Y'
  date_arg='-d'
fi

last_ts=
while true; do
    ts=$(stat "$stat_arg" "$wal_file")
    if [[ "$ts" != "$last_ts" ]]; then
      last_ts="$ts"
      echo "ts change", $(date "$date_arg" "$ts")
      # sqlite3-rsync "$db_file" "$remote_file"
    else
      sleep 0.1
    fi
done