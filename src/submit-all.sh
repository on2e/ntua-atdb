#!/bin/bash

[ -d "$ATDB_PROJECT_HOME" ] && cd $ATDB_PROJECT_HOME
mkdir -p out logs

shopt -s nullglob
for f in src/main/*.py; do
    base=$(basename "$f" .py)
    echo -n "Submitting file $base.py..."
    spark-submit "$f" > out/"$base".out 2> logs/"$base".log
    sleep 3
    echo
done
shopt -u nullglob
