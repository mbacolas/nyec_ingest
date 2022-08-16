#!/bin/bash
set -e

echo "*** RUNNING INIT_DB.SH ***"

FILE=/var/lib/postgresql/db_init/public.sql
if [ -f "$FILE" ]; then
    echo "*** LOADING $FILE ***"
    psql --u postgres postgres < "$FILE"
fi

echo "*** DONE RUNNING INIT_DB.SH ***"
