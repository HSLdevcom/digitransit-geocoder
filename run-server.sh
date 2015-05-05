#!/bin/sh
./elastic-wait.sh
echo "Starting API webserver"
python3 app.py -p $1 -vv -d `date '+%F' --date="$(stat -c '@%Y' /data/updated)"`
