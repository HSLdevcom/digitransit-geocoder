#!/bin/bash
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DIR/elastic-wait.sh
echo "Starting API webserver"
app -p $1 -vv -d `date '+%F' --date="$(stat -c '@%Y' /data/updated)"` --docs $DIR/../docs/_build/html/
