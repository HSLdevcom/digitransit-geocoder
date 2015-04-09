#!/bin/bash

/usr/share/elasticsearch/bin/elasticsearch \
    -p /var/run/elasticsearch.pid \
    --default.config=/etc/elasticsearch/elasticsearch.yml \
    --default.path.home=/usr/share/elasticsearch \
    --default.path.logs=/var/log/elasticsearch \
    --default.path.data=/data/elasticsearch \
    --default.path.work=/tmp/elasticsearch \
    --default.path.conf=/etc/elasticsearch &

while sleep 1
do
    if grep --quiet "started" /var/log/elasticsearch/elasticsearch.log
    then
        exit 0
    fi
done
