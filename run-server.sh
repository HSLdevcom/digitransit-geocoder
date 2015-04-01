#!/bin/sh
/usr/share/elasticsearch/bin/elasticsearch \
    -p /var/run/elasticsearch.pid \
    --default.config=/etc/elasticsearch/elasticsearch.yml \
    --default.path.home=/usr/share/elasticsearch \
    --default.path.logs=/var/log/elasticsearch \
    --default.path.data=/var/lib/elasticsearch \
    --default.path.work=/tmp/elasticsearch \
    --default.path.conf=/etc/elasticsearch &

sleep 30

python3 app.py
