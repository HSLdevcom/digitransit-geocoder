#!/bin/sh

/usr/share/elasticsearch/bin/elasticsearch \
    -p /var/run/elasticsearch.pid \
    --default.config=/etc/elasticsearch/elasticsearch.yml \
    --default.path.home=/usr/share/elasticsearch \
    --default.path.logs=/var/log/elasticsearch \
    --default.path.data=/var/lib/elasticsearch \
    --default.path.work=/tmp/elasticsearch \
    --default.path.conf=/etc/elasticsearch &
sleep 15 && \
    ./addresses.py geocoding-data/PKS_avoin_osoiteluettelo.csv &&
    ./mml.py geocoding-data/SuomenKuntajako_2015_10k.xml &&
    ./osm_pbf.py geocoding-data/finland-latest.osm.pbf &&
    ./palvelukartta.py geocoding-data/services.json
