#!/bin/bash

if [[ -n "$1" ]]; then
    if [[ "$1" = "--force" ]]; then
        FORCE=1
    else
        echo "Unknown argument $1"
    fi
fi

echo "Creating data directory"
mkdir -p /data/elasticsearch

./elastic-wait.sh

echo "Creating index"
./create_index.py

echo "Updating address data..."
if [[ "$(curl -z /data/osoitteet.csv --retry 5 -f http://ptp.hel.fi/avoindata/aineistot/Paakaupunkiseudun_osoiteluettelo.zip -o osoitteet.zip -s -L -w %{http_code})" == "200" ]] &&
      unzip -jDD osoitteet.zip &&
      mv PKS_avoin_osoiteluettelo.csv /data/osoitteet.csv &&
      rm osoitteet.zip *_kuvaus.pdf || [[ $FORCE ]]; then
    echo "Processing address data"
    ./addresses.py /data/osoitteet.csv
else
    echo -e "\tNo new data available"
fi

echo "Updating NLS municipal data..."
if [[ "$(curl -z /data/kuntajako.xml --retry 5 -f http://kartat.kapsi.fi/files/kuntajako/kuntajako_10k/etrs89/gml/TietoaKuntajaosta_2015_10k.zip -o kuntajako.zip -s -L -w %{http_code})" == "200" ]] &&
      unzip -jDD kuntajako.zip TietoaKuntajaosta_2015_10k/SuomenKuntajako_2015_10k.xml &&
      mv SuomenKuntajako_2015_10k.xml /data/kuntajako.xml &&
      rm kuntajako.zip || [[ $FORCE ]]; then
    ./mml_municipalities.py /data/kuntajako.xml
else
    echo -e "\tNo new data available"
fi

echo "Updating OpenStreetMap data..."
if [[ "$(curl -z /data/finland-latest.osm.pbf --retry 5 -f http://download.geofabrik.de/europe/finland-latest.osm.pbf -o /data/finland-latest.osm.pbf -s -L -w %{http_code})" == "200" || $FORCE ]]; then
    echo "Processing OpenStreetMap data"
    ./osm_pbf.py /data/finland-latest.osm.pbf /data/kuntajako.xml
else
    echo -e "\tNo new data available"
fi

echo "Downloading capital area service data..."
curl --retry 5 -f http://www.hel.fi/palvelukarttaws/rest/v2/unit/ -o /data/services.json &&
echo "Processing service data" &&
./palvelukartta.py /data/services.json

echo "Downloading lipas data..."
curl --retry 5 -f "http://lipas.cc.jyu.fi:80/geoserver/lipas/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=lipas:lipas_kaikki_pisteet&outputFormat=SHAPE-ZIP" -o lipas.zip &&
unzip -jDD lipas.zip &&
rm wfsrequest.txt lipas.zip &&
mv lipas_kaikki_pisteet.* /data/ &&
echo "Processing lipas data" &&
./lipas.py /data/lipas_kaikki_pisteet

echo "Updating GTFS data..."
if [[ "$(curl -z /data/stops.txt --retry 5 -f http://matka.hsl.fi/route-server/hsl.zip -o gtfs.zip -s -L -w %{http_code})" == "200" ]] &&
      unzip -DD gtfs.zip stops.txt &&
      mv stops.txt /data/ &&
      rm gtfs.zip || [[ $FORCE ]]; then
    echo "Processing GTFS data" &&
    ./stops.py /data/stops.txt
else
    echo -e "\tNo new data available"
fi

echo "Updating NLS road data..."
mkdir -p /data/nls
pushd /data/nls
TIME=$((find . -type f -printf '%T@ %p\n' | egrep '.*' || echo 0) | sort -n | tail -1 | cut -f1 -d" ")
wget -r -np -nd -l1 -N --no-verbose http://kartat.kapsi.fi/files/maastotietokanta/tiesto_osoitteilla/etrs89/gml/ &&
rm index.html* &&
popd &&
echo "Processing NLS road data" &&
find /data/nls -type f -newermt @$TIME -exec ./mml_addresses.py {} +

touch /data/updated
echo Done
