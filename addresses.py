#!/usr/bin/env python3

"""
Read addresses from CSV and insert into Elasticsearch.

CSV data example:
katunimi,osoitenumero,osoitenumero2,kiinteiston_jakokirjain,kaupunki,yhdistekentta,N,E,gatan,staden,tyyppi,tyyppi_selite,ajo_pvm
Adjutantinpolku,2,,,Helsinki,Adjutantinpolku 2 Helsinki,6674867,25500025,Adjutantstigen,Helsingfors,1,osoite tai katu,2015-01-13
Adjutantinpuisto,0,,,Helsinki,Adjutantinpuisto  Helsinki,6674852,25500094,Adjutantparken,Helsingfors,2,puisto tai kenttä,2015-01-13
Adolf Lindforsin tie,1,,,Helsinki,Adolf Lindforsin tie 1 Helsinki,6679671,25494149,Adolf Lindfors väg,Helsingfors,1,osoite tai katu,2015-01-13
Adolf Lindforsin tie,11,,,Helsinki,Adolf Lindforsin tie 11 Helsinki,6679775,25493855, Adolf Lindfors väg,Helsingfors,1,osoite tai katu,2015-01-13

Third field (osoitenumero2) is used when one geopoint represents multiple
street numbers from second to third field (for example "Virsutie 4-6")

Fourth field (kiinteiston_jakokirjain) is used when there are multiple geopoints
for one number (for example "Ylästöntie 76a").
"""

import csv
import logging
import sys

import pyelasticsearch
from pyproj import Proj, transform

# ETRS89 / GK25FIN. Note it is NOT ETRS89 / ETRS-GK25FIN, which is EPSG:3132
in_projision = Proj(init='epsg:3879')
# WGS84
out_projision = Proj(init='epsg:4326')

INDEX = 'reittiopas'
DOCTYPE = 'address'

logging.basicConfig(level=logging.WARNING)
es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents():  # Elasticsearch calls records documents
    with open(sys.argv[1], encoding='latin-1') as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            del line['yhdistekentta']
            del line['tyyppi']
            del line['tyyppi_selite']
            del line['ajo_pvm']
            # output from transform is lon, lat
            line['location'] = transform(in_projision, out_projision,
                                         float(line['E']), float(line['N']))
            del line['N']
            del line['E']
            if line['osoitenumero']:
                line['osoitenumero'] = int(line['osoitenumero'])
            else:
                logging.warning("No streetnumber:")
                logging.warning(line)
            if line['osoitenumero2']:
                line['osoitenumero2'] = int(line['osoitenumero2'])
            yield es.index_op(line)


def main():
    try:
        es.create_index(index=INDEX, settings={
            "analysis": {
                "analyzer": {
                    "myAnalyzer": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": ["myLowerCaseFilter"]}},
                "filter": {
                    "myLowerCaseFilter": {
                        "type": "lowercase"}}}})
    except pyelasticsearch.exceptions.IndexAlreadyExistsError:
        pass
    try:
        es.delete_all(index=INDEX, doc_type=DOCTYPE)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
        pass  # Doesn't matter if we didn't actually delete anything

    es.put_mapping(index=INDEX, doc_type=DOCTYPE,
                   mapping={
                       "properties": {
                           "location": {
                               "type": "geo_point"},
                           "katunimi": {
                               "type": "string",
                               "analyzer": "keyword",
                               "fields": {
                                   "raw": {
                                       "type": "string",
                                       "analyzer": "myAnalyzer"}}}}})

    for chunk in pyelasticsearch.bulk_chunks(documents(), docs_per_chunk=500):
        es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)

if __name__ == '__main__':
    main()
