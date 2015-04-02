#!/usr/bin/env python3

import csv
import logging
import sys

import pyelasticsearch

INDEX = 'reittiopas'
DOCTYPE = 'stop'

logging.basicConfig(level=logging.WARNING)
es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents():  # Elasticsearch calls records documents
    with open(sys.argv[1], encoding='latin-1') as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            line['location'] = (float(line['stop_lon']), float(line['stop_lat']))
            del line['stop_lon']
            del line['stop_lat']
            yield es.index_op(line)


def main():
    try:
        es.create_index(index=INDEX)
    except pyelasticsearch.exceptions.IndexAlreadyExistsError:
        pass
    try:
        es.delete_all(index=INDEX, doc_type=DOCTYPE)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
        pass  # Doesn't matter if we didn't actually delete anything

    es.put_mapping(index=INDEX, doc_type=DOCTYPE,
                   mapping={"properties": {
                       "location": {
                           "type": "geo_point"}}})

    for chunk in pyelasticsearch.bulk_chunks(documents(), docs_per_chunk=500):
        es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)

if __name__ == '__main__':
    main()
