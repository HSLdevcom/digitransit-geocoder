#!/usr/bin/env python3

import logging
import sys

import ijson
import pyelasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'service'

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents():
    with open(sys.argv[1], mode='rb') as jsonfile:
        for object in ijson.items(jsonfile, 'item'):
            try:
                object['location'] = (object['longitude'], object['latitude'])
            except KeyError:
                # XXX Some services don't have a location,
                # but we could get it from the address
                logging.error("Object without location: %s", object)
                continue
            del object['longitude']
            del object['latitude']
            yield es.index_op(object)


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
    for chunk in pyelasticsearch.bulk_chunks(documents(), docs_per_chunk=BULK_SIZE):
        es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)

if __name__ == '__main__':
    main()
