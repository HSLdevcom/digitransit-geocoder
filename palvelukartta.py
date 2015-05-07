#!/usr/bin/env python3

import logging

import click
import ijson
import pyelasticsearch

from utils import ES, INDEX, prepare_es

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DOCTYPE = 'service'

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents(jsonfile):
    for i in ijson.items(jsonfile, 'item'):
        try:
            i['location'] = (i['longitude'], i['latitude'])
        except KeyError:
            # XXX Some services don't have a location,
            # but we could get it from the address
            logging.error("Object without location: %s", i)
            continue
        del i['longitude']
        del i['latitude']
        yield ES.index_op(i)


@click.command()
@click.argument('jsonfile', type=click.File(mode='rb'))
def main(jsonfile):
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_point"}}}), ))
    for chunk in pyelasticsearch.bulk_chunks(documents(jsonfile),
                                             docs_per_chunk=BULK_SIZE):
        ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
