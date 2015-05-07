#!/usr/bin/env python3

import csv
import logging

import click
import pyelasticsearch

from utils import ES, INDEX, prepare_es

DOCTYPE = 'stop'

logging.basicConfig(level=logging.WARNING)
es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents(csvfile):  # Elasticsearch calls records documents
    reader = csv.DictReader(csvfile)
    for line in reader:
        line['location'] = (float(line['stop_lon']), float(line['stop_lat']))
        del line['stop_lon']
        del line['stop_lat']
        yield ES.index_op(line)


@click.command()
@click.argument('filename', type=click.Path(exists=True))
def main(filename):
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_point"}}}), ))

    with open(filename, encoding='utf-8') as csvfile:
        for chunk in pyelasticsearch.bulk_chunks(documents(csvfile), docs_per_chunk=500):
            ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
