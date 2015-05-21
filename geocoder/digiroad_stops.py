#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import logging

import click
import pyelasticsearch
from pyproj import Proj, transform

from geocoder.utils import ES, INDEX, prepare_es

# ETRS89 / ETRS-TM35FIN
in_projision = Proj(init='epsg:3067')
# WGS84
out_projision = Proj(init='epsg:4326')

DOCTYPE = 'digiroad_stop'

logging.basicConfig(level=logging.WARNING)


def documents(csvfile):  # Elasticsearch calls records documents
    '''
    Generator of ElasticSearch index operations from a CSV file
    '''
    for line in csv.DictReader(csvfile, delimiter=';'):
        # output from transform is lon, lat
        line['location'] = transform(in_projision, out_projision,
                                     float(line['COORDINATE_X']), float(line['COORDINATE_Y']))
        del line['COORDINATE_X']
        del line['COORDINATE_Y']

        # ElasticSearch doesn't like empty strings in date fields
        for field in ['VALID_FROM', 'VALID_TO']:
            if line[field] == '':
                line[field] = None

        yield ES.index_op(line)


@click.command()
@click.argument('cvsfilename', type=click.Path(exists=True))
def main(cvsfilename):
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_point"}}}), ))

    # Currently Digiroad uses Microsoft standard of prepending UTF-8 text file with BOM.
    # The utf-8-sig encoding will remove it from the stream, if it's there.
    with open(cvsfilename, encoding='utf-8-sig') as file:
        for chunk in pyelasticsearch.bulk_chunks(documents(file), docs_per_chunk=500):
            ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
