#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Read addresses from CSV and insert into Elasticsearch.

CSV data example:
katunimi,osoitenumero,osoitenumero2,kiinteiston_jakokirjain,kaupunki,yhdistekentta,N,E,gatan,staden,tyyppi,tyyppi_selite,ajo_pvm
Adjutantinpolku,2,,,Helsinki,Adjutantinpolku 2 Helsinki,6674867,25500025,Adjutantstigen,Helsingfors,1,osoite tai katu,2015-01-13
Virsutie,4,6,,Vantaa,Virsutie 4-6 Vantaa,6689909,25504051,Näverskovägen,Vanda,1,osoite tai katu,2015-01-13

Third field (osoitenumero2) is used when one geopoint represents multiple
street numbers from second to third field (for example "Virsutie 4-6")

Fourth field (kiinteiston_jakokirjain) is used when there are multiple geopoints
for one number (for example "Ylästöntie 76a").
"""

import csv
import logging

import click
import pyelasticsearch
from pyproj import Proj, transform

from geocoder.utils import ES, INDEX, prepare_es

# ETRS89 / GK25FIN. Note it is NOT ETRS89 / ETRS-GK25FIN, which is EPSG:3132
in_projision = Proj(init='epsg:3879')
# WGS84
out_projision = Proj(init='epsg:4326')

DOCTYPE = 'address'

logging.basicConfig(level=logging.WARNING)


def documents(csvfile):  # Elasticsearch calls records documents
    '''
    Generator of ElasticSearch index operations from a CSV file
    '''
    for line in csv.DictReader(csvfile):
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
            # The database uses both empty values and 0 for meaning addresses
            # with no number part. Normalize to 0.
            logging.info("No streetnumber:")
            logging.info(line)
            line['osoitenumero'] = 0
        if line['osoitenumero2']:
            line['osoitenumero2'] = int(line['osoitenumero2'])
        else:
            line['osoitenumero2'] = int(line['osoitenumero'])

        if line['osoitenumero'] % 2 == 0:
            line['left_side'] = True
        else:
            line['left_side'] = False

        yield ES.index_op(line)


@click.command()
@click.argument('cvsfilename', type=click.Path(exists=True))
def main(cvsfilename):
    street_mapping = {"type": "string",
                      "analyzer": "keyword",
                      "fields": {
                          "lower": {
                              "type": "string",
                              "analyzer": "myAnalyzer"}}}
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_point"},
                     "katunimi": street_mapping,
                     "gatan": street_mapping,
                     "kaupunki": street_mapping,
                     "staden": street_mapping,
                     "osoitenumero": {"type": "long"},
                     "osoitenumero2": {"type": "long"},
                     "left_side": {"type": "boolean"},
                 }}), ))

    with open(cvsfilename, encoding='latin-1') as file:
        for chunk in pyelasticsearch.bulk_chunks(documents(file), docs_per_chunk=500):
            ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
