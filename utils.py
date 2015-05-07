'''Utilities for working with ElasticSearch and geolocations. '''
import logging

from osgeo import osr
from urllib3.exceptions import ProtocolError

import pyelasticsearch

INDEX = 'reittiopas'
ES = pyelasticsearch.ElasticSearch('http://localhost:9200')


def send_bulk(operations, doctype):
    '''Send ElasticSearch bulk operations logging but not raising any errors.'''
    try:
        logging.debug("Sending %i index commands", len(operations))
        ES.bulk(operations, index=INDEX, doc_type=doctype)
    except pyelasticsearch.exceptions.ElasticHttpError as e:
        logging.error("ElasticSearch had a problem:")
        logging.error(e)
        logging.error(operations)


def prepare_es(mappings):
    '''
    Make sure the index exists, clean it from documents and update mappings.

    Argument is an iterable of tuples (doctype_string, mapping_dict).
    '''
    try:
        ES.create_index(index=INDEX, settings={
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
    except ProtocolError:
        logging.critical("Cannot connect to ElasticSearch, stopping...")
        raise
    for doctype, mapping in mappings:
        try:
            ES.delete_all(index=INDEX, doc_type=doctype)
        except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
            pass  # Doesn't matter if we didn't actually delete anything

        ES.put_mapping(index=INDEX, doc_type=doctype, mapping=mapping)


_source = osr.SpatialReference()
_source.ImportFromEPSG(3067)  # ETRS89 / ETRS-TM35FIN
_target = osr.SpatialReference()
_target.ImportFromEPSG(4326)  # WGS84
ETRS89_WGS84_TRANSFORM = osr.CoordinateTransformation(_source, _target)
