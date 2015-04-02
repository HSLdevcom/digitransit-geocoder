#!/usr/bin/env python3

"""
Read POIs from OSM pbf file and insert into Elasticsearch.
"""

import logging
import sys

import pyelasticsearch
from imposm.parser import OSMParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'poi'

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def nodes_callback(nodes):
    operations = []
    for osm_id, tags, lonlat in nodes:
        if 'animal_spotting' in tags:
            continue
        if ('created_by' in tags and len(tags) <= 3) or len(tags) <= 2:
            # These nodes are linked from somewhere else,
            # but we don't handle relations.
            # Vast majority of nodes are simply part of ways.
            continue
        tags['location'] = lonlat
        operations.append(es.index_op(tags))
        if len(operations) >= BULK_SIZE:
            logger.debug("Sending %i index commands", len(operations))
            try:
                es.bulk(operations, index=INDEX, doc_type=DOCTYPE)
            except pyelasticsearch.exceptions.ElasticHttpError as e:
                logger.error("ElasticSearch had a problem:")
                logger.error(e)
            operations = []
    if operations:  # Send the rest of the nodes too
        try:
            logger.debug("Sending %i index commands", len(operations))
            es.bulk(operations, index=INDEX, doc_type=DOCTYPE)
        except pyelasticsearch.exceptions.ElasticHttpError as e:
            logger.error("ElasticSearch had a problem:")
            logger.error(e)
            logger.error(operations)


def relations_callback(relations):
    for osm_id, tags, lonlat in relations:
        if "admin_level" in tags and tags["admin_level"] == "8":
            if "name:fi" in tags:
                print(tags["name:fi"])
            elif "name" in tags:
                print(tags["name"])
            else:
                print(osm_id)


def print_municipalities():
    OSMParser(concurrency=4, nodes_callback=nodes_callback, relations_callback=relations_callback).parse(sys.argv[1])


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
                   mapping={"date_detection": False,
                            "properties": {
                                "location": {
                                    "type": "geo_point"}}})

    OSMParser(concurrency=4, nodes_callback=nodes_callback).parse(sys.argv[1])

if __name__ == '__main__':
    main()
