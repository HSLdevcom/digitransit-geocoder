#!/usr/bin/env python3

"""
Read municipal boundaries from National Land Survey's GML (INSPIRE AU)
file and insert into Elasticsearch.
"""

import json
import logging

import click
from defusedxml import ElementTree
from osgeo import ogr
import pyelasticsearch

from utils import ES, prepare_es, ETRS89_WGS84_TRANSFORM

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'municipality'

GML_NS = '{http://www.opengis.net/gml/3.2}'
GN_NS = '{urn:x-inspire:specification:gmlas:GeographicalNames:3.0}'
AU_NS = '{urn:x-inspire:specification:gmlas:AdministrativeUnits:3.0}'


# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256


def parse(file):
    for member in ElementTree.parse(file).iter(GML_NS + 'featureMember'):
        # './/' is XPath for all desendants, not just direct children

        # The data includes also regional areas, but we are only interested
        # in municipalities.
        if member.findtext('.//' + AU_NS + 'nationalLevel') != "4thOrder":
            continue

        geom = member.find('.//' + GML_NS + 'MultiSurface')
        if not geom:
            # The file also includes boundaries between every neighbouring
            # municipality as LineStrings, which we ignore
            if member.find('.//' + GML_NS + 'LineString'):
                continue
            # We shouldn't encounter any other geometry types.
            # If we do, something has changed and all bets are off.
            raise Exception("Found unexpected geometry type for member",
                            member[0].get(GML_NS + 'id'))

        # OGR is horribly non-functional, so this variable is created
        # just for the projection
        ogr_geom = ogr.CreateGeometryFromGML(ElementTree.tostring(geom).decode())
        ogr_geom.Transform(ETRS89_WGS84_TRANSFORM)

        # Yes, it's hacky to convert to GeoJSON and then back, but ogr knows
        # what's valid GeoJSON better than we do, and pyelasticsearch wants
        # a Python dictionary
        document = {'boundaries': json.loads(ogr_geom.ExportToJson())}

        for name in member.iter(GN_NS + 'GeographicalName'):
            language = name.find(GN_NS + 'language').text
            name_text = name.find('.//' + GN_NS + 'text').text
            if language == 'fin':
                document['nimi'] = name_text
            elif language == 'swe':
                document['namn'] = name_text
            else:
                raise Exception("Unknown language found")
        yield document


@click.command()
@click.argument('file', type=click.File(encoding='latin-1'))
def main(file):
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "boundaries": {
                         "type": "geo_shape"}}}), ))

    for document in parse(file):
        try:
            ES.index(index=INDEX, doc_type=DOCTYPE, doc=document)
        except pyelasticsearch.exceptions.ElasticHttpError as e:
            logger.error(e)
            logger.error(document['nimi'])


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
