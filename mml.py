#!/usr/bin/env python3

"""
Read municipial boundaries from National Land Survey's GML (INSPIRE AU)
file and insert into Elasticsearch.
"""

import json
import logging
import sys

from defusedxml import ElementTree
from osgeo import ogr, osr
import pyelasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'municipiality'

GML_NS = '{http://www.opengis.net/gml/3.2}'
GN_NS = '{urn:x-inspire:specification:gmlas:GeographicalNames:3.0}'
AU_NS = '{urn:x-inspire:specification:gmlas:AdministrativeUnits:3.0}'

source = osr.SpatialReference()
source.ImportFromEPSG(3067)  # ETRS89 / ETRS-TM35FIN
target = osr.SpatialReference()
target.ImportFromEPSG(4326)  # WGS84
transform = osr.CoordinateTransformation(source, target)

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256


def main():
    es = pyelasticsearch.ElasticSearch('http://localhost:9200')
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
                           "type": "geo_shape"}}})

    for member in ElementTree.parse(sys.argv[1]).iter(GML_NS + 'featureMember'):
        # './/' is XPath for all desendants, not just direct children
        geom = member.find('.//' + GML_NS + 'MultiSurface')
        if not geom:
            # The file also includes boundaries between every neighbouring
            # municipiality as LineStrings, which we ignore
            if member.find('.//' + GML_NS + 'LineString'):
                continue
            # We shouldn't encounter any other geometry types.
            # If we do, something has changed and all bets are off.
            raise Exception("Found unexpected geometry type for member",
                            member[0].get(GML_NS + 'id'))

        # OGR is horribly non-functional, so this variable is created
        # just for the projection
        ogr_geom = ogr.CreateGeometryFromGML(ElementTree.tostring(geom).decode())
        ogr_geom.Transform(transform)

        # Yes, it's hacky to convert to GeoJSON and then back, but ogr knows
        # what's valid GeoJSON better than we do, and pyelasticsearch wants
        # a Python dictionary
        document = {'location': json.loads(ogr_geom.ExportToJson())}

        for name in member.iter(GN_NS + 'GeographicalName'):
            language = name.find(GN_NS + 'language').text
            name_text = name.find('.//' + GN_NS + 'text').text
            if language == 'fin':
                document['nimi'] = name_text
            elif language == 'swe':
                document['namn'] = name_text
            else:
                raise Exception("Unknown language found")

        try:
            es.index(index=INDEX, doc_type=DOCTYPE, doc=document)
        except pyelasticsearch.exceptions.ElasticHttpError as e:
            logger.error(e)
            logger.error(document['nimi'])

if __name__ == '__main__':
    main()
