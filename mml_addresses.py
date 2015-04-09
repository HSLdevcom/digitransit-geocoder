#!/usr/bin/env python3

"""
Read roads with addressinfo from National Land Survey's GML file,
interpolate and insert into Elasticsearch.
"""

import json
import logging
import sys
from zipfile import ZipFile

from defusedxml import ElementTree
from osgeo import ogr, osr
import pyelasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'interpolated_address'

NLS_NS = '{http://xml.nls.fi/XML/Namespace/Maastotietojarjestelma/SiirtotiedostonMalli/2011-02}'

source = osr.SpatialReference()
source.ImportFromEPSG(3067)  # ETRS89 / ETRS-TM35FIN
target = osr.SpatialReference()
target.ImportFromEPSG(4326)  # WGS84
transform = osr.CoordinateTransformation(source, target)

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


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
                           "type": "geo_shape"}}})
    for chunk in pyelasticsearch.bulk_chunks(documents(), docs_per_chunk=BULK_SIZE):
        es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


def documents():
    for i in sys.argv[1:]:
        if i[-4:] == ".zip":
            with ZipFile(i) as z:
                for j in z.namelist():
                    yield from read_file(z.open(j))
        else:
            yield from read_file(i)


def read_file(filename):
    # XXX handle osoitepisteet
    # Find all elements with child 'minOsoitenumeroVasen'
    for line in ElementTree.parse(filename).iterfind('.//' + NLS_NS + 'minOsoitenumeroVasen/..'):
        doc = {
            'min_vasen': int(line.findtext('.//' + NLS_NS + 'minOsoitenumeroVasen')),
            'max_vasen': int(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroVasen')),
            'min_oikea': int(line.findtext('.//' + NLS_NS + 'minOsoitenumeroOikea')),
            'max_oikea': int(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroOikea'))}

        if line.findtext('.//' + NLS_NS + 'nimi_ruotsi') is not None:
            doc['namn'] = line.findtext('.//' + NLS_NS + 'nimi_ruotsi')
        if line.findtext('.//' + NLS_NS + 'nimi_suomi') is not None:
            doc['nimi'] = line.findtext('.//' + NLS_NS + 'nimi_suomi')
        if 'namn' not in doc and 'nimi' not in doc:
            logging.error('No name found for %s', line.get('gid'))
            continue
        elif 'namn' not in doc:
            doc['namn'] = doc['nimi']
        elif 'nimi' not in doc:
            doc['nimi'] = doc['namn']

        geom = line.find('.//{http://www.opengis.net/gml}posList')

        # OGR is horribly non-functional, so this variable is created
        # just for the projection
        ogr_geom = ogr.CreateGeometryFromGML('<LinearRing>' + ElementTree.tostring(geom).decode() + '</LinearRing>')
        ogr_geom.Transform(transform)
        ogr_geom.FlattenTo2D()

        # Yes, it's hacky to convert to GeoJSON and then back, but ogr knows
        # what's valid GeoJSON better than we do, and pyelasticsearch wants
        # a Python dictionary
        doc['location'] = json.loads(ogr_geom.ExportToJson())
        yield es.index_op(doc)


if __name__ == '__main__':
    main()
