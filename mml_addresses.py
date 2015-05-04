#!/usr/bin/env python3

"""
Read roads with addressinfo from National Land Survey's GML file,
interpolate and insert into Elasticsearch.
"""

import argparse
import json
import logging
from zipfile import ZipFile

from defusedxml import ElementTree
from osgeo import ogr, osr
import pyelasticsearch

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

INDEX = 'reittiopas'
DOCTYPE = 'interpolated_address'

NLS_NS = '{http://xml.nls.fi/XML/Namespace/Maastotietojarjestelma/SiirtotiedostonMalli/2011-02}'

source = osr.SpatialReference()
source.ImportFromEPSG(3067)  # ETRS89 / ETRS-TM35FIN
target = osr.SpatialReference()
target.ImportFromEPSG(4326)  # WGS84
transform = osr.CoordinateTransformation(source, target)

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action='count',
                        help="Use once for info, twice for more")
    parser.add_argument('filenames', metavar='N', nargs='+',
                        help='XML or zips containing XML files to process')
    args = parser.parse_args()
    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    elif args.verbose == 2:
        logger.setLevel(logging.DEBUG)

    try:
        es.create_index(index=INDEX)
    except pyelasticsearch.exceptions.IndexAlreadyExistsError:
        pass

    es.put_mapping(index=INDEX, doc_type=DOCTYPE,
                   mapping={"properties": {
                       "location": {
                           "type": "geo_shape",
                           "tree": "quadtree",
                           # For some reason ES is faster with coarse index than no index at all
                           "precision": "10km"},
                       "filename": {
                           "type": "string",
                           "analyzer": "keyword"}}})

    for chunk in pyelasticsearch.bulk_chunks(documents(args.filenames), docs_per_chunk=500):
        es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


def documents(filenames):
    for i in filenames:
        logger.info('Processing file %s', i)
        if i[-4:] == ".zip":
            with ZipFile(i) as z:
                for j in z.namelist():
                    with z.open(j) as f:
                        yield from read_file(f, j)
        else:
            with open(i) as f:
                yield from read_file(f, i)


def _not_zero_or_none(a):
    return a is not '0' and a is not None
def read_file(file, filename):
    # Delete all documents from this map tile (the NLS data is divided into files by tile)
    es.delete_by_query(index=INDEX, doc_type=DOCTYPE, query="filename:%s" % filename)
    et = ElementTree.parse(file)
    # Find all elements with child 'minOsoitenumeroVasen'
    for line in et.iterfind('.//' + NLS_NS + 'Tieviiva'):
        doc = {'filename': filename}
        # Some road parts have only either left or right side
        if (_not_zero_or_none(line.findtext('.//' + NLS_NS + 'minOsoitenumeroVasen')) or
            _not_zero_or_none(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroVasen'))):
            if (_not_zero_or_none(line.findtext('.//' + NLS_NS + 'minOsoitenumeroVasen')) and
                _not_zero_or_none(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroVasen'))):
                doc['min_vasen'] = int(line.findtext('.//' + NLS_NS +
                                                     'minOsoitenumeroVasen')),
                doc['max_vasen'] = int(line.findtext('.//' + NLS_NS +
                                                     'maxOsoitenumeroVasen')),
            else:
                logger.error('%s has min or max data for left side, but not both',
                              line.get('gid'))
                continue
        if (_not_zero_or_none(line.findtext('.//' + NLS_NS + 'minOsoitenumeroOikea')) or
            _not_zero_or_none(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroOikea'))):
            if (_not_zero_or_none(line.findtext('.//' + NLS_NS + 'minOsoitenumeroOikea')) and
                _not_zero_or_none(line.findtext('.//' + NLS_NS + 'maxOsoitenumeroOikea'))):
                doc['min_oikea'] = int(line.findtext('.//' + NLS_NS +
                                                     'minOsoitenumeroOikea')),
                doc['max_oikea'] = int(line.findtext('.//' + NLS_NS +
                                                     'maxOsoitenumeroOikea')),
            else:
                logger.error('%s has min or max data for right side, but not both',
                              line.get('gid'))
                continue
        # Some parts of the roads do not have any addresses
        if 'min_vasen' not in doc and 'min_oikea' not in doc:
            logger.debug('No address data found for %s', line.get('gid'))
            continue

        if line.findtext('.//' + NLS_NS + 'nimi_ruotsi') is not None:
            doc['namn'] = line.findtext('.//' + NLS_NS + 'nimi_ruotsi')
        if line.findtext('.//' + NLS_NS + 'nimi_suomi') is not None:
            doc['nimi'] = line.findtext('.//' + NLS_NS + 'nimi_suomi')
        # This shouldn't happen of course, but data can be bad
        if 'namn' not in doc and 'nimi' not in doc:
            logger.error('No name found for %s', line.get('gid'))
            continue
        # If one language is missing, fill it from the other so that we can
        # always search only in the user's main language
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

    for line in et.iterfind('.//' + NLS_NS + 'Osoitepiste'):
        doc = {'osoitenumero': line.findtext('.//' + NLS_NS + 'numero')}
        if line.findtext('.//' + NLS_NS + 'nimi_ruotsi') is not None:
            doc['namn'] = line.findtext('.//' + NLS_NS + 'nimi_ruotsi')
        if line.findtext('.//' + NLS_NS + 'nimi_suomi') is not None:
            doc['nimi'] = line.findtext('.//' + NLS_NS + 'nimi_suomi')
        # This shouldn't happen of course, but data can be bad
        if 'namn' not in doc and 'nimi' not in doc:
            logger.error('No name found for %s', line.get('gid'))
            continue

        geom = line.find('.//{http://www.opengis.net/gml}pos')
        # OGR is horribly non-functional, so this variable is created
        # just for the projection
        ogr_geom = ogr.CreateGeometryFromGML('<Point>' + ElementTree.tostring(geom).decode() + '</Point>')
        ogr_geom.Transform(transform)

        # Yes, it's hacky to convert to GeoJSON and then back, but ogr knows
        # what's valid GeoJSON better than we do, and pyelasticsearch wants
        # a Python dictionary
        doc['location'] = json.loads(ogr_geom.ExportToJson())
        yield es.index_op(doc)


if __name__ == '__main__':
    main()
