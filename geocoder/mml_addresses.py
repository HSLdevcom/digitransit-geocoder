#!/usr/bin/env python3

"""
Read roads with addressinfo from National Land Survey's GML file,
interpolate and insert into Elasticsearch.
"""

from contextlib import contextmanager
from functools import partial
import json
import logging
from os.path import basename
from zipfile import ZipFile, BadZipFile

import click
from defusedxml import ElementTree
from osgeo import ogr
import pyelasticsearch

from geocoder.utils import ES, INDEX, ETRS89_WGS84_TRANSFORM, prepare_es

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

DOCTYPE = 'interpolated_address'

NLS_NS = '{http://xml.nls.fi/XML/Namespace/Maastotietojarjestelma/SiirtotiedostonMalli/2011-02}'


class NoNameFoundException(Exception):
    '''Raised when road or point element has no street name'''
    pass


def nls_find(line, target):
    '''Find target element in NLS XML schema from ElementTree Element'''
    return line.findtext('.//' + NLS_NS + target)


def find_min(line, side):
    '''Find min address number from NLS road element'''
    return nls_find(line, 'minOsoitenumero' + side)


def find_max(line, side):
    '''Find max address number from NLS road element'''
    return nls_find(line, 'maxOsoitenumero' + side)


def find_minmax(line, doc, side):
    '''
    Save min and max address numbers from a given side of NLS road element into dict
    '''
    def _not_zero_or_none(a):
        '''Sometimes NLS uses '0' to indicate a missing value'''
        return a is not '0' and a is not None

    if (_not_zero_or_none(find_min(line, side)) or
            _not_zero_or_none(find_max(line, side))):
        if (_not_zero_or_none(find_min(line, side)) and
                _not_zero_or_none(find_max(line, side))):
            doc['min_' + side.lower()] = int(find_min(line, side))
            doc['max_' + side.lower()] = int(find_max(line, side))
        else:
            logger.error('%s has min or max data for side %s, but not both',
                         line.get('gid'), side)


def find_name(doc, line):
    '''
    Save Finnish and Swedish names from a NLS road element into dict.

    Raises NoNameFoundException if neither is found.
    '''
    if nls_find(line, 'nimi_ruotsi') is not None:
        doc['namn'] = nls_find(line, 'nimi_ruotsi')
    if nls_find(line, 'nimi_suomi') is not None:
        doc['nimi'] = nls_find(line, 'nimi_suomi')
    # This shouldn't happen of course, but data can be bad
    if 'namn' not in doc and 'nimi' not in doc:
        logger.error('No name found for %s', line.get('gid'))
        raise NoNameFoundException
    # If one language is missing, fill it from the other so that we can
    # always search only in the user's main language
    elif 'namn' not in doc:
        doc['namn'] = doc['nimi']
    elif 'nimi' not in doc:
        doc['nimi'] = doc['namn']


def gml2dict(element, gml_type):
    '''Convert GML Element into a GeoJSON like dict'''
    # OGR is horribly non-functional, so this variable is created
    # just for the projection
    ogr_geom = ogr.CreateGeometryFromGML(
        '<' + gml_type + '>' + ElementTree.tostring(element).decode() +
        '</' + gml_type + '>')
    ogr_geom.Transform(ETRS89_WGS84_TRANSFORM)
    ogr_geom.FlattenTo2D()

    # Yes, it's hacky to convert to GeoJSON and then back, but ogr knows
    # what's valid GeoJSON better than we do, and pyelasticsearch wants
    # a Python dictionary
    return json.loads(ogr_geom.ExportToJson())


@click.command()
@click.option('-v', '--verbose', count=True)
@click.argument('files', nargs=-1, type=click.File('rb'), required=True)
def main(files, verbose=0):
    '''
    Read National LandSurvey's GML files (XML or zips containing XML files)
    into ElasticSearch.
    '''
    if verbose >= 1:
        progressbar = partial(
            click.progressbar, label="Processing GML files",
            item_show_func=lambda x: (x and click.format_filename(x.name)))
    else:
        @contextmanager
        def progressbar(data):
            '''Dummy no-op context manager'''
            yield data
    if verbose == 2:
        logger.setLevel(logging.DEBUG)

    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_shape",
                         "tree": "quadtree",
                         # For some reason ES is faster with coarse index than no index at all
                         "precision": "10km"},
                     "filename": {
                         "type": "string",
                         "analyzer": "keyword"}}}), ))

    with progressbar(files) as bar:
        for chunk in pyelasticsearch.bulk_chunks(documents(bar), docs_per_chunk=500):
            ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


def documents(files):
    '''
    Process given XML or zip files into ElasticSearch bulk operations
    '''
    for i in files:
        logger.info('Processing file %s', i.name)
        try:
            with ZipFile(i) as z:
                for j in z.namelist():
                    with z.open(j) as f:
                        yield from read_file(f, basename(j))
        except BadZipFile:
            i.seek(0)  # Trying to open as zip has read some of the file
            yield from read_file(i, basename(i.name))


def read_file(file, filename):
    '''
    Process given XML file into ElasticSearch bulk operations
    '''
    # Delete all previous documents from this map tile
    # (the NLS data is divided into files by tile)
    ES.delete_by_query(index=INDEX, doc_type=DOCTYPE, query="filename:%s" % filename)
    et = ElementTree.parse(file)
    for line in et.iterfind('.//' + NLS_NS + 'Tieviiva'):
        doc = {'filename': filename}
        # Some road parts have only either left or right side
        find_minmax(line, doc, 'Vasen')
        find_minmax(line, doc, 'Oikea')

        # Some parts of the roads do not have any addresses
        if 'min_vasen' not in doc and 'min_oikea' not in doc:
            logger.debug('No address data found for %s', line.get('gid'))
            continue

        try:
            find_name(doc, line)
        except NoNameFoundException:
            continue

        doc['location'] = gml2dict(
            line.find('.//{http://www.opengis.net/gml}posList'), 'LineString')
        yield ES.index_op(doc)

    for line in et.iterfind('.//' + NLS_NS + 'Osoitepiste'):
        doc = {'osoitenumero': line.findtext('.//' + NLS_NS + 'numero')}
        try:
            find_name(doc, line)
        except NoNameFoundException:
            continue
        doc['location'] = gml2dict(
            line.find('.//{http://www.opengis.net/gml}pos'), 'Point')
        yield ES.index_op(doc)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
