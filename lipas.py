#!/usr/bin/env python3
import logging

import click
from osgeo import ogr
import pyelasticsearch
import shapefile

from utils import ES, INDEX, prepare_es, ETRS89_WGS84_TRANSFORM

DOCTYPE = 'lipas'


def documents(shapefilename):
    '''Generator of ElasticSearch index operations from a shape file.'''
    for rec in shapefile.Reader(shapefilename,
                                encoding='latin-1').iterShapeRecords():
        if not rec.shape.points:
            logging.warning("No coordinate data for %s", rec.record[1])
            continue
        if not ((rec.shape.points[0][0] == rec.record[21]) and
                (rec.shape.points[0][1] == rec.record[22])):
            logging.error("Shapefile and DBF-file coordinates for %s do not match:"
                          "%s vs %s",
                          rec.record[1], rec.shape.points, rec.record[21:23])
            continue
        # OGR is horribly non-functional, so this variable is created
        # just for the projection
        ogr_geom = ogr.CreateGeometryFromJson(rec.record[20])
        ogr_geom.Transform(ETRS89_WGS84_TRANSFORM)

        yield ES.index_op({'location': {'lat': ogr_geom.GetY(),
                                        'lon': ogr_geom.GetX()},
                           'type_fi': rec.record[2],
                           'type_se': rec.record[3],
                           'type_en': rec.record[4],
                           'name_fi': rec.record[5],
                           'name_se': rec.record[6]})


@click.command()
@click.argument('shapefilename', type=click.Path())
def main(shapefilename):
    prepare_es(((DOCTYPE,
                 {"properties": {
                     "location": {
                         "type": "geo_point"}}}), ))

    for chunk in pyelasticsearch.bulk_chunks(
            documents(shapefilename), docs_per_chunk=500):
        ES.bulk(chunk, doc_type=DOCTYPE, index=INDEX)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
