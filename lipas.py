#!/usr/bin/env python3
import logging
import sys

from osgeo import ogr, osr
import pyelasticsearch
import shapefile


INDEX = 'reittiopas'
DOCTYPE = 'lipas'

source = osr.SpatialReference()
source.ImportFromEPSG(3067)  # ETRS89 / ETRS-TM35FIN
target = osr.SpatialReference()
target.ImportFromEPSG(4326)  # WGS84
transform = osr.CoordinateTransformation(source, target)

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def documents():
    for rec in shapefile.Reader(sys.argv[1], encoding='latin-1').iterShapeRecords():
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
        ogr_geom.Transform(transform)

        yield es.index_op({'location': {'lat': ogr_geom.GetY(),
                                        'lon': ogr_geom.GetX()},
                           'type_fi': rec.record[2],
                           'type_se': rec.record[3],
                           'type_en': rec.record[4],
                           'name_fi': rec.record[5],
                           'name_se': rec.record[6]})


try:
    es.create_index(index=INDEX)
except pyelasticsearch.exceptions.IndexAlreadyExistsError:
    pass
try:
    es.delete_all(index=INDEX, doc_type=DOCTYPE)
except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
    pass  # Doesn't matter if we didn't actually delete anything


es.put_mapping(index=INDEX, doc_type=DOCTYPE,
               mapping={
                   "properties": {
                       "location": {
                           "type": "geo_point"}}})

for chunk in pyelasticsearch.bulk_chunks(documents(), docs_per_chunk=500):
    es.bulk(chunk, doc_type=DOCTYPE, index=INDEX)
