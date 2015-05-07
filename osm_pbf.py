#!/usr/bin/env python3

"""
Read POIs from OSM pbf file and insert into Elasticsearch.
"""

import logging

import click
from imposm.parser import OSMParser
import rtree
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point

import mml_municipalities
from utils import ES, send_bulk, prepare_es

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.addHandler(logging.StreamHandler())

POI_DOCTYPE = 'poi'
ADDRESS_DOCTYPE = 'osm_address'

# Right now numbers greater than 256 have no effect, because imposm parser does not return bigger batches
BULK_SIZE = 256

all_nodes = {}
all_relations = {}
all_ways = {}
all_addresses = {}
all_coords = {}


def coords_callback(new_coords):
    # XXX We could optimize memory use by doing two passes:
    #     first record all the coord ids we need to calculate way centroids
    #     and second to find those coords
    for osm_id, lon, lat in new_coords:
        all_coords[osm_id] = (lon, lat)


def nodes_callback(new_nodes):
    operations = []
    for osm_id, tags, lonlat in new_nodes:
        if 'animal_spotting' in tags:
            continue

        # Save those nodes which contain address data
        for t in tags:
            if t.startswith('addr:'):
                all_nodes[osm_id] = [tags, lonlat, None]

        # But if the POI is not interesting, skip it
        if ('created_by' in tags and len(tags) <= 3) or len(tags) <= 2:
            # These nodes are linked from somewhere else,
            # but we don't handle relations.
            # Vast majority of nodes are simply part of ways.
            continue

        tags['location'] = lonlat
        operations.append(ES.index_op(tags))
        if len(operations) >= BULK_SIZE:
            send_bulk(operations, POI_DOCTYPE)
            operations = []
    if operations:  # Send the rest of the nodes too
        send_bulk(operations, POI_DOCTYPE)


def relations_callback(new_relations):
    for osm_id, tags, targets in new_relations:
        if 'type' not in tags or \
           (tags['type'] != 'street' and tags['type'] != 'associatedStreet'):
            # This relation won't give useful address data.
            # It might be a bus route, multipolygon etc.
            continue
        if 'name' not in tags:
            logging.warning("No name in %s relation %s", tags['type'], osm_id)
            continue
        all_relations[osm_id] = (tags, targets)


def ways_callback(new_ways):
    for osm_id, tags, nodes in new_ways:
        for t in tags:
            if t.startswith('addr:'):
                all_ways[osm_id] = [tags, nodes, None]
                break


def add_address(street, number, location, unit=None, main_entrance=False,
                municipality=None, index=None, municipality_list=None):
    if not municipality and index and municipality_list:
        p = Point(*location)
        for name, geom in [municipality_list[i] for i in index.intersection(
                (location[0], location[1], location[0], location[1]))]:
            if geom.contains(p):
                municipality = name
                break
    address = (municipality, street, number, unit)
    if address in all_addresses:
        if main_entrance:
            logging.info("Overriding existing address %s with main entrance %s at %s",
                         all_addresses[address], address, location)
            all_addresses[address] = location
        else:
            logging.info("Trying to add two identical addresses"
                         "(probably multiple entrances for same staircase),"
                         "using only the first one: "
                         "%s in locations %s and %s",
                         all_addresses[address], address, location)
        return
    all_addresses[address] = location


def get_unit(tags):
    main_found = False
    if 'entrance' in tags and tags['entrance'] == 'main':
        main_found = True
    if 'addr:unit' in tags and 'addr:staircase' in tags:
        logger.warning("Found both unit and staircase for node")
    if 'addr:unit' in tags:
        return tags['addr:unit'], main_found
    elif 'addr:staircase' in tags:
        return tags['addr:staircase'], main_found
    return None, False


@click.command()
@click.argument('pbffilename', type=click.Path(exists=True))
@click.argument('municipalityfilename', type=click.Path(exists=True))
def main(pbffilename, municipalityfilename):
    mapping = {"date_detection": False,
               "properties": {
                   "location": {
                       "type": "geo_point"},
                   "street": {
                       "type": "string",
                       "analyzer": "keyword",
                       "fields": {
                           "raw": {
                               "type": "string",
                               "analyzer": "myAnalyzer"}}}}}
    prepare_es(((POI_DOCTYPE, mapping),
                (ADDRESS_DOCTYPE, mapping)))

    # Storing the polygons separately is hugely more efficient compared to
    # storing in index due to the pickling rtree does
    municipalities = []
    p = rtree.index.Property()
    # XXX 10/10/3 is better than the default 100/100/32, but perhaps not the best
    p.index_capacity = 10
    p.leaf_capacity = 10
    p. near_minimum_overlap_factor = 3
    idx = rtree.index.Index(properties=p)
    for i, m in enumerate(mml_municipalities.parse(municipalityfilename)):
        polygon = Polygon(m['boundaries']['coordinates'][0][0])
        municipalities.append((m['nimi'], polygon))
        idx.insert(i, polygon.bounds)

    OSMParser(concurrency=4,
              coords_callback=coords_callback,
              nodes_callback=nodes_callback,
              relations_callback=relations_callback,
              ways_callback=ways_callback
              ).parse(pbffilename)

    for r_id, r in all_relations.items():
        # If we directly add addresses here, we would need to pop the nodes/ways
        # out of the lists so we don't add them again later in case they also
        # have all the data in their tags.
        # Instead we just add data to them.
        for id, type, description in r[1]:
            if type == 'way' and id in all_ways:
                if all_ways[id][2] is not None:
                    logging.warning("Way %s has more than one associated street", id)
                    continue
                all_ways[id][2] = r_id
            elif type == 'node' and id in all_nodes:
                if all_nodes[id][2] is not None:
                    logging.warning("Node %s has more than one associated street", id)
                    continue
                all_nodes[id][2] = r_id

    for n in all_nodes.values():
        if 'addr:housenumber' in n[0]:
            if 'addr:street' in n[0]:
                street = n[0]['addr:street']
            elif n[2]:
                street = all_relations[n[2]][0]['name']
            else:
                continue
            add_address(street, n[0]['addr:housenumber'], n[1], *get_unit(n[0]),
                        index=idx, municipality_list=municipalities)

    for id, w in all_ways.items():
        if 'addr:housenumber' in w[0]:
            if 'addr:street' in w[0]:
                street = w[0]['addr:street']
            elif w[2]:
                street = all_relations[w[2]][0]['name']
            else:
                # XXX Some ways clearly have address data such as addr:housenumber,
                #     but don't have any related ways and their nodes have no data.
                #     When rendered, the human reader can interpret by looking at
                #     nearby features, so reverse geocoding is a possibility.
                logger.info('Way with addressdata but no street: %s', id)
                continue

            # Building address known, look for entrances
            found = False
            for node_id in w[1]:
                if node_id in all_nodes:
                    n = all_nodes[node_id]
                    unit, main_found = get_unit(n[0])
                    if not unit:
                        continue
                    add_address(street,
                                w[0]['addr:housenumber'],
                                n[1], unit, main_found,
                                index=idx, municipality_list=municipalities)
                    found = True  # Don't just break, there might be multiple entrances

            # No entrances, so find the coordinates for OSM ids in the way
            # and use them to calculate the geometric center of the way
            if not found:
                logger.info("Didn't find an entrance for a way %s", id)
                if len(w[1]) < 3:
                    logger.warning("Way %s didn't have at least three coordinates", id)
                    continue
                # unit tag doesn't belong in buildings, so we don't search for it.
                # In all of Finland only one address in Kirkkonummi seems to have it.
                add_address(street,
                            w[0]['addr:housenumber'],
                            [x.tolist()[0] for x in
                             Polygon([all_coords[y] for y in w[1]]).centroid.xy],
                            index=idx, municipality_list=municipalities)
        elif 'addr:street' in w[0]:
            # Only street name found, look for house nodes on this street
            found = False
            for node_id in w[1]:
                if node_id in all_nodes and 'addr:housenumber' in all_nodes[node_id][0]:
                    found = True
                    add_address(w[0]['addr:street'],
                                all_nodes[node_id][0]['addr:housenumber'],
                                all_nodes[node_id][1],
                                index=idx, municipality_list=municipalities)
            if not found:
                logger.info("Didn't find a housenumber for way %s", id)
        else:
            logger.info('Way with addressdata but no street or housenumber: %s', id)

    operations = []
    for (municipality, street, number, unit), lonlat in all_addresses.items():
        operations.append(ES.index_op({'municipality': municipality,
                                       'street': street,
                                       'number': number,
                                       'unit': unit,
                                       'location': lonlat}))
        if len(operations) >= BULK_SIZE:
            send_bulk(operations, ADDRESS_DOCTYPE)
            operations = []
    if operations:  # Send the rest of the nodes too
        send_bulk(operations, ADDRESS_DOCTYPE)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
