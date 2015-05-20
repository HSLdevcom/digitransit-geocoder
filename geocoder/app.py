#!/usr/bin/env python3
# pylint: disable=abstract-method,arguments-differ
import json
import logging

import click
from jinja2 import Template
from shapely.geometry import LineString
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, URLSpec, asynchronous, HTTPError, StaticFileHandler


DATE = None
ES_URL = "http://localhost:9200/reittiopas/"


def finish_request(handler):
    '''Set CORS and content type headers and call finish on given handler.'''
    if 'Origin' in handler.request.headers:
        handler.set_header('Access-Control-Allow-Origin', handler.request.headers['Origin'])
    else:
        handler.set_header('Access-Control-Allow-Origin', '*')
    handler.set_header('Content-Type', 'application/json; charset="utf-8"')
    handler.finish()


class MetaHandler(RequestHandler):
    '''RequestHandler for the meta endpoint.'''
    def get(self):
        '''
        When the geocoder data was refreshed as ISO 8601 date, for example::

            {"updated": "2015-01-01"}

        If the last update is not known::

            {"updated": null}
        '''
        self.write({'updated': DATE})
        finish_request(self)


class Handler(RequestHandler):
    '''Superclass for other endpoints.'''
    @asynchronous
    def get(self, template_string, url, **kwargs):
        body = Template(template_string).render(kwargs)
        logging.debug("Sending query: %s", body)
        AsyncHTTPClient().fetch(ES_URL + url,
                                allow_nonstandard_methods=True,
                                body=body,
                                callback=self.on_response)

    def on_response(self, response):
        logging.debug("Got response: %s", response)
        '''Callback for handling replies from ElasticSearch.'''
        if response.error:
            logging.error(response)
            if response.body:
                logging.error(response.body.decode())
            logging.error(response.request.body.decode())
            raise HTTPError(500)
        self.write(self.transform_es(json.loads(response.body.decode('utf-8'))))
        finish_request(self)

    def transform_es(self, data):
        """
        Transform input data from ES as dict into output format dict.
        """
        return data


class AddressSearchHandler(Handler):
    '''RequestHandler for the getting the location of one address.'''

    def get(self, **kwargs):
        '''
        Get an address location as hijack protected JSON array
        in an object under the name "results".

        The response format is identical to the street search endpoint,
        except that the array contains only one element.
        '''
        super().get(
            # noqa
            url="_msearch",
            template_string='{"type": "address"}\n'
                            '{"query": {'
                               '"filtered": {'
                                 '"filter": {'
                                   '"bool" : {'
                                     '"must" : ['
                                       '{or: ['
                                         '{"term": {"kaupunki": "{{ city.lower() }}"}},'
                                         '{"term": {"staden": "{{ city.lower() }}"}}'
                                       ']},'
                                       '{or: ['
                                         '{"term": {"katunimi.raw": "{{ streetname.lower() }}"}},'
                                         '{"term": {"gatan.raw": "{{ streetname.lower() }}"}}'
                                       ']},'
                                       '{"range": {'
                                         '"osoitenumero": {"lte": {{ streetnumber }} }}},'
                                       '{"range": {'
                                         '"osoitenumero2": {"gte" : {{ streetnumber }} }}}'
                            ']}}}}}\n'
                            '{"type": "osm_address"}\n'
                            '{"query": {'
                               '"filtered": {'
                                 '"filter": {'
                                   '"bool" : {'
                                     '"must" : ['
                                       '{"term": { "municipality": "{{ city.lower() }}"}},'
                                       '{"term": { "street": "{{ streetname.title() }}"}},'  # XXX Title case doesn't work for "Ida Aalbergin tie"
                                       '{"term": { "number": {{ streetnumber }} }}'
                            ']}}}}}\n'
                            '\n',
            **kwargs)

    def transform_es(self, data):
        addresses = {}
        for addr in [x['_source'] for x in data['responses'][1]["hits"]["hits"]]:
            addresses[(addr['municipality'], addr['street'], addr['number'])] = {
                'municipality': addr['municipality'],
                'street': addr['street'],
                'number': addr['number'],
                'unit': addr['unit'],
                'location': addr['location'],
                'source': 'OSM'
            }
        for addr in [x['_source'] for x in data['responses'][0]["hits"]["hits"]]:
            if addr['osoitenumero'] == addr['osoitenumero2']:
                number = str(addr['osoitenumero'])
            else:
                number = str(addr['osoitenumero']) + '-' + str(addr['osoitenumero2'])
            id = (addr['kaupunki'], addr['katunimi'], number)
            if id not in addresses:
                addresses[id] = {
                    'municipalityFi': addr['kaupunki'],
                    'municipalitySv': addr['staden'],
                    'streetFi': addr['katunimi'],
                    'streetSv': addr['gatan'],
                    'number': number,
                    'unit': None,
                    'location': addr['location'],
                    'source': 'HRI.fi'
                }
            else:
                logging.info('Returning OSM address instead of official: %s', id)

        if not addresses:
            raise HTTPError(404)
        return {'results': list(addresses.values())}


class StreetSearchHandler(AddressSearchHandler):
    '''RequestHandler for getting all the house numbers on a street.'''

    def get(self, **kwargs):
        '''
        Get all address locations on a street as a hijack protected JSON array
        in an object under the name "results".
        All non ASCII chars are unicode escaped.

        :>jsonarr string municipalityFi: Municipality name in Finnish
        :>jsonarr string streetFi: Streetname in Finnish
        :>jsonarr string municipalitySv: Municipality in Swedish
        :>jsonarr string streetSv: Streetname in Swedish
        :>jsonarr string number: Either a single integer as string, or "<integer>-<integer>" indicating a range of housenumbers in same address.
        :>jsonarr string unit: Possible letter separating multiple addresses which share the same number
        :>jsonarr latlon_array location: Array of two floats, latitude and longitude in WGS84
        :>jsonarr string source: Either "HRI.fi" for data from Helsinki Region Infoshare, or "OSM" for OpenStreetMap
        :responseheader Content-Type: application/json; charset="utf-8"
        :responseheader Access-Control-Allow-Origin: Same as request Origin header if supplied, * otherwise

        Example response::

            {"results" : [
                {
                    "municipalityFi" : "Helsinki",
                    "streetFi" : "Ida Aalbergin tie",
                    "municipalitySv" : "Helsingfors",
                    "streetSv" : "Ida Aalbergs v\u00e4",
                    "unit" : null,
                    "number" : "1",
                    "location" : [24.9003449796381, 60.2306259161157],
                    "source" : "HRI.fi"
                },
                {
                    "municipalityFi" : "Helsinki",
                    "streetFi" : "Ida Aalbergin tie",
                    "municipalitySv" : "Helsingfors",
                    "streetSv" : "Ida Aalbergs v\u00e4",
                    "unit" : null,
                    "number" : "2",
                    "location" : [24.9015735934518, 60.2301511387295],
                    "source" : "HRI.fi"
                },
                ...
            ]}
        '''
        super(AddressSearchHandler, self).get(
            # noqa
            url="_msearch",
            template_string='{"type": "address"}\n'
                            '{"size": 2000,'
                             '"query": {'
                               '"filtered": {'
                                 '"filter": {'
                                   '"bool" : {'
                                     '"must" : ['
                                       '{or: ['
                                         '{"term": {"kaupunki": "{{ city.lower() }}"}},'
                                         '{"term": {"staden": "{{ city.lower() }}"}}'
                                       ']},'
                                       '{or: ['
                                         '{"term": {"katunimi.raw": "{{ streetname.lower() }}"}},'
                                         '{"term": {"gatan.raw": "{{ streetname.lower() }}"}}'
                                       ']}'
                            ']}}}}}\n'
                            '{"type": "osm_address"}\n'
                            '{"size": 2000,'
                             '"query": {'
                               '"filtered": {'
                                 '"filter": {'
                                   '"bool" : {'
                                     '"must" : ['
                                       '{"term": { "municipality": "{{ city.lower() }}"}},'
                                       '{"term": { "street": "{{ streetname.title() }}"}}'  # XXX Title case doesn't work for "Ida Aalbergin tie"
                            ']}}}}}\n'
                            '\n',
            **kwargs)


class SuggestHandler(Handler):
    """RequestHandler for autocomplete/typo fix suggestions."""

    def get(self, **kwargs):
        '''
        :query string city: Limit the results to within given city. Can be given multiple times to limit within all the cities.

        Example response::

            {"streetnames_fi" : [
                {"Mannerheiminaukio" : [
                    {"key" : "helsinki",
                     "doc_count" : 2
                }]},
                {"Mannerheimintie" : [
                    {"key" : "helsinki",
                     "doc_count" : 154
             }]}],
             "streetnames_sv" : [
                 {"Mannerheimplatsen" : [
                     {"key" : "helsingfors",
                      "doc_count" : 2
                 }]},
                 {"Mannerheimv<E4>gen" : [
                     {"doc_count" : 154,
                      "key" : "helsingfors"
             }]}],
             "fuzzy_streetnames" : [],
             "stops" : [
                {"stop_name" : "Hesperian puisto",
                 "stop_desc" : "Mannerheimintie",
                 "stop_code" : "1909",
                 "location" : [24.9294653999999, 60.1783596999998],
                 "zone_id" : "1",
                 "stop_url" : "http://aikataulut.hsl.fi/pysakit/fi/1130206.html",
                 "location_type" : "0",
                 "parent_station" : " ",
                 "wheelchair_boarding" : "0",
                 "stop_id" : "1130206"
                },
                ...
            ]}

        If a streetname is found in multiple cities,
        the response includes all of them::

            "streetnames_fi" : [
                {"Tietotie" : [
                    {"doc_count" : 10,
                     "key" : "vantaa"
                    },
                    {"key" : "espoo",
                     "doc_count" : 6
            }]}],
        '''
        kwargs['cities'] = self.get_arguments('city')
        super().get(
            # noqa
            # _msearch allows multiple queries at the same time,
            # but is very finicky about the format.
            url="_msearch",
             # All queries are case insensitive
            template_string=''
             # Find street names by matching correctly written part from middle
             '{"search_type" : "count", "type": "address"}\n'
             '{"query": {'
                '"filtered": {'
                 '"query": {'
                   '"wildcard": {'
                     '"katunimi.raw": "*{{ search_term.lower() }}*"}'
                  '}'
                  '{% if cities %}'
                  ',"filter": {'
                    'or: ['
                    '{% for city in cities %}'
                      '{"term": {"kaupunki": "{{ city.lower() }}"}},'
                      '{"term": {"staden": "{{ city.lower() }}"}}'
                      '{% if not loop.last %},{% endif %}'
                    '{% endfor %}'
                    ']}'
                '{% endif %}'
              '}},'
              '"aggs": {'
                '"streets": {'
                  '"terms": { "field": "katunimi", "size": 20 },'
                  '"aggs": {'
                    '"cities": {'
                      '"terms": { "field": "kaupunki", "size": 20 }}}}}}\n'
             '{"search_type" : "count", "type": "address"}\n'
             '{"query": {'
                '"filtered": {'
                 '"query": {'
                   '"wildcard": {'
                     '"gatan.raw": "*{{ search_term.lower() }}*"}'
                  '}'
                  '{% if cities %}'
                  ',"filter": {'
                    'or: ['
                    '{% for city in cities %}'
                      '{"term": {"kaupunki": "{{ city.lower() }}"}},'
                      '{"term": {"staden": "{{ city.lower() }}"}}'
                      '{% if not loop.last %},{% endif %}'
                    '{% endfor %}'
                    ']}'
                '{% endif %}'
              '}},'
              '"aggs": {'
                '"streets": {'
                  '"terms": { "field": "gatan", "size": 20 },'
                  '"aggs": {'
                    '"cities": {'
                      '"terms": { "field": "staden", "size": 20 }}}}}}\n'
             # Find correctly written stops from names
             '{"type": "stop"}\n'
             '{"size": 20,'
              '"query": {'
                '"wildcard": {'
                  '"stop_name": "*{{ search_term.lower() }}*"}}}\n'
             # Find correctly written stops from descriptions
             # (often crossing street name, or closest address)
             '{"type": "stop"}\n'
             '{"size": 20,'
              '"query": {'
                '"wildcard": {'
                  '"stop_desc": "*{{ search_term.lower() }}*"}}}\n'
             # Find correctly written stop codes
             '{"type": "stop"}\n'
             '{"size": 20,'
              '"query": {'
                '"wildcard": {'
                  '"stop_code": "*{{ search_term.lower() }}*"}}}\n'
             # Find incorrectly written street names with maximum Levenstein
             # distance of 2 (hardcoded into Elasticsearch)
             # XXX Would be nice if we could do a fuzzy wildcard search...
             # http://www.elastic.co/guide/en/elasticsearch/reference/master/search-suggesters-completion.html
             # allows at least fuzzy prefix suggestions
             '{"search_type" : "count", "type": "address"}\n'
             '{"query": {'
                '"fuzzy": {'
                  '"raw": "{{ search_term.lower() }}"}},'
              '"aggs": {'
                '"streets": {"terms": {"field": "katunimi", "size": 20 }}}}\n'
             '\n',  # ES requires a blank line at the end (not documented)
             **kwargs)

    def transform_es(self, data):
        r = data['responses']
        streetnames_fi = []
        for s in r[0]["aggregations"]["streets"]["buckets"]:
            streetnames_fi.append({s["key"]: s["cities"]["buckets"]})
        streetnames_sv = []
        for s in r[1]["aggregations"]["streets"]["buckets"]:
            streetnames_sv.append({s["key"]: s["cities"]["buckets"]})
        stops = {}
        for s in r[2]["hits"]["hits"] + r[3]["hits"]["hits"] + r[4]["hits"]["hits"]:
            if s["_id"] not in stops:
                stops[s["_id"]] = s["_source"]
        return {
            # Address is a single key/value dict, where the streetname is the key.
            # In Python3 it's a bit tricky to get that key:
            # dict_keys -> iterator -> value
            'streetnames_fi': sorted(streetnames_fi,
                                     key=lambda x: x.keys().__iter__().__next__()),
            'streetnames_sv': sorted(streetnames_sv,
                                     key=lambda x: x.keys().__iter__().__next__()),
            'stops': sorted(list(stops.values()),
                            key=lambda x: x['stop_name'] + x['stop_desc']),
            'fuzzy_streetnames': r[5]["aggregations"]["streets"]["buckets"],
        }


class ReverseHandler(Handler):

    def initialize(self):
        pass

    @asynchronous
    def get(self, **kwargs):
        """
        Reverse geocoding request -- get the nearest city or address for given coordinates.

        :query city: If given, return the city at given coordinates. If not, return nearest address. Useful for zoomed out views.
        :>jsonarr string kaupunki: Municipality name in Finnish
        :>jsonarr string katunimi: Streetname in Finnish
        :>jsonarr string staden: Municipality in Swedish
        :>jsonarr string gatan: Streetname in Swedish
        :>jsonarr int osoitenumero: Housenumber
        :>jsonarr int osoitenumero2: If the address is a range of housenumbers, this field contains the end of the range
        :>jsonarr string kiinteiston_jakokirjain: Possible letter separating multiple addresses which share the same number
        :>jsonarr latlon_array location: Array of two floats, latitude and longitude in WGS84
        :responseheader Content-Type: application/json; charset="utf-8"
        :responseheader Access-Control-Allow-Origin: Same as request Origin header if supplied, * otherwise
        :status 404: if given latitude and longitude are malformed or coordinates weren't inside any city boundaries in a city request
        :status 200: in all other cases since every valid coordinate will have a nearest address



        Example response::

            {"kaupunki" : "Espoo",
             "katunimi" : "Kattilantie",
             "staden" : "Esbo",
             "gatan" : "Kattilavägen",
             "osoitenumero" : 45,
             "osoitenumero2" : 45,
             "kiinteiston_jakokirjain" : "",
             "location" : [24.5038823316986, 60.3216807160152]}
        """
        if 'city' not in self.request.arguments:
            url = "address/_search?pretty&size=1"
            template = Template('''{
                 "sort" : [{"_geo_distance" : {
                                "location": {
                                    "lat":  {{ lat }},
                                    "lon": {{ lon }}
                                },
                                "order" : "asc",
                                "unit" : "km",
                                "mode" : "min",
                                "distance_type" : "plane"
                                } }] }''')
        else:
            # When the user hasn't zoomed in, there's no hope in pinpointing
            # addresses accurately. So instead, we return municipalities.
            url = "municipality/_search?pretty&size=1"
            # Addresses have geo_points, but municipalities geo_shapes.
            # The shapes cannot be used in distance queries or sorting,
            # so we check whether a point shape intersects (ES default,
            # but here explicitly) with the municipality boundaries.
            template = Template('''{
            "query": {
                "filtered": {
                  "filter": {
                    "geo_shape": {
                      "boundaries": {
                        "relation": "intersects",
                        "shape": {
                          "coordinates": [
                            {{ lon }},
                            {{ lat }}
                          ],
                          "type": "point"
                        }
                      }
                    }
                  }
                }
            }}''')

        AsyncHTTPClient().fetch(ES_URL + url,
                                allow_nonstandard_methods=True,
                                body=template.render(kwargs),
                                callback=self.on_response)

    def transform_es(self, data):
        if not data['hits']['hits']:
            raise HTTPError(404)
        return data["hits"]["hits"][0]["_source"]


class InterpolateHandler(Handler):
    '''RequestHandler for coordinates interpolated from NLS data.'''
    def initialize(self):
        pass

    @asynchronous
    def get(self, streetname, streetnumber):
        """
        Request a location of address only known by interpolation.

        National Land Survey offers road data with a range of addresses given for
        road segments. Interpolation is done linearly along the road curve.

        .. warning:

            It's not guaranteed that the given address actually exists!
            For example a road segment might only have streetnumbers 200 and 300,
            but this endpoint will return an interpolation for every number between
            those.

        :resjson latlon_array coordinates: Two floats, latitude and longitude
        :responseheader Content-Type: application/json; charset="utf-8"
        :responseheader Access-Control-Allow-Origin: Same as request Origin header if supplied, * otherwise
        :status 404: if given address is not found in any road segment
        :status 200: in all other cases



        Example response::

            {"coordinates": [[24.9397464196591, 60.16920195781718]]}

        """
        self.streetnumber = int(streetnumber)
        if self.streetnumber % 2 == 0:
            self.side = "vasen"
        else:
            self.side = "oikea"
        url = "interpolated_address/_search?pretty&size=20"
        template = Template('''{
                 "query": { "filtered": {
                     "filter": {
                         "bool" : {
                             "must" : [
                                 {"term": {"nimi": "{{ streetname.lower() }}"}},
                                 {"range":
                                    {"min_{{ side }}": {"lte" : {{ streetnumber }} }}},
                                 {"range":
                                    {"max_{{ side }}": {"gte" : {{ streetnumber }} }}}]}

              }}}}''')
        AsyncHTTPClient().fetch(ES_URL + url,
                                allow_nonstandard_methods=True,
                                body=template.render({'streetname': streetname,
                                                      'streetnumber': streetnumber,
                                                      'side': self.side}),
                                callback=self.on_response)

    def transform_es(self, data):
        if data["hits"]["hits"]:
            street = data["hits"]["hits"][0]["_source"]
            if street["max_" + self.side][0] == street["min_" + self.side][0]:
                fraction = 0.5
            else:
                fraction = (self.streetnumber - int(street["min_" + self.side][0])) / \
                           (int(street["max_" + self.side][0]) - int(street["min_" + self.side][0]))
            return {'coordinates':
                    list(LineString(street['location']['coordinates'])
                         .interpolate(fraction, normalized=True).coords)[0]}
        raise HTTPError(404)


def make_app(settings={}, path='../docs/_build/html/'):
    return Application(
        [URLSpec(r"/suggest/(?P<search_term>[\w\-%]*)",
                 SuggestHandler),
         # The URL regexps are searched in order, so more specific URLs must come first
         URLSpec(r"/address/(?P<city>[\w\-%]*)/(?P<streetname>[\w\-%]*)/(?P<streetnumber>[\w\-%]*)",
                 AddressSearchHandler),
         URLSpec(r"/street/(?P<city>[\w\-%]*)/(?P<streetname>[\w\-%]*)",
                 StreetSearchHandler),
         URLSpec(r"/interpolate/(?P<streetname>[\w\-%]*)/(?P<streetnumber>[\w\-%]*)",
                 InterpolateHandler),
         URLSpec(r"/reverse/(?P<lat>\d+\.\d+),(?P<lon>\d+\.\d+)",
                 ReverseHandler),
         URLSpec(r"/meta",
                 MetaHandler),
         URLSpec(r"/(.*)",
                 StaticFileHandler,
                 {"path": path,
                  "default_filename": "index.html"})],
        **settings)


app = make_app()


@click.command()
@click.option('--docs', help="The directory containing API docs",
              default='../docs/_build/html/', show_default=True)
@click.option("-p", '--port', help="TCP port to serve the API from",
              default=8888, show_default=True)
@click.option("-v", "--verbose", count=True, help="Use once for info, twice for more")
@click.option("-d", "--date", help="The metadata updated date")
def main(docs, port=8888, verbose=0, date=None):
    global DATE, app
    settings = {}
    if verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif verbose == 2:
        logging.basicConfig(level=logging.DEBUG)
        settings = {'debug': True}
    app = make_app(settings, path=docs)

    DATE = date
    app.listen(port)
    IOLoop.current().start()


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
