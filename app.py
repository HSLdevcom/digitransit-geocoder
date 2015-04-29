#!/usr/bin/env python3
import argparse
import json
import logging

from jinja2 import Template
from shapely.geometry import LineString
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, asynchronous, HTTPError

ES_URL = "http://localhost:9200/reittiopas/"


class Handler(RequestHandler):
    def initialize(self, template_string, url):
        self.template_string = template_string
        self.url = url

    @asynchronous
    def get(self, **kwargs):
        template = Template(self.template_string)
        AsyncHTTPClient().fetch(ES_URL + self.url,
                   allow_nonstandard_methods=True,
                   body=template.render(kwargs),
                   callback=self.on_response)

    def on_response(self, response):
        if response.error:
            logging.error(response)
            if response.body:
                logging.error(response.body.decode())
            logging.error(response.request.body.decode())
            raise HTTPError(500)
        self.write(self.transform_es(json.loads(response.body.decode('utf-8'))))
        if 'Origin' in self.request.headers:
            self.set_header('Access-Control-Allow-Origin', self.request.headers['Origin'])
        else:
            self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Content-Type', 'application/json; charset="utf-8"')
        self.finish()

    def transform_es(self, data):
        """
        Transform input data from ES as dict into output format dict.
        """
        return data


class StreetSearchHandler(Handler):
    def transform_es(self, data):
        addresses = {}
        for a in list(map(lambda x: x['_source'], data['responses'][1]["hits"]["hits"])):
            addresses[(a['municipality'], a['street'], a['number'])] = {
                'municipality': a['municipality'],
                'street': a['street'],
                'number': a['number'],
                'unit': a['unit'],
                'location': a['location'],
            }
        for a in list(map(lambda x: x['_source'], data['responses'][0]["hits"]["hits"])):
            id = (a['kaupunki'], a['katunimi'], str(a['osoitenumero']))
            if id not in addresses:
                addresses[id] = {
                    'municipality': a['kaupunki'],
                    'street': a['katunimi'],
                    'number': str(a['osoitenumero']),
                    'unit': None,
                    'location': a['location'],
                }
            else:
                logging.info('Returning OSM address instead of official: %s', id)

        return {'results': list(addresses.values())}


class SearchHandler(Handler):
    def transform_es(self, data):
        return {'results': list(map(lambda x: x['_source'], data["hits"]["hits"]))}


class SuggestHandler(Handler):
    """
    Handler for autocomplete/typo fix suggestions
    """

    def transform_es(self, data):
        r = data['responses']
        stops = {}
        for s in r[1]["hits"]["hits"] + r[2]["hits"]["hits"]:
            if s["_id"] not in stops:
                stops[s["_id"]] = s["_source"]
        streetnames = {}
        for s in r[0]["aggregations"]["streets"]["buckets"]:
            streetnames[s["key"]] = s["cities"]["buckets"]
        return {
            'streetnames': streetnames,
            'fuzzy_streetnames': r[3]["aggregations"]["streets"]["buckets"],
            'stops': list(stops.values())
        }


class ReverseHandler(Handler):
    """
    Handler for reverse geocoding requests (i.e. "What's the name for these coordinates")

    The answer depends on the zoom level. Zoomed out, the user cannot pinpoint addresses,
    so return just the city. Closer, search for nearest address.
    """

    def initialize(self):
        pass

    @asynchronous
    def get(self, **kwargs):
        zoom = int(self.get_argument('zoom', default="8"))
        if zoom >= 8:
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
        return data["hits"]["hits"][0]["_source"]


class InterpolateHandler(Handler):
    def initialize(self):
        pass

    @asynchronous
    def get(self, streetname, streetnumber):
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
                                 {"term": {"nimi": "{{ streetname }}"}},
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
        logging.debug(data)
        if data["hits"]["hits"]:
            street = data["hits"]["hits"][0]["_source"]
            if street["max_" + self.side][0] == street["min_" + self.side][0]:
                fraction = 0.5
            else:
                fraction = (self.streetnumber - int(street["min_" + self.side][0])) / \
                           (int(street["max_" + self.side][0]) - int(street["min_" + self.side][0]))
            return {'coordinates': list(LineString(street['location']['coordinates']).interpolate(fraction, normalized=True).coords)}
        return {}  # No results found




def make_app():
    return Application(
        [url(r"/suggest/(?P<search_term>.*)",
             SuggestHandler,
             # _msearch allows multiple queries at the same time, but is very
             # finicky about the format.
             {'url': "_msearch",
              # All queries are case insensitive
              'template_string':
              # Find street names by matching correctly written part from anywhere,
              # ordered by number of addresses
              '{"search_type" : "count"}\n'
              '{"query": {'
              '"wildcard": {'
              '"raw": "*{{ search_term.lower() }}*"}},'
              '"aggs": {'
              '"streets": { "terms": { "field": "katunimi", "size": 20 },'
                         '"aggs": {"cities":'
                                   '{"terms": { "field": "kaupunki", "size": 20 }}}}}}\n'
              '{}\n'
              # Find correctly written stops from names
              '{'
              '"size": 20,'
              '"query": {'
              '"wildcard": {'
              '"stop_name": "*{{ search_term.lower() }}*"}}}\n'
              '{}\n'
              # Find correctly written stops from descriptions
              # (often crossing street name, or closest address)
              '{'
              '"size": 20,'
              '"query": {'
              '"wildcard": {'
              '"stop_desc": "*{{ search_term.lower() }}*"}}}\n'
              '{"search_type" : "count"}\n'
              # Find incorrectly written street names with maximum Levenstein
              # distance of 2 (hardcoded into Elasticsearch)
              # XXX Would be nice if we could do a fuzzy wildcard search...
              # http://www.elastic.co/guide/en/elasticsearch/reference/master/search-suggesters-completion.html
              # allows at least fuzzy prefix suggestions
              '{"query": {'
              '"fuzzy": {'
              '"raw": "{{ search_term.lower() }}"}},'
              '"aggs": {'
              '"streets": { "terms": { "field": "katunimi", "size": 20 }}}}\n'
              '\n',  # ES requires a blank line at the end (not documented)
              }),
         # The URL regexps are searched in order, so more specific URLs must come first
         url(r"/search/(?P<city>.*)/(?P<streetname>.*)/(?P<streetnumber>.*)",
             StreetSearchHandler,
             {'url': "_msearch",
              'template_string':
              '{}\n'
              '{"query": { "filtered": {'
                     '"filter": {'
                         '"bool" : {'
                             '"must" : ['
                                 '{"term": { "kaupunki": "{{ city.lower() }}"}},'
                                 '{"term": { "katunimi": "{{ streetname.title() }}"}},'
                                 '{"term": { "osoitenumero": {{ streetnumber }} }}'
                             ']}'
              '}}}}\n'
              '{}\n'
              '{"query": { "filtered": {'
                     '"filter": {'
                         '"bool" : {'
                             '"must" : ['
                                 '{"term": { "municipality": "{{ city.lower() }}"}},'
                                 '{"term": { "street": "{{ streetname.title() }}"}},'
                                 '{"term": { "number": {{ streetnumber }} }}'
                             ']}'
              '}}}}\n'
              '\n'
              }),
         url(r"/search/(?P<city>.*)/(?P<streetname>.*)",
             SearchHandler,
             {'url': "address/_search?pretty&size=2000",
              'template_string': '''{
                 "query": { "filtered": {
                     "filter": {
                         "bool" : {
                             "must" : [
                                 {"term": { "kaupunki": "{{ city.lower() }}"}},
                                 {"term": { "katunimi": "{{ streetname.title() }}"}}
                             ]}
              }}}}'''
              }),
         url(r"/interpolate/(?P<streetname>.*)/(?P<streetnumber>.*)",
             InterpolateHandler),
         url(r"/reverse/(?P<lat>.*),(?P<lon>.*)",
             ReverseHandler)],
        debug=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int,
                        help="TCP port to serve the API from", default=8888)
    parser.add_argument("-v", "--verbose", action='count',
                        help="Use once for info, twice for more")
    args = parser.parse_args()
    if args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif args.verbose == 2:
        logging.basicConfig(level=logging.DEBUG)

    app = make_app()
    app.listen(args.port)
    IOLoop.current().start()


if __name__ == '__main__':
        main()
