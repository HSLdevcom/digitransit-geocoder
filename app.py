#!/usr/bin/env python3
import logging

from jinja2 import Template
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
        http = AsyncHTTPClient()
        template = Template(self.template_string)
        http.fetch(ES_URL + self.url,
                   allow_nonstandard_methods=True,
                   body=template.render(kwargs),
                   callback=self.on_response)

    def on_response(self, response):
        if response.error:
            logging.error(response)
            logging.error(response.body.decode())
            logging.error(response.request.body.decode())
            raise HTTPError(500)
        self.write(response.body)
        self.finish()


class ReverseHandler(Handler):
    def initialize(self):
        pass

    @asynchronous
    def get(self, **kwargs):
        zoom = int(self.get_argument('zoom', default="8"))
        http = AsyncHTTPClient()
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

        http.fetch(ES_URL + url,
                   allow_nonstandard_methods=True,
                   body=template.render(kwargs),
                   callback=self.on_response)


def make_app():
    return Application(
        [url(r"/suggest/(?P<search_term>.*)",
             Handler,
             {'template_string':
              '{"search_type" : "count"}\n'
              '{"query": {'
              '"wildcard": {'
              '"raw": "*{{ search_term.lower() }}*"}},'
              '"aggs": {'
              '"streets": { "terms": { "field": "katunimi" }}}}\n'
              '{}\n'
              '{"query": {'
              '"wildcard": {'
              '"stop_name": "*{{ search_term.lower() }}*"}}}\n'
              '{}\n'
              '{"query": {'
              '"wildcard": {'
              '"stop_desc": "*{{ search_term.lower() }}*"}}}\n'
              '{"search_type" : "count"}\n'
              '{"query": {'
              '"fuzzy": {'
              '"raw": "{{ search_term.lower() }}"}},'
              '"aggs": {'
              '"streets": { "terms": { "field": "katunimi" }}}}\n'
              '\n',  # ES requires a blank line at the end (not documented)
              'url': "_msearch?pretty&size=5"}),
         url(r"/search/(?P<streetname>.*)/(?P<streetnumber>.*)",
             Handler,
             {'template_string': '''{
                 "query": { "filtered": {
                     "filter": {
                         "bool" : {
                             "must" : [
                                 {"term": { "katunimi": "{{ streetname }}"}},
                                 {"term": { "osoitenumero": {{ streetnumber }} }}
                             ]}
              }}}}''',
              'url': "address/_search?pretty&size=5"}),
         url(r"/reverse/(?P<lat>.*),(?P<lon>.*)",
             ReverseHandler)],
        debug=True)


def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()


if __name__ == '__main__':
        main()
