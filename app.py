#!/usr/bin/env python3
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
            raise HTTPError(500)
        self.write(response.body)
        self.finish()


def make_app():
    return Application(
        [url(r"/search/(?P<streetname>.*)", Handler,
             {'template_string': '''{
                 "query": {
                     "prefix": {
                         "katunimi": "{{ streetname }}"
                     }
                 },
                 "aggs" : {
                     "streets" : { "terms" : { "field" : "katunimi" } } } }''',
              'url': "address/_search?pretty&size=5&search_type=count"}),
         url(r"/reverse/(?P<lat>.*),(?P<lon>.*)", Handler,
             {'template_string': '''{
                 "sort" : [{"_geo_distance" : {
                                "location": {
                                    "lat":  {{ lat }},
                                    "lon": {{ lon }}
                                },
                                "order" : "asc",
                                "unit" : "km",
                                "mode" : "min",
                                "distance_type" : "sloppy_arc"
                                } }] }''',
              'url': "address/_search?pretty&size=1"})],
        debug=True)


def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()


if __name__ == '__main__':
        main()
