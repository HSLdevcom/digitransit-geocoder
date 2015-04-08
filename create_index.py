#!/usr/bin/env python3

import pyelasticsearch


INDEX = 'reittiopas'

es = pyelasticsearch.ElasticSearch('http://localhost:9200')


def main():
    try:
        es.create_index(index=INDEX, settings={
            "analysis": {
                "analyzer": {
                    "myAnalyzer": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": ["myLowerCaseFilter"]}},
                "filter": {
                    "myLowerCaseFilter": {
                        "type": "lowercase"}}}})
    except pyelasticsearch.exceptions.IndexAlreadyExistsError:
        print("Index already created")


if __name__ == '__main__':
    main()
