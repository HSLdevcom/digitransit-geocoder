#!/usr/bin/env python3
'''Script for just creating the ElasticSearch index with acustom analyzer.'''

import click

from geocoder.utils import prepare_es


@click.command()
def main():
    prepare_es([])

if __name__ == '__main__':
    main()
