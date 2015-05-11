#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from argparse import Namespace

import click
from locust import runners

import locustfile


def find_limit(num_clients=10, host='http://localhost:8888'):
    options = Namespace()
    options.host = host

    options.num_clients = num_clients
    max = None
    min = 0
    while True:
        done = False
        print(min, max)
        options.num_requests = options.num_clients * 10
        options.hatch_rate = options.num_clients

        runners.locust_runner = runners.LocalLocustRunner(
            [locustfile.TypingUser, locustfile.MovingUser], options)
        runners.locust_runner.start_hatching(wait=True)
        runners.locust_runner.greenlet.join()

        for name, value in runners.locust_runner.stats.entries.items():
            if value.median_response_time > 300:
                done = True
        if done:
            max = options.num_clients
        else:
            min = options.num_clients
        if max is not None:
            if max < min + 5:
                for name, value in runners.locust_runner.stats.entries.items():
                    print(name,
                          value.min_response_time,
                          value.median_response_time,
                          value.max_response_time,
                          value.total_rps)
                return (min, max)
            options.num_clients = (max + min) / 2
        else:
            options.num_clients = options.num_clients * 2


@click.command()
@click.option("-h", '--host',
              default='http://localhost:8888', show_default=True)
@click.option("-n", '--num_clients',
              default=10, show_default=True)
def main(num_clients, host):
    print(find_limit(num_clients, host))


if __name__ == '__main__':
    main()
