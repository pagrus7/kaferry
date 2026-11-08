"""Configure the replicated fixture's isolated Toxiproxy routes."""

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen

IMAGE = 'ghcr.io/shopify/toxiproxy:2.13.0@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa'
API = 'http://127.0.0.1:48074'
LISTENERS = ('internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3')


def route_port(side, node, listener):
    base = 46092 if side == 'source' else 47092
    shifts = dict(internal=-20000, controller=-18000, admin=0, worker1=100, worker2=200, worker3=300)
    return base + shifts[listener] + node - 1


def routes():
    return [dict(name=f'{side}-{node}-{listener}',
                 listen=f'0.0.0.0:{route_port(side, node, listener)}',
                 upstream=f'{side}-{node}:{9092 + index}', enabled=True)
            for side in ('source', 'destination') for node in range(1, 4)
            for index, listener in enumerate(LISTENERS)]


def client_ports():
    return tuple(route_port(side, node, listener)
                 for side in ('source', 'destination')
                 for listener in LISTENERS[2:] for node in range(1, 4))


def endpoints(side, worker=0):
    listener = f'worker{worker}' if worker else 'admin'
    return ','.join(f'127.0.0.1:{route_port(side, node, listener)}' for node in range(1, 4))


def request(method, path, value=None):
    data = json.dumps(value).encode() if value is not None else None
    query = Request(API + path, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urlopen(query, timeout=10) as response:
        return json.loads(response.read(1024 * 1024))


def populate(output):
    # A fresh container is required: do not reset a previously owned fault state.
    if request('GET', '/proxies'):
        raise ValueError('replicated proxy container is not fresh')
    expected = routes()
    request('POST', '/populate', expected)
    observed = request('GET', '/proxies')
    if set(observed) != {row['name'] for row in expected} or any(
            not matching_route(observed[row['name']], row) or
            observed[row['name']].get('toxics') for row in expected):
        raise ValueError('replicated proxy routes do not match the fixture')
    (output / 'proxy-routes.json').write_text(json.dumps(expected, indent=2) + '\n')


def matching_route(observed, expected):
    # Go may report an IPv4 wildcard listener as its dual-stack IPv6 wildcard.
    port = expected['listen'].rsplit(':', 1)[1]
    return (all(observed.get(key) == expected[key] for key in ('name', 'upstream')) and
            observed.get('enabled') is True and
            observed.get('listen') in (f'0.0.0.0:{port}', f'[::]:{port}'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    populate(parser.parse_args().output)


if __name__ == '__main__':
    main()
