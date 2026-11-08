"""Non-broker checks for replicated network isolation and startup failures."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.qualification import network


class NetworkTest(unittest.TestCase):
    def test_routes_keep_each_client_and_internal_listener_distinct(self):
        # Given six brokers and four independent client paths per broker.
        rows = network.routes()
        # When configuring proxies, then metadata and quorum ports cannot bypass them.
        self.assertEqual(36, len(rows))
        self.assertEqual(36, len({row['name'] for row in rows}))
        self.assertEqual(36, len({row['listen'] for row in rows}))
        self.assertEqual(36, len({row['upstream'] for row in rows}))
        by_name = {row['name']: row for row in rows}
        self.assertEqual('source-2:9092', by_name['source-2-internal']['upstream'])
        self.assertEqual('0.0.0.0:26093', by_name['source-2-internal']['listen'])
        self.assertEqual('0.0.0.0:29094', by_name['destination-3-controller']['listen'])
        self.assertEqual('destination-3:9097', by_name['destination-3-worker3']['upstream'])
        self.assertEqual(24, len(network.client_ports()))
        self.assertEqual('127.0.0.1:47392,127.0.0.1:47393,127.0.0.1:47394', network.endpoints('destination', 3))
        self.assertEqual('127.0.0.1:46092,127.0.0.1:46093,127.0.0.1:46094', network.endpoints('source'))

    def test_populate_requires_fresh_container_and_exact_routes_before_receipt(self):
        # Given an existing fault state, reject it without resetting another run.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with patch.object(network, 'request', return_value={'owned': {}}) as request:
                with self.assertRaisesRegex(ValueError, 'not fresh'):
                    network.populate(output)
            self.assertEqual(1, request.call_count)
            expected = network.routes()
            # The pinned server reports its bound listener, not the request string.
            observed = {row['name']: dict(row, listen=row['listen'].replace('0.0.0.0', '[::]'), toxics=[])
                        for row in expected}
            # When a route is missing, disabled, altered or toxic, then no success receipt exists.
            for changed in ['missing', 'enabled', 'upstream', 'toxics']:
                bad = {name: dict(row) for name, row in observed.items()}
                name = 'source-2-worker1'
                if changed == 'missing':
                    del bad[name]
                else:
                    bad[name][changed] = {'enabled': False, 'upstream': 'other:9095', 'toxics': [{}]}[changed]
                with self.subTest(changed=changed), patch.object(network, 'request', side_effect=[{}, {}, bad]):
                    with self.assertRaisesRegex(ValueError, 'do not match'):
                        network.populate(output)
                self.assertFalse((output / 'proxy-routes.json').exists())
            # Then a verified population records all generic routes.
            with patch.object(network, 'request', side_effect=[{}, {}, observed]) as request:
                network.populate(output)
            self.assertEqual(('POST', '/populate', expected), request.call_args_list[1].args)
            self.assertTrue((output / 'proxy-routes.json').is_file())

    def test_bound_listener_equivalence_still_requires_wildcard_and_exact_port(self):
        # Given a listener reported by the server, accept only equivalent wildcard binds.
        expected = network.routes()[0]
        for listener, accepted in [('0.0.0.0:26092', True), ('[::]:26092', True),
                                   ('127.0.0.1:26092', False), ('[::1]:26092', False),
                                   ('[::]:26093', False), ('::26092', False), ('', False), (None, False)]:
            with self.subTest(listener=listener):
                self.assertEqual(accepted, network.matching_route(dict(expected, listen=listener), expected))
        self.assertFalse(network.matching_route(dict(expected, enabled=1), expected))
