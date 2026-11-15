"""Non-broker checks for replicated network isolation and startup failures."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.qualification import network


class NetworkTest(unittest.TestCase):
    def test_populate_requires_fresh_container_and_exact_routes_before_receipt(self):
        # Given an existing fault state, reject it without resetting another run.
        with tempfile.TemporaryDirectory() as temporary, patch.object(network, 'reservation', return_value={'configured_ports': network.reserved_ports()}):
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
            self.assertTrue((output / 'proxy-reservation.json').is_file())

    def test_bound_listener_equivalence_still_requires_wildcard_and_exact_port(self):
        # Given a listener reported by the server, accept only equivalent wildcard binds.
        expected = network.routes()[0]
        for listener, accepted in [('0.0.0.0:26092', True), ('[::]:26092', True),
                                   ('127.0.0.1:26092', False), ('[::1]:26092', False),
                                   ('[::]:26093', False), ('::26092', False), ('', False), (None, False)]:
            with self.subTest(listener=listener):
                self.assertEqual(accepted, network.matching_route(dict(expected, listen=listener), expected))
        self.assertFalse(network.matching_route(dict(expected, enabled=1), expected))
