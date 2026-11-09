"""Explicit broker-free Docker checks for Toxiproxy listener source-port reservation."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from tools.qualification import network

ROOT = Path(__file__).resolve().parents[2]


class ProxyPortReservationImagesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Given the exact pinned proxy image and a Linux Docker execution platform.
        subprocess.run(['docker', 'image', 'inspect', network.IMAGE], check=True, capture_output=True, timeout=10)
        architecture = subprocess.check_output(['docker', 'version', '--format', '{{.Server.Arch}}'], text=True, timeout=10).strip()
        if architecture not in ('amd64', 'arm64'):
            raise ValueError('port reservation probe requires Linux AMD64 or ARM64 Docker')
        cls.temporary = tempfile.TemporaryDirectory(prefix='kaferry-port-reservation-')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.binary = Path(cls.temporary.name) / 'portprobe'
        cls.coverage = Path(cls.temporary.name) / 'coverage'
        cls.coverage.mkdir(mode=0o777)
        cls.coverage.chmod(0o777)
        env = dict(os.environ, CGO_ENABLED='0', GOOS='linux', GOARCH=architecture)
        subprocess.run([env.get('GO', 'go'), 'build', '-cover', '-trimpath', '-o', str(cls.binary),
                        './src/tests/replicated/portprobe'], cwd=ROOT, env=env, check=True, timeout=180)
        cls.addClassCleanup(cls.write_coverage)

    @classmethod
    def write_coverage(cls):
        output = Path(os.environ.get('KAFERRY_REPORT_DIR', ROOT / 'build-results')) / 'proxy-port-reservation' / uuid.uuid4().hex
        output.mkdir(parents=True, exist_ok=True)
        shutil.copytree(cls.coverage, output / 'raw')
        if not list((output / 'raw').glob('covmeta.*')) or not list((output / 'raw').glob('covcounters.*')):
            raise AssertionError('instrumented proxy probe did not retain Go coverage counters')
        subprocess.run([os.environ.get('GO', 'go'), 'tool', 'covdata', 'textfmt',
                        '-i', str(output / 'raw'), '-o', str(output / 'coverage.out')], check=True, timeout=30)
        shutil.copyfile(output / 'coverage.out', output.parent / 'coverage.out')

    def exercise(self, settings, expected):
        port = network.route_port('source', 3, 'worker3')
        project = 'kaferry-port-reservation-'+uuid.uuid4().hex
        command = ['docker', 'create', '--name', project,
                   '--network', 'none', '--label', 'kaferry.port-reservation-test=true',
                   '--label', 'com.docker.compose.project='+project,
                   '--label', 'com.docker.compose.service=toxiproxy',
                   '--sysctl', f'net.ipv4.ip_local_port_range={port} {port+3}']
        for name, value in settings.items():
            command.extend(['--sysctl', f'{name}={value}'])
        container = subprocess.check_output(command+[network.IMAGE], text=True, timeout=10).strip()
        coverage_ready = False
        try:
            subprocess.run(['docker', 'cp', str(self.binary), container+':/portprobe'], check=True, timeout=10)
            subprocess.run(['docker', 'cp', str(self.coverage), container+':/coverage'], check=True, timeout=10)
            coverage_ready = True
            subprocess.run(['docker', 'start', container], check=True, capture_output=True, timeout=10)
            with patch.dict(os.environ, KAFERRY_FIXTURE_PROJECT=project):
                if expected:
                    configured = network.reservation()
                    self.assertEqual(container, configured['container'])
                    self.assertEqual(expected, configured['configured_ports'])
                else:
                    with self.assertRaisesRegex(ValueError, 'exact private listener reservation'):
                        network.reservation()
            result = subprocess.run(['docker', 'exec', '--env', 'GOCOVERDIR=/coverage',
                                     container, '/portprobe', str(port), expected],
                                    text=True, capture_output=True, timeout=30)
            self.assertEqual(0, result.returncode, result.stdout+result.stderr)
            observed = json.loads(result.stdout)
            print(json.dumps(observed, sort_keys=True))
            self.assertTrue(observed['finite_pool_exhausted'])
            self.assertEqual(f'{port} {port+3}', observed['ephemeral_range'])
            return observed
        finally:
            try:
                if coverage_ready:
                    subprocess.run(['docker', 'cp', container+':/coverage/.', str(self.coverage)], check=True, timeout=10)
            finally:
                subprocess.run(['docker', 'rm', '--force', container], check=True, capture_output=True, timeout=15)

    def test_unreserved_port_is_taken_and_proxy_cannot_rebind(self):
        # Given a disabled listener in a namespace with no reserved source ports.
        # When a disabled listener has no reservation, fill every automatic source port.
        observed = self.exercise({}, '')
        port = network.route_port('source', 3, 'worker3')
        # Then a different proxy owns that port and the first cannot listen again.
        self.assertEqual(list(range(port, port+4)), observed['upstream_source_ports'])
        self.assertEqual(4, observed['sockets_held_at_rebind'])
        self.assertEqual(500, observed['rebind_status'])

    def test_rendered_default_and_isolated_profiles_exclude_every_listener(self):
        # Given both project forms, inspect their actual rendered proxy settings.
        for project in ('kaferry-replicated', 'kaferry-lab-'+uuid.uuid4().hex+'-1'):
            with self.subTest(project=project):
                env = dict(os.environ, KAFERRY_REPLICATED_RUN_ID='0'*32)
                rendered = json.loads(subprocess.check_output(['docker', 'compose', '-f',
                    'src/tests/replicated/compose.yaml', '-p', project, '--profile', 'replicated',
                    'config', '--format', 'json'], cwd=ROOT, env=env, text=True, timeout=10))
                services = rendered['services']
                proxy = services['toxiproxy']
                self.assertEqual(network.IMAGE, proxy['image'])
                self.assertFalse(proxy.get('privileged', False))
                self.assertNotEqual('host', proxy.get('network_mode'))
                self.assertTrue(all(not value.get('sysctls') for name, value in services.items() if name != 'toxiproxy'))
                # When other upstream sockets exhaust the finite source-port pool.
                observed = self.exercise(proxy.get('sysctls', {}), network.reserved_ports())
                port = network.route_port('source', 3, 'worker3')
                # Then the actual kernel excludes every listener and explicit rebind works.
                self.assertEqual([int(value) for value in network.reserved_ports().split(',')], observed['reserved_ports'])
                self.assertEqual(list(range(port+1, port+4)), observed['upstream_source_ports'])
                self.assertEqual(3, observed['sockets_held_at_rebind'])
                self.assertEqual(200, observed['rebind_status'])


if __name__ == '__main__':
    unittest.main()
