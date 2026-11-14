"""Build the pinned proxy with diagnostic observations at each link transition."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

MODULE = 'github.com/Shopify/toxiproxy/v2@v2.13.0'
REVISION = 'ff428b0e2c46af894c55711c2a3e427fce6b1ef2'
MODULE_SUM = 'h1:+SASZmq0GBNEc38gHzWvWkntJCk7olgfZr1sP6O/Bhk='
ASSETS = Path(__file__).with_name('proxy_evidence')
PREIMAGES = {
    'link.go': '6fb4999d564a49befddf27135da9b389fb17a60642061a40cae65c54f811dabd',
    'toxics/toxic.go': '2a7183c48f8848b8c7a7c970461a309e3093f780a7820987070ab0be068ed4a2',
}


def patch_source(source):
    for name, expected in PREIMAGES.items():
        if hashlib.sha256((source/name).read_bytes()).hexdigest() != expected:
            raise ValueError('pinned proxy preimage differs: '+name)
    patch = (ASSETS/'activation.patch').read_bytes()
    subprocess.run(['patch', '--batch', '--fuzz=0', '-p1'], input=patch, cwd=source, check=True)
    shutil.copyfile(ASSETS/'activation.go.txt', source/'toxics/kaferry_evidence.go')
    shutil.copyfile(ASSETS/'activation_test.go.txt', source/'kaferry_evidence_test.go')
    return hashlib.sha256(patch+(ASSETS/'activation.go.txt').read_bytes()).hexdigest()


def prepare(output, env):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    go = env.get('GO', 'go')
    module = json.loads(subprocess.check_output([go, 'mod', 'download', '-json', MODULE], env=env))
    if module['Sum'] != MODULE_SUM or module.get('Origin', {}).get('Hash') != REVISION:
        raise ValueError('proxy evidence requires the exact pinned upstream source')
    with tempfile.TemporaryDirectory(prefix='kaferry-proxy-') as temporary:
        source = Path(temporary)/'source'
        shutil.copytree(module['Dir'], source)
        source.chmod(source.stat().st_mode | 0o200)
        for path in source.rglob('*'):
            path.chmod(path.stat().st_mode | 0o200)
        patch_sha = patch_source(source)
        # The real patched link must retain both records even if its caller fails.
        subprocess.run([go, 'test', '-count=1', '-run', '^TestKaferryActivationEvidence', '.'],
                       cwd=source, env=env, check=True)
        architecture = subprocess.check_output([go, 'env', 'GOARCH'], env=env, text=True).strip()
        binary = output/'toxiproxy'
        subprocess.run([go, 'build', '-mod=readonly', '-ldflags',
                        '-X github.com/Shopify/toxiproxy/v2.Version=2.13.0', '-o', str(binary), './cmd/server'],
                       cwd=source, env=dict(env, CGO_ENABLED='0', GOOS='linux', GOARCH=architecture), check=True)
    provenance = dict(upstream_module=MODULE, upstream_revision=REVISION, upstream_sum=MODULE_SUM,
                      observation_sha256=patch_sha, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                      clock='Linux host CLOCK_REALTIME; UTC RFC3339Nano',
                      observations='per-link pipeline installation and selected toxic dispatch')
    (output/'provenance.json').write_text(json.dumps(provenance, indent=2)+'\n')
    return binary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    prepare(parser.parse_args().output/'proxy-evidence', dict(os.environ))


if __name__ == '__main__':
    main()
