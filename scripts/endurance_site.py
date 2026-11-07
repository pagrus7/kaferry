"""Build GitHub Pages from checksummed endurance JSON already published on releases."""

import argparse
from datetime import datetime, timezone
import hashlib
from html import escape
import json
from pathlib import Path
import re
import shutil
import subprocess
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
ASSET = re.compile(r'(endurance-(?:linux|darwin)-(?:amd64|arm64)-[0-9]+)-endurance-results\.json')


def api(path):
    pages = json.loads(subprocess.check_output(['gh', 'api', '--paginate', '--slurp', path]))
    return [item for page in pages for item in page]


def download(repository, asset):
    return subprocess.check_output(['gh', 'api', f'repos/{repository}/releases/assets/{asset["id"]}',
                                    '-H', 'Accept: application/octet-stream'])


def collect(repository):
    runs = []
    for release in api(f'repos/{repository}/releases?per_page=100'):
        if release['draft'] or release['prerelease']:
            continue
        assets = {a['name']: a for a in api(f'repos/{repository}/releases/{release["id"]}/assets?per_page=100')}
        for name, asset in assets.items():
            match = ASSET.fullmatch(name)
            if not match:
                continue
            prefix = match[1]
            checksum_name = prefix + '-endurance.SHA256SUMS'
            if checksum_name not in assets:
                raise ValueError(f'{release["tag_name"]}/{name}: missing checksums')
            checksums = dict(line.split(maxsplit=1)[::-1] for line in
                             download(repository, assets[checksum_name]).decode().splitlines())
            content = download(repository, asset)
            if checksums.get(name) != hashlib.sha256(content).hexdigest():
                raise ValueError(f'{release["tag_name"]}/{name}: checksum mismatch')
            report = json.loads(content)
            validate(report)
            started = datetime.fromisoformat(report['manifest']['started_at'])
            if started.tzinfo is None:
                raise ValueError(f'{name}: start time needs a UTC offset')
            runs.append(dict(prefix=prefix, release=release['tag_name'], report=report, content=content,
                             started=started.astimezone(timezone.utc),
                             downloads={key: f'https://github.com/{repository}/releases/download/'
                                        f'{quote(release["tag_name"], safe="")}/{quote(prefix + suffix)}'
                                        for key, suffix in [('Report', '-endurance-results.md'),
                                                            ('Logs and samples', '-endurance-evidence.tar.gz'),
                                                            ('Checksums', '-endurance.SHA256SUMS')]
                                        if prefix + suffix in assets}))
    return sorted(runs, key=lambda run: run['started'], reverse=True)


def validate(report):
    receipt = report['manifest']
    metrics = report['measurements']
    if report['schema'] != 1 or receipt['suite'] != 'endurance' or receipt['status'] not in ('passed', 'failed', 'interrupted'):
        raise ValueError('unsupported or unfinished endurance report')
    if len(metrics) > 1 or any(m.get('binary_sha256') != receipt.get('binary_sha256') for m in metrics):
        raise ValueError('endurance measurements identify a different binary')
    if receipt['status'] == 'passed':
        m = metrics[0] if metrics else {}
        d = m.get('delivery', {})
        if (not m.get('passed') or m.get('skipped') or not m.get('endurance', {}).get('load_validated') or
                not d.get('reconciliation_complete') or d.get('integrity', {}).get('matched') is not True):
            raise ValueError('passing endurance evidence is incomplete')


def number(value, digits=0, scale=1):
    return f'{value / scale:,.{digits}f}' if isinstance(value, (int, float)) and not isinstance(value, bool) else 'Unavailable'


def duration(seconds):
    if seconds is None:
        return 'Unavailable'
    if seconds >= 3600 and seconds % 3600 == 0:
        return f'{seconds // 3600:,} hour' + ('s' if seconds != 3600 else '')
    if seconds >= 60 and seconds % 60 == 0:
        return f'{seconds // 60:,} minute' + ('s' if seconds != 60 else '')
    return f'{number(seconds)} seconds'


def link(url, text):
    return f'<a href="{escape(url, quote=True)}">{escape(text)}</a>'


def table(rows):
    return '<dl class="measurements">' + ''.join(
        f'<div><dt>{escape(label)}</dt><dd>{escape(str(value))}</dd></div>' for label, value in rows) + '</dl>'


def render_run(run, latest):
    receipt = run['report']['manifest']
    m = next(iter(run['report']['measurements']), {})
    d = m.get('delivery', {})
    observed = d.get('observed', {})
    elapsed = m.get('elapsed_seconds')
    workload = receipt.get('workload', {})
    status = receipt['status']
    rehearsal = receipt.get('requested_seconds', 0) < 86400
    title = f'{run["release"]} · {m.get("platform", receipt.get("machine", "platform unavailable"))}'
    badge = f'<span class="status {status}">{status.upper()}</span>'
    badge += '<span class="rehearsal">REHEARSAL</span>' if rehearsal else ''
    measurements = table([
        ('Publishing requested / drain', f'{duration(receipt.get("requested_seconds"))} / {duration(receipt.get("drain_seconds"))}'),
        ('Measured test duration', duration(elapsed if elapsed and elapsed > 0 else None)),
        ('Submitted / acknowledged', f'{number(d.get("submitted"))} / {number(d.get("published"))}'),
        ('Unique received / verified', f'{number(d.get("unique_received"))} / {number(d.get("verified"))}'),
        ('Missing / duplicate / corrupted', ' / '.join(number(d.get(k)) for k in ('missing', 'duplicates', 'corrupted'))),
        ('Unexpected / reordered / publish errors', ' / '.join(number(d.get(k)) for k in ('unexpected', 'reordered', 'publish_errors'))),
        ('Record fingerprints match', {True: 'Yes', False: 'No'}.get(d.get('integrity', {}).get('matched'), 'Unavailable')),
        ('Destination pauses / recovered', f'{number(m.get("destination_stalls"))} / {number(m.get("endurance", {}).get("recoveries"))}'),
        ('Sampled peak memory', number(m.get('sampled_peak_rss_bytes') if m.get('rss_samples', 0) else None, 1, 2**20) + ' MiB'),
        ('Delivery p95 / p99 / p99.9 (ms)', ' / '.join(number(observed.get(k), 1, .001) for k in
                                                   ('p95_latency_seconds', 'p99_latency_seconds', 'p999_latency_seconds'))),
        ('Maximum delivery time (ms)', number(observed.get('max_latency_seconds'), 1, .001)),
        ('Observed peak backlog', number(observed.get('peak_backlog')) + ' records'),
        ('Shutdown (seconds)', number(m.get('shutdown_seconds') if m.get('passed') else None, 3)),
    ])
    configuration = table([
        ('Base input rate', number(workload.get('rate')) + ' records/second plus bursts'),
        ('Source / destination partitions', f'{number(workload.get("source_partitions"))} / {number(workload.get("destination_partitions"))}'),
        ('Routing / replication factor', f'{workload.get("routing", "Unavailable")} / {number(workload.get("replication_factor"))}'),
        ('Broker versions', f'{receipt.get("source_version", "Unavailable")} → {receipt.get("destination_version", "Unavailable")}'),
        ('Executable', receipt.get('binary_version', 'Unavailable')),
        ('Fixture source commit', receipt.get('source_commit', 'Unavailable')),
        ('Binary SHA-256', receipt.get('binary_sha256', 'Unavailable')),
    ])
    downloads = [link('data/' + run['prefix'] + '.json', 'Full JSON')]
    downloads.extend(link(url, label) for label, url in run['downloads'].items())
    note = ('This rehearsal does not establish 24 hours of continuous uptime. ' if rehearsal else '')
    if status != 'passed':
        note += 'This run did not pass; requested duration and incomplete observations do not establish successful delivery. '
    return (f'<details class="run" id="{escape(run["prefix"])}" {"open" if latest else ""}>'
            f'<summary><span>{escape(title)}</span><span class="badges">{badge}</span></summary>'
            f'<div class="run-body"><p class="date">Started {escape(receipt["started_at"])} (UTC offset shown)</p>'
            f'<p class="notice">{escape(note)}</p><h3>Delivery &amp; resources</h3>{measurements}'
            f'<h3>Workload &amp; identity</h3>{configuration}<nav class="downloads" aria-label="Run evidence">'
            + ''.join(downloads) + '</nav></div></details>')


def build(repository, output):
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', repository):
        raise ValueError('repository must be OWNER/REPO')
    runs = collect(repository)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'data').mkdir()
    for run in runs:
        (output / 'data' / (run['prefix'] + '.json')).write_bytes(run['content'])
    latest = runs[0] if runs else None
    headline = (f'{latest["report"]["manifest"]["status"].capitalize()} · '
                f'{duration(latest["report"]["manifest"].get("requested_seconds"))} requested · {latest["release"]}'
                if latest else 'No published endurance runs yet')
    m = next(iter(latest['report']['measurements']), {}) if latest else {}
    d = m.get('delivery', {})
    highlights = table([
        ('Records verified', number(d.get('verified'))),
        ('Missing / duplicate / corrupted', ' / '.join(number(d.get(k)) for k in ('missing', 'duplicates', 'corrupted'))),
        ('Sampled peak memory (MiB)', number(m.get('sampled_peak_rss_bytes') if m.get('rss_samples', 0) else None, 1, 2**20)),
    ]) if latest else ''
    history = ''.join(f'<tr><td>{link("#" + run["prefix"], run["release"])}</td>'
                      f'<td>{run["started"].date().isoformat()}</td>'
                      f'<td>{escape(next(iter(run["report"]["measurements"]), {}).get("platform", "Unavailable"))}</td>'
                      f'<td>{escape(duration(run["report"]["manifest"].get("requested_seconds")))}</td>'
                      f'<td>{escape(run["report"]["manifest"]["status"].upper())}</td></tr>' for run in runs)
    template = (ROOT / 'web/endurance/index.html').read_text()
    for key, value in dict(repository=escape(repository), headline=escape(headline),
                           updated=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
                           highlights=highlights, history=history,
                           runs=''.join(render_run(run, i == 0) for i, run in enumerate(runs))).items():
        template = template.replace('{{' + key + '}}', value)
    (output / 'index.html').write_text(template)
    shutil.copyfile(ROOT / 'assets/kaferry-logo.svg', output / 'logo.svg')
    print(f'Built {len(runs)} published endurance runs in {output}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', default='pagrus7/kaferry')
    parser.add_argument('--output', type=Path, required=True, help='new output directory')
    args = parser.parse_args()
    build(args.repository, args.output)


if __name__ == '__main__':
    main()
