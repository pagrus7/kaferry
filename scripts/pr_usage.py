"""Keep Codex usage and phase attribution in a local, ignored PR ledger."""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
FIELDS = ('input_tokens', 'cached_input_tokens', 'cache_write_input_tokens',
          'output_tokens', 'reasoning_output_tokens', 'total_tokens')
PHASES = ('investigation', 'implementation', 'review', 'rework', 'delivery', 'unattributed')


def now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timestamps must include a timezone')
    return result.astimezone(timezone.utc).isoformat()


def read_rollout(path):
    """Read counters only; never retain prompts, tool arguments or response text."""
    meta, rows, warnings = {}, [], []
    previous = dict.fromkeys(FIELDS, 0)
    model, tier = 'unknown', 'unknown'
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            if not line.endswith('\n'):
                warnings.append('unfinished final record; refresh after the writer flushes')
                break
            try:
                event = json.loads(line)
                payload = event.get('payload') or {}
                if event.get('type') == 'session_meta' and not meta:
                    # Forks may copy parent metadata after their own header.
                    meta = {'id': payload['id']}
                elif event.get('type') == 'turn_context':
                    model = payload.get('model') or 'unknown'
                    tier = payload.get('service_tier') or 'unknown'
                elif event.get('type') == 'event_msg' and payload.get('type') == 'token_count':
                    info = payload.get('info')
                    if not info:
                        warnings.append(f'usage unavailable at record {number}')
                        continue
                    total = info['total_token_usage']
                    # Cache writes were absent from some older rollouts.
                    total = {key: total.get(key, 0) if key == 'cache_write_input_tokens'
                             else total[key] for key in FIELDS}
                    if any(type(value) is not int or value < 0 for value in total.values()):
                        raise ValueError('invalid counters')
                    if total == previous:
                        continue
                    delta = {key: total[key] - previous[key] for key in FIELDS}
                    if any(value < 0 for value in delta.values()):
                        warnings.append(f'counter reset at record {number}; usage across reset unknown')
                        previous = total
                        continue
                    # A first counter can carry inherited usage. Only attribute the
                    # last request when the supplied first total includes more.
                    if not rows and not any(previous.values()) and info.get('last_token_usage'):
                        last = {key: info['last_token_usage'].get(key, 0) for key in FIELDS}
                        if last != total:
                            warnings.append('initial counter includes earlier usage; only last request imported')
                            delta = last
                    if any(type(value) is not int or value < 0 for value in delta.values()):
                        raise ValueError('invalid counters')
                    previous = total
                    output = delta['total_tokens'] - delta['input_tokens']
                    if (output not in (delta['output_tokens'],
                                       delta['output_tokens'] + delta['reasoning_output_tokens'])
                            or delta['cached_input_tokens'] > delta['input_tokens']
                            or delta['reasoning_output_tokens'] > output):
                        raise ValueError('inconsistent counters')
                    delta['output_tokens'] = output
                    rows.append({'at': timestamp(event['timestamp']), 'model': model,
                                 'tier': tier, **delta})
            except (ValueError, KeyError, TypeError, AttributeError):
                warnings.append(f'unsupported or invalid record {number}; remaining usage not imported')
                break
    if not meta.get('id'):
        raise ValueError('rollout has no thread identity')
    return meta, rows, sorted(set(warnings))


def resolve_session(value, sessions):
    value = value or os.environ.get('CODEX_THREAD_ID')
    if not value:
        raise ValueError('provide --session with a thread ID or .jsonl path')
    path = Path(value).expanduser()
    if path.is_file():
        if path.suffix != '.jsonl':
            raise ValueError('provide an uncompressed .jsonl rollout')
        return path.resolve()
    if not re.fullmatch(r'[a-zA-Z0-9-]+', value):
        raise ValueError('session path does not exist or thread ID is invalid')
    matches = list(sessions.rglob(f'*-{value}.jsonl'))
    if len(matches) != 1:
        raise ValueError('thread ID must match exactly one uncompressed rollout; provide its path')
    return matches[0].resolve()


def refresh(session, snapshot=None):
    try:
        meta, rows, warnings = snapshot if snapshot is not None else read_rollout(Path(session['path']))
        if meta['id'] != session['id']:
            raise ValueError('source thread identity changed')
        old = session.get('rows', [])
        if rows[:len(old)] != old:
            raise ValueError('source counters changed or shrank')
        session.update(rows=rows, warnings=warnings, refreshed_at=now())
    except (OSError, ValueError) as error:
        session['warnings'] = [f'source unavailable or changed ({type(error).__name__}); retained prior import']


def label_at(session, at):
    label = None
    for marker in session['markers']:
        if marker['at'] is None or marker['at'] <= at:
            label = marker
    return label


def load_rates(path):
    rates = json.loads(path.read_text())
    if not all(isinstance(rates.get(k), str) and rates[k] for k in ('as_of', 'basis', 'source')):
        raise ValueError('rate card needs as_of, basis and source')
    for rate in rates['models'].values():
        if any(type(rate.get(k)) not in (int, float) or not math.isfinite(rate[k]) or rate[k] < 0
               for k in ('input', 'cached_input', 'output')):
            raise ValueError('rates must be finite nonnegative numbers per million tokens')
    return rates


def summarize(work, rates):
    groups = {}
    warnings, unattributed, outside = [], 0, 0
    for session in work['sessions'].values():
        warnings.extend(f'{session["id"]}: {w}' for w in session.get('warnings', []))
        included = 0
        for row in session.get('rows', []):
            label = label_at(session, row['at'])
            if not label or label['phase'] == 'stopped':
                outside += row['total_tokens']
                continue
            included += 1
            key = (label['phase'], label['purpose'], label['round'], session['id'], row['model'], row['tier'])
            if key not in groups:
                groups[key] = dict(zip(('phase', 'purpose', 'round', 'session', 'model', 'tier'), key),
                                   **dict.fromkeys(FIELDS, 0), estimated_usd=0.0, unpriced_tokens=0)
            group = groups[key]
            for field in FIELDS:
                group[field] += row[field]
            if label['phase'] == 'unattributed' or label['purpose'] == 'unattributed':
                unattributed += row['total_tokens']
            rate = rates['models'].get(row['model'])
            if rate is None or row['cache_write_input_tokens']:
                group['unpriced_tokens'] += row['total_tokens']
            else:
                group['estimated_usd'] += ((row['input_tokens'] - row['cached_input_tokens']) * rate['input']
                                           + row['cached_input_tokens'] * rate['cached_input']
                                           + row['output_tokens'] * rate['output']) / 1_000_000
        if not included:
            warnings.append(f'{session["id"]}: no usage recorded inside work boundaries')
    rows = sorted(groups.values(), key=lambda r: (r['phase'], r['purpose'], r['round'], r['session'], r['model'], r['tier']))
    total = {field: sum(row[field] for row in rows) for field in (*FIELDS, 'estimated_usd', 'unpriced_tokens')}
    return {'work': work['work'], 'pr': work.get('pr'), 'generated_at': now(),
            'registered_sessions': len(work['sessions']), 'totals': total,
            'unattributed_tokens': unattributed, 'outside_work_tokens': outside,
            'warnings': warnings, 'rate_card': rates, 'actual_billed_usd': None, 'rows': rows,
            'file_reads': summarize_reads(work.get('reads', []))}


def summarize_reads(reads):
    groups = {}
    for read in reads:
        key = tuple(read[k] for k in ('path', 'phase', 'purpose', 'round', 'session'))
        if key not in groups:
            groups[key] = dict(zip(('path', 'phase', 'purpose', 'round', 'session'), key),
                               reads=0, lines=0, bytes=0, ranges=[])
        group = groups[key]
        group['reads'] += 1
        group['lines'] += read['lines']
        group['bytes'] += read['bytes']
        group['ranges'].append([read['start'], read['end']])
    return [groups[key] for key in sorted(groups)]


def tracked_read(work, filename, start, end, session):
    if start < 1 or (end is not None and end < start):
        raise ValueError('line range must start at 1 or later and end at or after start')
    thread = session or os.environ.get('CODEX_THREAD_ID')
    if thread not in work['sessions']:
        raise ValueError('read needs a registered thread ID in --session or CODEX_THREAD_ID')
    at = now()
    label = label_at(work['sessions'][thread], at)
    if not label or label['phase'] == 'stopped':
        raise ValueError('mark an active phase before reading')
    path = (ROOT / filename).resolve()
    relative = path.relative_to(ROOT.resolve()).as_posix()
    # Preserve newline bytes so the measurement matches the selected text.
    with path.open(encoding='utf-8', newline='') as stream:
        lines = []
        for number, line in enumerate(stream, 1):
            if end is not None and number > end:
                break
            if number >= start:
                lines.append(line)
    text = ''.join(lines)
    observation = {'at': at, 'session': thread, 'path': relative,
                   'phase': label['phase'], 'purpose': label['purpose'], 'round': label['round'],
                   'start': start if lines else None, 'end': start + len(lines) - 1 if lines else None,
                   'lines': len(lines), 'bytes': len(text.encode('utf-8'))}
    return text, observation


def render(report):
    total = report['totals']
    lines = [f'Local usage: {report["work"]} | PR: {report["pr"] or "unassigned"}',
             f'Registered threads: {report["registered_sessions"]}; unregistered work is not measured.',
             f'Recorded tokens: {total["total_tokens"]:,}; unattributed: {report["unattributed_tokens"]:,}; '
             f'outside work boundaries: {report["outside_work_tokens"]:,}',
             f'Priced subtotal: ${total["estimated_usd"]:.4f}; unpriced tokens: {total["unpriced_tokens"]:,}',
             'Actual billed spend: unknown. No subscription allocation or billing reconciliation.',
             f'Rate card ({report["rate_card"]["as_of"]}): {report["rate_card"]["basis"]}', '']
    for dimensions in (('phase', 'purpose', 'round'), ('model', 'tier'), ('session',)):
        groups = defaultdict(lambda: dict.fromkeys(('input_tokens', 'cached_input_tokens', 'output_tokens',
                                                   'estimated_usd', 'unpriced_tokens'), 0))
        for row in report['rows']:
            group = groups[tuple(str(row[k]) for k in dimensions)]
            for key in group:
                group[key] += row[key]
        lines.append(' / '.join(dimensions) + ' | input | cached (within input) | output (includes reasoning) | est. USD | unpriced tokens')
        for key, group in sorted(groups.items()):
            lines.append(' / '.join(key) + f' | {group["input_tokens"]:,} | {group["cached_input_tokens"]:,}'
                         f' | {group["output_tokens"]:,} | ${group["estimated_usd"]:.4f} | {group["unpriced_tokens"]:,}')
        lines.append('')
    lines.append('Tracked file reads | phase / purpose / round | thread | reads | emitted lines | UTF-8 bytes')
    for row in report.get('file_reads', []):
        lines.append(f'{row["path"]} | {row["phase"]} / {row["purpose"]} / {row["round"]}'
                     f' | {row["session"]} | {row["reads"]:,} | {row["lines"]:,} | {row["bytes"]:,}')
    lines.append('Only tracked read commands are measured; repeats count again. Tool output may be truncated.')
    lines.append('File read volume is not token cost; request usage also includes history, instructions and caching.')
    lines.extend(f'Warning: {warning}' for warning in report['warnings'])
    return '\n'.join(lines) + '\n'


def save(path, value, replace=True):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
            if replace:
                os.replace(temporary, path)
            else:
                os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def archive_report(directory, report):
    history = directory / 'history'
    history.mkdir(exist_ok=True)
    # Preserve an existing report when initializing snapshot history.
    prior = directory / 'report.json'
    if not any(history.glob('*.json')) and prior.exists():
        save(history / '000001.json', json.loads(prior.read_text()), replace=False)
    sequence = max((int(p.stem) for p in history.glob('*.json')), default=0) + 1
    save(history / f'{sequence:06d}.json', report, replace=False)


def history_rows(directory, work=None):
    paths = sorted((directory / work / 'history').glob('*.json')) if work else sorted(directory.glob('*/history/*.json'))
    if not paths:
        raise ValueError('no saved snapshots; run report for the work first')
    if not work:
        # Across tasks, compare only the newest snapshot; never sum cumulative reports.
        latest = {}
        for path in paths:
            latest[path.parent.parent.name] = path
        paths = list(latest.values())
    result = []
    for path in paths:
        report = json.loads(path.read_text())
        rates = json.dumps(report['rate_card'], sort_keys=True).encode()
        result.append({'snapshot': path.stem, 'at': report['generated_at'], 'work': report['work'],
                       'pr': report['pr'], 'tokens': report['totals']['total_tokens'],
                       'unattributed': report['unattributed_tokens'],
                       'unpriced': report['totals']['unpriced_tokens'],
                       'estimated_usd': report['totals']['estimated_usd'],
                       'rate_card': hashlib.sha256(rates).hexdigest()[:12],
                       'read_lines': (sum(r['lines'] for r in report['file_reads'])
                                      if 'file_reads' in report else None),
                       'warnings': len(report['warnings'])})
    return result


def render_history(rows):
    lines = ['Saved cumulative snapshots; values are not added together.',
             'Compare dollar estimates only with the same rate card; imports can add earlier usage.',
             'snapshot | recorded at | work / PR | tokens | unattributed | unpriced | est. USD | rate card | tracked lines | warnings']
    for row in rows:
        read_lines = f'{row["read_lines"]:,}' if row['read_lines'] is not None else 'unknown'
        lines.append(f'{row["snapshot"]} | {row["at"]} | {row["work"]} / {row["pr"] or "unassigned"}'
                     f' | {row["tokens"]:,} | {row["unattributed"]:,} | {row["unpriced"]:,}'
                     f' | ${row["estimated_usd"]:.4f} | {row["rate_card"]} | {read_lines} | {row["warnings"]}')
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for command in ('mark', 'stop', 'attach', 'report', 'read', 'history'):
        sub = commands.add_parser(command)
        sub.add_argument('work', nargs='?' if command == 'history' else None,
                         help='stable local name; history without a name compares latest task snapshots')
        if command in ('mark', 'stop'):
            sub.add_argument('--session', help='thread ID or .jsonl path; default CODEX_THREAD_ID')
        if command == 'mark':
            sub.add_argument('--phase', choices=PHASES, default='unattributed')
            sub.add_argument('--purpose', default='unattributed')
            sub.add_argument('--round', type=int, default=1)
            sub.add_argument('--from-start', action='store_true', help='include existing usage on first registration')
        if command == 'attach':
            sub.add_argument('--pr', type=int, required=True)
        if command == 'read':
            sub.add_argument('file', help='UTF-8 repository file, relative to the repository root')
            sub.add_argument('--start', type=int, default=1, help='first line, inclusive')
            sub.add_argument('--end', type=int, help='last line, inclusive; default end of file')
            sub.add_argument('--session', help='registered thread ID; default CODEX_THREAD_ID')
        if command == 'report':
            sub.add_argument('--rates', type=Path, default=Path(__file__).with_name('pr_usage_rates.json'))
        if command in ('report', 'history'):
            sub.add_argument('--json', action='store_true', help='print JSON instead of text')
    args = parser.parse_args(argv)
    if args.work is not None and not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}', args.work):
        parser.error('work must be 1-80 letters, digits, dots, underscores or hyphens, starting with a letter or digit')
    directory = ROOT / '.cache' / 'usage'
    directory.mkdir(parents=True, exist_ok=True)
    try:
        # All work files share a lock, including checks for duplicate registration.
        with (directory / '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if args.command == 'history':
                rows = history_rows(directory, args.work)
                print(json.dumps(rows, indent=2) if args.json else render_history(rows), end='\n' if args.json else '')
                return
            work_directory = directory / args.work
            path = work_directory / 'ledger.json'
            work = json.loads(path.read_text()) if path.exists() else {'work': args.work, 'sessions': {}}
            if args.command != 'mark' and not path.exists():
                raise ValueError('unknown work; register a session with mark first')
            if args.command in ('mark', 'stop'):
                sessions = Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex'))) / 'sessions'
                source = resolve_session(args.session, sessions)
                meta, rows, warnings = read_rollout(source)
                thread = meta['id']
                if args.command == 'mark':
                    if args.round < 1 or not args.purpose.strip() or any(ord(c) < 32 for c in args.purpose):
                        raise ValueError('purpose must be nonempty plain text and round must be positive')
                    for other in directory.glob('*/ledger.json'):
                        if other != path and thread in json.loads(other.read_text()).get('sessions', {}):
                            raise ValueError('thread already belongs to another work; use a separate thread per work')
                    if args.from_start and thread in work['sessions']:
                        raise ValueError('--from-start is only allowed on first registration')
                    session = work['sessions'].setdefault(thread, {**meta, 'path': str(source), 'markers': [],
                                                                  'rows': rows, 'warnings': warnings})
                    marker = {'at': None if args.from_start else now(), 'phase': args.phase,
                              'purpose': args.purpose, 'round': args.round}
                else:
                    session = work['sessions'][thread]
                    marker = {'at': now(), 'phase': 'stopped', 'purpose': 'stopped', 'round': 1}
                refresh(session, (meta, rows, warnings))
                session['markers'].append(marker)
            elif args.command == 'read':
                text, observation = tracked_read(work, args.file, args.start, args.end, args.session)
                sys.stdout.write(text)
                sys.stdout.flush()
                work.setdefault('reads', []).append(observation)
                save(path, work)
                return
            elif args.command == 'attach':
                if args.pr < 1:
                    raise ValueError('PR number must be positive')
                work['pr'] = args.pr
            else:
                rates = load_rates(args.rates)
                for session in work['sessions'].values():
                    refresh(session)
                report = summarize(work, rates)
                save(path, work)
                archive_report(work_directory, report)
                save(work_directory / 'report.json', report)
                text = render(report)
                (work_directory / 'report.txt').write_text(text)
                print(json.dumps(report, indent=2) if args.json else text, end='\n' if args.json else '')
                return
            work_directory.mkdir(exist_ok=True)
            save(path, work)
            print(f'Saved locally: {path}')
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f'Usage ledger: {error}\n')


if __name__ == '__main__':
    main()
