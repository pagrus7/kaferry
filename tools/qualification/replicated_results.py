"""Collect Go scenario results with their independent, checksummed safety evidence."""

import json
import math
from pathlib import Path

from tools.ci.suites import REPLICATED_CASES
from tools.qualification.fault_results import observation_path, validate_case


IDENTITY = ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')


def copy_case(directory, id, identity):
    family, name = id.split('/')
    exposure = json.loads((directory/(family+'-'+name)/('replicated-'+family+'.json')).read_text())
    if exposure['id'] != id:
        raise ValueError('replicated result differs from the selected case')
    precondition = exposure.get('precondition')
    safety_path = observation_path(directory, exposure['safety_case'], 'case.json')
    safety = validate_case(safety_path, require_fault=precondition is None)
    if (safety['id'] != id or safety['execution'] != 'candidate CLI' or
            safety['parameters'] != dict(source_partitions=24, routing='PRESERVE') or
            any(safety[key] != identity[key] for key in IDENTITY)):
        raise ValueError('replicated evidence differs from the selected executable identity')
    if precondition is not None:
        # Go owns the healthy fixture gate. Only its safely finalized, no-fault
        # result can explain a skipped case and authorize the one fresh retry.
        if (family != 'network' or set(precondition) != {'status', 'healthy', 'max_acknowledgement_gap_seconds', 'fault_attempted', 'stage', 'detail'} or
                precondition['status'] != 'inconclusive' or not isinstance(precondition['healthy'], dict) or
                type(precondition['max_acknowledgement_gap_seconds']) not in (int, float) or
                not math.isfinite(precondition['max_acknowledgement_gap_seconds']) or
                precondition['max_acknowledgement_gap_seconds'] < 0 or
                set(precondition['healthy']) != {'before', 'after'} or exposure['events'] or safety['fault_events'] or
                exposure['restarts'] != 0 or len(exposure['worker_measurements']) != 1 or
                exposure['worker_measurements'][0]['exit_code'] != 0 or
                exposure['exact_lifetime_work_caps_qualified'] is not True):
            raise ValueError('inconclusive precondition requires a completed no-fault copy')
        safety.update(status='inconclusive', precondition=precondition)
    for reference in (safety['safety'], safety['bootstrap']):
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(directory.resolve()).as_posix()
    safety[family+'_exposure'] = exposure
    return safety


def collect(directory, selected, identity, run):
    if (not run or not run.isalnum() or not selected or len(selected) != len(set(selected)) or
            any(id not in REPLICATED_CASES for id in selected)):
        raise ValueError('invalid replicated fault selection')
    directory = Path(directory)
    paths = sorted((directory/'fault-oracle'/run).glob('*/observations/case.json'))
    observed = [json.loads(path.read_text())['id'] for path in paths]
    if len(observed) != len(set(observed)) or set(observed) - set(selected):
        raise ValueError('replicated evidence does not match the exact selected cases')
    cases = []
    for id in selected:
        try:
            family, name = id.split('/')
            exposure_path = directory if id == 'ownership/zombie-worker' else directory/(family+'-'+name)
            filename = 'replicated-ownership-case.json' if family == 'ownership' and id != 'ownership/zombie-worker' else 'replicated-'+family+'.json'
            exposure = json.loads((exposure_path/filename).read_text())
            precondition = exposure.get('precondition')
            if precondition is not None:
                if (exposure['id'] != id or exposure['oracle_run'] != run or
                        exposure['binary_sha256'] != identity['binary_sha256'] or
                        precondition['status'] != 'inconclusive' or precondition['fault_attempted'] is not False or
                        not isinstance(precondition['stage'], str) or not precondition['stage'].strip() or
                        not isinstance(precondition['detail'], str) or not precondition['detail'].strip() or
                        exposure.get('events') or exposure.get('fault')):
                    raise ValueError('inconclusive precondition requires an exact no-fault stage proof')
                if 'healthy' in precondition:
                    # Measured network preconditions still require their completed
                    # no-fault reconciliation and resource checks.
                    case = copy_case(directory, id, identity)
                else:
                    case = dict(identity, id=id, status='inconclusive', precondition=precondition)
            elif id == 'ownership/zombie-worker':
                from tools.qualification.ownership_audit import validate
                case = validate(directory, identity)
            elif id.startswith('ownership/'):
                from tools.qualification.ownership_cases import validate
                case = validate(directory, id, identity)
            else:
                case = copy_case(directory, id, identity)
            if 'safety' in case:
                if id not in observed:
                    raise ValueError('replicated evidence does not match the exact selected cases')
                observed_path = paths[observed.index(id)]
                if (directory/case['safety']['observations']).resolve() != (observed_path.parent/'samples.jsonl').resolve():
                    raise ValueError('replicated evidence references another observer run')
        except (KeyError, TypeError, IndexError) as error:
            raise ValueError('malformed replicated fault evidence') from error
        cases.append(case)
    return cases
