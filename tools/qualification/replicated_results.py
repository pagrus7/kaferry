"""Collect Go scenario results with their independent, checksummed safety evidence."""

import json
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
        # Go owns the measured rate gate. Only its safely finalized, no-fault
        # result can explain a skipped case and authorize the one fresh retry.
        if (family != 'network' or set(precondition) != {'status', 'healthy'} or
                precondition['status'] != 'inconclusive' or not isinstance(precondition['healthy'], dict) or
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
    if sorted(observed) != sorted(selected):
        raise ValueError('replicated evidence does not match the exact selected cases')
    cases = []
    for id in selected:
        try:
            if id == 'ownership/zombie-worker':
                from tools.qualification.ownership_audit import validate
                case = validate(directory, identity)
            elif id.startswith('ownership/'):
                from tools.qualification.ownership_cases import validate
                case = validate(directory, id, identity)
            else:
                case = copy_case(directory, id, identity)
            observed_path = paths[observed.index(id)]
            if (directory/case['safety']['observations']).resolve() != (observed_path.parent/'samples.jsonl').resolve():
                raise ValueError('replicated evidence references another observer run')
        except (KeyError, TypeError, IndexError) as error:
            raise ValueError('malformed replicated fault evidence') from error
        cases.append(case)
    return cases
