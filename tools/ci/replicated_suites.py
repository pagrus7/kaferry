"""Suite data and observation budgets used by the replicated lab."""

import json
from pathlib import Path


REPLICATED_CASES = {'ownership/zombie-worker': 'TestReplicatedZombieWorker'} | {
    'broker/'+name: 'TestReplicatedBrokerFaults/'+name for name in
    ('source-leader', 'destination-leader', 'rolling-source', 'rolling-destination', 'destination-isr-loss')} | {
    'network/'+name: 'TestReplicatedNetworkFaults/'+name for name in
    ('destination-outage', 'destination-leaders-outage', 'latency', 'bandwidth', 'slow-close')} | {
    'ownership/'+name: 'TestReplicatedOwnershipCases/'+name for name in
    ('worker-kill-rebalance', 'coordinator-kill-rebalance', 'worker-scale')}
GO_SUITES = {
    'soak': ('integration,load', '^TestStreamingSoak', False),
    'fault-replicated': ('integration,fault,load', '^TestReplicatedZombieWorker$|^TestReplicatedBrokerFaults$|^TestReplicatedNetworkFaults$|^TestReplicatedOwnershipCases$', False),
}
EXPECTED = {
    'soak': ['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'],
    'fault-replicated': list(REPLICATED_CASES.values()),
}


def fault_timeout_catalog():
    catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
    standard = {'session.timeout.ms', 'delivery_timeout', 'shutdown_timeout'}
    ownership = standard | {'socket.timeout.ms', 'progress_save_timeout'}
    broker = ownership | {'replica.lag.time.max.ms'}
    def allowed(id):
        if id in REPLICATED_CASES and id.startswith('ownership/') and id != 'ownership/zombie-worker':
            return ownership | {'max.poll.interval.ms'} | ({'replica.lag.time.max.ms'} if id == 'ownership/coordinator-kill-rebalance' else set())
        return broker if id in REPLICATED_CASES and id.startswith('broker/') else ownership if id in REPLICATED_CASES else standard
    if not catalog or any(not budgets or set(budgets)-allowed(id) or
                          id in REPLICATED_CASES and set(budgets) != allowed(id) or
                          any(type(ms) is not int or ms <= 0 for ms in budgets.values())
                          for id, budgets in catalog.items()):
        raise ValueError('fault observation timeout catalog must contain positive milliseconds')
    return catalog


def replicated_execution_seconds(selected):
    catalog = fault_timeout_catalog()
    if not selected or len(selected) != len(set(selected)) or any(id not in REPLICATED_CASES or id not in catalog for id in selected):
        raise ValueError('unknown or unregistered replicated fault case')
    # Preserve the zombie's existing four-minute outer deadline. Each new
    # deterministic case adds its own complete configured recovery sum. The
    # network healthy reference grows from one session to thirty seconds.
    return sum(240 + (sum(catalog[id].values())+999)//1000 +
               (30-catalog[id]['session.timeout.ms']//1000 if id.startswith('network/') else 0)
               if id != 'ownership/zombie-worker' else 240 for id in selected)


