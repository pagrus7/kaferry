"""Created-topic visibility must converge within the original setup budget."""

from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from tools.qualification.broker_probe import wait_topic_ready


class KafkaError:
    UNKNOWN_TOPIC_OR_PART = 3
    LEADER_NOT_AVAILABLE = 5

    def __init__(self, code):
        self.value = code

    def code(self):
        return self.value


class KafkaException(Exception):
    pass


class ConfigResource:
    Type = SimpleNamespace(TOPIC=2)

    def __init__(self, kind, name):
        self.kind, self.name = kind, name


class Clock:
    def __init__(self):
        self.now, self.sleeps = 0.0, []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def metadata(**change):
    part = dict(error=None, leader=1, replicas=[1, 2, 3], isrs=[1, 2, 3])
    part.update(change)
    return SimpleNamespace(error=None, partitions={0: SimpleNamespace(**part)})


class Admin:
    def __init__(self, clock, topics=None, configs=None):
        self.clock = clock
        self.topics = list(topics if topics is not None else [metadata()])
        self.configs = list(configs if configs is not None else ['2'])
        self.calls = []
        self.metadata_delay = self.config_delay = self.future_delay = 0

    def list_topics(self, name, timeout):
        self.calls.append(('metadata', timeout))
        self.clock.now += self.metadata_delay
        row = self.topics.pop(0) if len(self.topics) > 1 else self.topics[0]
        return SimpleNamespace(topics={name: row})

    def describe_configs(self, resources, request_timeout):
        self.calls.append(('config', request_timeout))
        self.clock.now += self.config_delay
        value = self.configs.pop(0) if len(self.configs) > 1 else self.configs[0]

        def result(timeout):
            self.calls.append(('future', timeout))
            self.clock.now += self.future_delay
            if isinstance(value, Exception):
                raise value
            return {'min.insync.replicas': SimpleNamespace(value=value)}

        return {resources[0]: SimpleNamespace(result=result)}


class TopicReadinessTest(unittest.TestCase):
    def setUp(self):
        kafka, admin = ModuleType('confluent_kafka'), ModuleType('confluent_kafka.admin')
        kafka.KafkaError, kafka.KafkaException = KafkaError, KafkaException
        admin.ConfigResource = ConfigResource
        modules = patch.dict('sys.modules', {'confluent_kafka': kafka, 'confluent_kafka.admin': admin})
        modules.start()
        self.addCleanup(modules.stop)
        self.clock = Clock()
        timing = patch('tools.qualification.broker_probe.time', self.clock)
        timing.start()
        self.addCleanup(timing.stop)
        self.observations = []

    def wait(self, admin, replicated=True):
        wait_topic_ready(admin, 'created-topic', 1, replicated, self.observations)

    def test_config_visibility_can_lag_already_ready_metadata(self):
        # Given creation succeeded and metadata has RF3/ISR3, another broker
        # can still report UNKNOWN_TOPIC_OR_PART when reading configuration.
        admin = Admin(self.clock, configs=[KafkaException(KafkaError(3)), '2'])
        # When visibility converges, observe the same created topic again.
        self.wait(admin)
        # Then retain the transient code and exact successful configuration.
        observed = self.observations[0]
        self.assertEqual('ready', observed['status'])
        self.assertEqual(['config-not-visible', 'ready'], [row['state'] for row in observed['attempts']])
        self.assertEqual(3, observed['attempts'][0]['error_code'])
        self.assertEqual('2', observed['attempts'][1]['min_insync_replicas'])
        self.assertEqual(['metadata', 'config', 'future'] * 2, [kind for kind, _ in admin.calls])
        self.assertEqual([0.1], self.clock.sleeps)

    def test_rf_isr_and_leader_must_all_be_ready_before_config_query(self):
        # Given incomplete replica/ISR/leader metadata, no config success can admit it.
        admin = Admin(self.clock, topics=[metadata(replicas=[1]), metadata(isrs=[1, 2]),
                                          metadata(leader=-1), metadata()])
        # When metadata converges, wait for every required topology property.
        self.wait(admin)
        # Then the first three observations remain pending; only exact topology passes.
        self.assertEqual(['metadata-pending'] * 3 + ['ready'],
                         [row['state'] for row in self.observations[0]['attempts']])
        self.assertEqual(['metadata'] * 4 + ['config', 'future'], [kind for kind, _ in admin.calls])

    def test_metadata_visibility_errors_are_observed_without_config_queries(self):
        # Given stale topic visibility and a leader election during creation.
        absent = metadata()
        absent.error = KafkaError(3)
        admin = Admin(self.clock, topics=[absent, metadata(error=KafkaError(5)), metadata()])
        # When subsequent observations converge, retain those setup transitions.
        self.wait(admin)
        # Then only those metadata states are polled, retaining their exact codes.
        attempts = self.observations[0]['attempts']
        self.assertEqual([[3], [5]], [row['metadata_error_codes'] for row in attempts[:2]])
        self.assertEqual(['metadata'] * 3 + ['config', 'future'], [kind for kind, _ in admin.calls])

    def test_wrong_min_isr_and_other_errors_fail_immediately(self):
        # Given a wrong configuration or any error other than config visibility.
        for value in ['1', '3', None, KafkaException(KafkaError(5)), KafkaException(KafkaError(29)),
                      KafkaException(KafkaError(40)), TimeoutError('expired'), RuntimeError('unexpected')]:
            with self.subTest(value=value):
                admin = Admin(self.clock, configs=[value])
                # When setup checks that configuration, the failure propagates.
                if isinstance(value, Exception):
                    with self.assertRaises(type(value)) as raised:
                        self.wait(admin)
                    self.assertIs(value, raised.exception)
                else:
                    with self.assertRaisesRegex(ValueError, 'min.insync.replicas=2'):
                        self.wait(admin)
                # Then no retry, delay, topic recreation or weakened config gate occurs.
                self.assertEqual(['metadata', 'config', 'future'], [kind for kind, _ in admin.calls])
                self.assertEqual([], self.clock.sleeps)

    def test_metadata_authorization_and_other_errors_are_not_polled(self):
        # Given a metadata failure that cannot be explained by new-topic visibility.
        for code in [29, 40, -195]:
            with self.subTest(code=code):
                row = metadata()
                row.error = KafkaError(code)
                admin = Admin(self.clock, topics=[row])
                # When metadata is queried, the error fails setup immediately.
                with self.assertRaises(KafkaException) as raised:
                    self.wait(admin)
                # Then no config request or polling delay follows that error.
                self.assertEqual(code, raised.exception.args[0].code())
                self.assertEqual(['metadata'], [kind for kind, _ in admin.calls])
                self.assertEqual([], self.clock.sleeps)

    def test_each_blocking_call_uses_remaining_original_budget(self):
        # Given time spent in each stage, later calls inherit only the remaining budget.
        admin = Admin(self.clock)
        admin.metadata_delay, admin.config_delay, admin.future_delay = 9, 11, 3
        # When each stage consumes time, the next receives the remaining allowance.
        self.wait(admin)
        # Then no blocking call starts a new 30-second window.
        self.assertEqual([('metadata', 10), ('config', 21), ('future', 10)], admin.calls)
        self.assertEqual(23, self.observations[0]['elapsed_seconds'])
        # When a future returns after the budget, then even valid configuration fails.
        admin = Admin(self.clock)
        admin.future_delay = 31
        with self.assertRaisesRegex(ValueError, 'within 30s'):
            self.wait(admin)
        self.assertEqual('failed', self.observations[-1]['status'])

    def test_visibility_failure_exhausts_same_window_without_new_operation_budget(self):
        # Given config visibility never converges, retain every attempted observation.
        admin = Admin(self.clock, configs=[KafkaException(KafkaError(3))])
        # When the original window expires, setup fails without another operation.
        with self.assertRaisesRegex(ValueError, 'within 30s'):
            self.wait(admin)
        observed = self.observations[0]
        # Then the readiness clock is not reset, and every wait fits the original window.
        self.assertAlmostEqual(30, self.clock.now)
        self.assertEqual(30, observed['budget_seconds'])
        self.assertEqual('failed', observed['status'])
        self.assertTrue(observed['attempts'])
        self.assertTrue(all(row['error_code'] == 3 for row in observed['attempts']))
        self.assertTrue(all(0 < timeout <= 30 for _, timeout in admin.calls))
        self.assertEqual(3 * len(observed['attempts']), len(admin.calls))

    def test_plain_fixture_needs_metadata_without_replicated_configuration(self):
        # Given an ordinary fixture, only its metadata gate is required.
        admin = Admin(self.clock, topics=[metadata(replicas=[1], isrs=[1])])
        # When that metadata is ready, admit the topic.
        self.wait(admin, replicated=False)
        # Then no replicated configuration query is performed.
        self.assertEqual([('metadata', 10)], admin.calls)
        self.assertEqual('ready', self.observations[0]['status'])


if __name__ == '__main__':
    unittest.main()
