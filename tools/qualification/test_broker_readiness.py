"""Created topics must expose the required topology and configuration."""

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


if __name__ == '__main__':
    unittest.main()
