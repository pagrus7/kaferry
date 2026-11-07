#!/usr/bin/env bash
set -euo pipefail

work=$(mktemp -d /tmp/kaferry-demo.XXXXXX)
kafka=/opt/kafka/bin
broker_pid=
cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [[ -n "$broker_pid" ]]; then
        kill "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" 2>/dev/null || true
    fi
    if (( result != 0 )); then
        printf '\nDemo failed (exit %s). Kafka diagnostics:\n' "$result" >&2
        tail -n 40 "$work"/*.log >&2 || true
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'Starting a temporary Kafka broker...\n'
cat > "$work/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$work/data
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF
cluster_id=$("$kafka/kafka-storage.sh" random-uuid)
"$kafka/kafka-storage.sh" format -t "$cluster_id" -c "$work/server.properties" > "$work/broker.log" 2>&1
"$kafka/kafka-server-start.sh" "$work/server.properties" >> "$work/broker.log" 2>&1 &
broker_pid=$!
until timeout 10s "$kafka/kafka-topics.sh" --bootstrap-server localhost:9092 --list > "$work/setup.log" 2>&1; do
    kill -0 "$broker_pid"
    sleep 1
done

printf 'Creating orders and orders-copy; writing three sample records...\n'
for topic in orders orders-copy; do
    "$kafka/kafka-topics.sh" --bootstrap-server localhost:9092 --create \
        --topic "$topic" --partitions 1 --replication-factor 1 >> "$work/setup.log" 2>&1
done
printf 'order-1\norder-2\norder-3\n' > "$work/expected.txt"
"$kafka/kafka-console-producer.sh" --bootstrap-server localhost:9092 --topic orders \
    --sync --producer-property acks=all < "$work/expected.txt" >> "$work/setup.log" 2>&1

sql="WITH CONNECTION default AS ('bootstrap.servers' = 'localhost:9092')
INSERT INTO TOPIC 'orders-copy'
SELECT * FROM TOPIC 'orders' WITH (GROUP = 'demo')
WHERE RECORD_OFFSET < LATEST;"
printf '\nRunning Kaferry:\n%s\n\n' "$sql"
kaferry run -e "$sql"

printf '\nReading orders-copy:\n'
"$kafka/kafka-console-consumer.sh" --bootstrap-server localhost:9092 --topic orders-copy \
    --partition 0 --offset earliest --max-messages 3 --timeout-ms 10000 \
    > "$work/actual.txt" 2> "$work/consumer.log"
cat "$work/actual.txt"
diff -u "$work/expected.txt" "$work/actual.txt"
end=$("$kafka/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic orders-copy --time -1 2> "$work/offsets.log")
[[ "$end" == 'orders-copy:0:3' ]]
printf '\nDemo passed: verified all 3 records in orders-copy.\n'
