# Local stack: MQTT → Redpanda → xstate-kafka

A single-node stack to try the service end to end on your machine:

```
devices ──MQTT──> mosquitto ──> Redpanda Connect ──> sensor-events ──> xstate-kafka ──> sensor-out
                                (../connect/mqtt.yaml)               (Redpanda)  └──> sensor-changelog
```

The demo machine (`machine/sensor.xstate.json`) is a sensor with three states:

| State | `reading.high` | `reading.normal` | After 30 s with no reading |
|---|---|---|---|
| `ok` | → `alarm`, emits `alarm.raised` | stays | → `offline`, emits `device.offline` |
| `alarm` | stays | → `ok`, emits `alarm.cleared` | → `offline`, emits `device.offline` |
| `offline` | → `alarm`, emits `device.online`, `alarm.raised` | → `ok`, emits `device.online` | |

Events carry `seq` and an optional `boot`. The service config (`service.json`) turns on `dedupe` with them, so redelivered and stale events are skipped.

## Run it

You need Docker with Compose v2 (BuildKit), `curl`, and ports 1883, 4195, 9464, 9644 and 19092 free.

```sh
docker compose up -d --build   # the first build compiles librdkafka: a few minutes
./smoke.sh --no-up             # or just ./smoke.sh, which also does the up
```

`smoke.sh` publishes readings for two fresh devices over MQTT, including a redelivered event and one from before a reboot. It then checks:

- the four expected events on `sensor-out`;
- no dead letters for those devices;
- `xstate_duplicates_total` grew by 2.

It prints `OK` or what differed.

Try it by hand:

```sh
docker compose exec mosquitto mosquitto_pub -t devices/kitchen/events -q 1 \
  -m '{"type":"reading.high","value":31.5,"seq":1}'
docker compose exec redpanda rpk topic consume sensor-out --read-committed -f '%k %v\n'
curl -s localhost:9464/metrics | grep xstate_
```

## Ports

| Port | Service |
|---|---|
| 1883 | MQTT (mosquitto, anonymous) |
| 19092 | Kafka API for clients on the host (`rpk -X brokers=localhost:19092`) |
| 9644 | Redpanda admin API |
| 4195 | Redpanda Connect (`/ready`, `/metrics`) |
| 9464 | xstate-kafka Prometheus metrics |

## Protobuf events

1. Generate the device-side schema:
   ```sh
   docker compose run --rm --no-deps xstate proto events /etc/xstate/service.json > sensor.proto
   ```
   or, from the repository, `cargo run -p xstate-kafka --features kafka -- proto events crates/xstate-kafka/deploy/service.json`.
2. Have devices publish encoded `Event` messages.
3. Restart the bridge with the matching content type:
   ```sh
   CONTENT_TYPE=application/x-protobuf docker compose up -d connect
   ```

The service picks the decoder from each record's `content-type` header, so JSON and protobuf devices can share the input topic. To do that, run one bridge per format, each subscribed to its own MQTT topics.

## Tear down

```sh
docker compose down      # keep the topics and the service's checkpoints
docker compose down -v   # also delete the Redpanda data and checkpoints
```

## Notes

- **Restore.** The service restores actors from `sensor-changelog`, a compacted topic, plus the local checkpoints in the `xstate` volume. Restart it with `docker compose restart xstate` and an actor continues where it left off.
- **Debug tracing.** `XSTATE_KAFKA_DEBUG=1 docker compose up -d xstate` traces each batch in `docker compose logs -f xstate`.
- **Image versions.** The tags are pinned: Redpanda v26.2.6, Redpanda Connect 4.115.0, Mosquitto 2.0.22, and Rust 1.97 for the build.
