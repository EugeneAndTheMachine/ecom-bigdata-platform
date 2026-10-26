"""Entry point:  python -m producer [--input ...] [--speed 10] [--chaos] [--max-events N]

Pipeline per row:  CSV row -> validate/map (DLQ on failure) -> chaos -> pace -> Kafka (Avro)
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from dataclasses import replace

from producer import metrics
from producer.chaos import ChaosInjector, utc_now_ts
from producer.config import ProducerConfig
from producer.publisher import ClickstreamPublisher
from producer.rate_limiter import ReplayPacer
from producer.reader import iter_rows
from producer.transform import InvalidEvent, ReplayClock, to_record

log = logging.getLogger("producer")
_STOP = False


def _handle_stop(*_: object) -> None:
    global _STOP
    _STOP = True


def parse_args(cfg: ProducerConfig) -> ProducerConfig:
    p = argparse.ArgumentParser(description="Replay REES46 events into Kafka")
    p.add_argument("--input", default=cfg.input_path)
    p.add_argument("--speed", type=float, default=cfg.speed, help="1 = real time, 50 = 50x, 0 = max")
    p.add_argument("--time-mode", choices=["shift", "original"], default=cfg.time_mode)
    p.add_argument("--max-events", type=int, default=cfg.max_events)
    p.add_argument("--chaos", action="store_true", default=cfg.chaos_enabled)
    a = p.parse_args()
    return replace(cfg, input_path=a.input, speed=a.speed, time_mode=a.time_mode,
                   max_events=a.max_events, chaos_enabled=a.chaos)


def run(cfg: ProducerConfig) -> int:
    metrics.start_metrics_server(cfg.metrics_port)
    publisher = ClickstreamPublisher(cfg.bootstrap_servers, cfg.schema_registry_url,
                                     cfg.topic, cfg.dlq_topic, cfg.schema_path)
    clock = ReplayClock(speed=cfg.speed, time_mode=cfg.time_mode)
    pacer = ReplayPacer()
    chaos = ChaosInjector(cfg.chaos_late_ratio, cfg.chaos_dup_ratio, cfg.chaos_bot_burst_prob,
                          cfg.chaos_late_delay_s, seed=cfg.seed, enabled=cfg.chaos_enabled)
    log.info("replay start input=%s speed=%sx mode=%s chaos=%s", cfg.input_path, cfg.speed,
             cfg.time_mode, cfg.chaos_enabled)

    sent = 0
    started = time.monotonic()
    for row in iter_rows(cfg.input_path):
        if _STOP or (cfg.max_events and sent >= cfg.max_events):
            break
        metrics.SOURCE_ROWS.inc()
        try:
            record = to_record(row, clock)
        except InvalidEvent as bad:
            publisher.send_dlq(bad.row, bad.reason)
            continue

        pacer.wait_until(clock.scheduled_wall_time(record["original_event_time"]))
        metrics.REPLAY_LAG.set(pacer.lag_seconds)

        now_ts = utc_now_ts()
        for out in chaos.release_due(now_ts) + chaos.process(record, now_ts):
            if out.get("chaos_tag"):
                metrics.CHAOS.labels(out["chaos_tag"]).inc()
            publisher.send(out)
            sent += 1

        if sent and sent % 100_000 == 0:
            rate = sent / max(time.monotonic() - started, 1e-6)
            log.info("sent=%d rate=%.0f ev/s lag=%.1fs", sent, rate, pacer.lag_seconds)

    for out in chaos.drain():
        publisher.send(out)
    remaining = publisher.flush()
    log.info("replay done sent=%d unflushed=%d", sent, remaining)
    return 0 if remaining == 0 else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    return run(parse_args(ProducerConfig.from_env()))


if __name__ == "__main__":
    sys.exit(main())
