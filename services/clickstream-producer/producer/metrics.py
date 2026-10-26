from prometheus_client import Counter, Gauge, start_http_server

EVENTS_SENT = Counter("producer_events_sent_total", "Events handed to the Kafka producer (failures counted in delivery_errors)", ["event_type"])
EVENTS_DLQ = Counter("producer_events_dlq_total", "Events routed to the DLQ", ["reason"])
DELIVERY_ERRORS = Counter("producer_delivery_errors_total", "Kafka delivery failures")
CHAOS = Counter("producer_chaos_injected_total", "Injected chaos records", ["kind"])
REPLAY_LAG = Gauge("producer_replay_lag_seconds", "How far the producer is behind its replay schedule")
SOURCE_ROWS = Counter("producer_source_rows_total", "Rows read from the REES46 file")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
