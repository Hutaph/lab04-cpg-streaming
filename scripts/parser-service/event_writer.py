import json
from pathlib import Path
from typing import Any

from topics import TOPICS


class EventWriter:
    def __init__(self, dry_run: bool, out_dir: Path, bootstrap_servers: str | None):
        self.dry_run = dry_run
        self.out_dir = out_dir
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

        if dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            from kafka import KafkaProducer

            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers or "localhost:9092",
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )

    def write_many(self, topic_key: str, events: list[dict[str, Any]]) -> None:
        if self.dry_run:
            path = self.out_dir / f"{topic_key}.jsonl"
            with path.open("a", encoding="utf-8") as handle:
                for event in events:
                    handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            return

        assert self.producer is not None
        topic = TOPICS[topic_key]
        for event in events:
            self.producer.send(topic, key=event["id"], value=event)

    def flush(self) -> None:
        if self.producer is not None:
            self.producer.flush()
