"""
Exercise 10 - Six fictive electricity-wattage sensor data sources.

Simulates one or more sensors producing readings and writes each reading as
its own Avro file to HDFS, partitioned by sensor/temporal_aspect/date.
"""

import argparse
import json
import os
import random
import re
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from hdfs.ext.avro import AvroWriter

from src.client import InsecureClient, get_hdfs_client


def get_uuid() -> str:
    return str(uuid4())


@dataclass
class SensorObj:
    sensor_id: str
    modality: float
    unit: str
    temporal_aspect: str


@dataclass
class PackageObj:
    payload: SensorObj
    correlation_id: str = field(default_factory=get_uuid)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: int = field(default=1)

    def to_dict(self) -> dict:
        return {
            "payload": json.dumps(asdict(self.payload)),
            "correlation_id": self.correlation_id,
            "created_at": self.created_at.timestamp(),
            "schema_version": self.schema_version,
        }


VALID_SENSOR_IDS: list[int] = [1, 2, 3, 4, 5, 6]
VALID_TEMPORAL_ASPECTS: list[str] = ["real_time", "edge_prediction"]
VALID_RANGE: tuple[int, int] = (-600, 600)
DEFAULT_INTERVAL_SECONDS: float = 1.0

SCHEMA = {
    "type": "record",
    "namespace": "default",
    "name": "SENSORPACKAGES",
    "fields": [
        {"name": "payload", "doc": "JSON-encoded sensor reading.", "type": "string"},
        {"name": "correlation_id", "doc": "UUID unique to this message.", "type": "string"},
        {"name": "created_at", "doc": "UTC timestamp of message creation.", "type": "double"},
        {"name": "schema_version", "doc": "Integer version of this message schema.", "type": "int"},
    ],
}


def get_sensor_sample(sensor_id: int) -> SensorObj:
    modality = random.uniform(VALID_RANGE[0], VALID_RANGE[1])
    return SensorObj(
        sensor_id=str(sensor_id),
        modality=modality,
        unit="MW",
        temporal_aspect="real_time",
    )


def get_filename(package: PackageObj, fmt: str = "avro") -> str:
    dt = package.created_at
    return (
        f"/data/raw/sensor_id={package.payload.sensor_id}"
        f"/temporal_aspect={package.payload.temporal_aspect}"
        f"/year={dt:%Y}/month={dt:%m}/day={dt:%d}"
        f"/{package.correlation_id}.{fmt}"
    )


def generate_sample(sensor_id: int, hdfs_client: InsecureClient) -> None:
    package = PackageObj(payload=get_sensor_sample(sensor_id=sensor_id))
    filename = get_filename(package)
    with AvroWriter(client=hdfs_client, hdfs_path=filename, schema=SCHEMA, overwrite=True) as writer:
        writer.write(package.to_dict())
    print(f"[sensor {sensor_id}] wrote {filename}")


class RepeatTimer(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def resolve_sensor_id(cli_value):
    if cli_value is not None:
        return cli_value
    env_value = os.environ.get("SENSOR_ID")
    if env_value is not None:
        return int(env_value)
    hostname = os.environ.get("HOSTNAME", "")
    match = re.search(r"-(\d+)$", hostname)
    if match:
        return int(match.group(1)) + 1
    return None


def run_single_sensor(sensor_id: int, interval: float) -> None:
    hdfs_client = get_hdfs_client()
    print(f"Starting sensor {sensor_id} at {interval}s interval (Ctrl+C to stop)")
    timer = RepeatTimer(interval, generate_sample, [sensor_id, hdfs_client])
    try:
        timer.start()
        while True:
            pass
    except KeyboardInterrupt:
        pass
    finally:
        timer.cancel()


def run_all_sensors(interval: float) -> None:
    hdfs_client = get_hdfs_client()
    print(f"Starting all {len(VALID_SENSOR_IDS)} sensors at {interval}s interval (Ctrl+C to stop)")
    timers = [RepeatTimer(interval, generate_sample, [sid, hdfs_client]) for sid in VALID_SENSOR_IDS]
    try:
        for timer in timers:
            timer.start()
        while True:
            pass
    except KeyboardInterrupt:
        pass
    finally:
        for timer in timers:
            timer.cancel()


def main():
    parser = argparse.ArgumentParser(description="Fictive electricity-wattage sensor data source")
    parser.add_argument("--sensor-id", type=int, choices=VALID_SENSOR_IDS, default=None)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS)
    args = parser.parse_args()

    sensor_id = resolve_sensor_id(args.sensor_id)
    if sensor_id is not None:
        run_single_sensor(sensor_id, args.interval)
    else:
        run_all_sensors(args.interval)


if __name__ == "__main__":
    main()
