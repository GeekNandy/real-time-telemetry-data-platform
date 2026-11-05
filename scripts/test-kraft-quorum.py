#!/usr/bin/env python3
"""Validate the merged six-node Kafka Compose configuration without starting containers."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
VOTERS = "101@kafka-controller-1:9093,102@kafka-controller-2:9093,103@kafka-controller-3:9093"
CONTROLLERS = {f"kafka-controller-{n}": str(100 + n) for n in (1, 2, 3)}
BROKERS = {"kafka": "1", "kafka2": "2", "kafka3": "3"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    env = os.environ.copy()
    # These values are for interpolation only; this script never starts containers.
    env.setdefault("DEMO_JWT_SECRET", "kraft-static-contract-not-for-runtime")
    env.setdefault("DATABASE_PASSWORD", "kraft-static-contract-not-for-runtime")
    command = [
        "docker", "compose", "-p", "telemetry-kraft-contract",
        "-f", "compose.yaml", "-f", "compose.replicated.yaml",
        "-f", "compose.kraft-quorum.yaml", "config", "--format", "json",
    ]
    model = json.loads(subprocess.check_output(command, cwd=ROOT, env=env, text=True))
    services = model["services"]
    require(set(CONTROLLERS) <= services.keys(), "Missing controller services")
    require(set(BROKERS) <= services.keys(), "Missing broker services")
    for service, node_id in CONTROLLERS.items():
        definition = services[service]
        settings = definition["environment"]
        require(settings["KAFKA_NODE_ID"] == node_id, f"{service}: node ID")
        require(settings["KAFKA_PROCESS_ROLES"] == "controller", f"{service}: role")
        require(settings["KAFKA_CONTROLLER_QUORUM_VOTERS"] == VOTERS, f"{service}: voters")
        require(settings["KAFKA_CONTROLLER_LISTENER_NAMES"] == "CONTROLLER", f"{service}: listener")
        require(settings["KAFKA_LISTENERS"] == "CONTROLLER://:9093", f"{service}: listener binding")
        require(not definition.get("ports"), f"{service}: controller port must not be published")
        require(definition["image"] == "apache/kafka:4.0.0", f"{service}: pinned Kafka image")
        require(definition.get("healthcheck"), f"{service}: missing healthcheck")
        mounts = [v["target"] for v in definition["volumes"]]
        require("/var/lib/kafka/data" in mounts, f"{service}: persistent metadata missing")
    volumes = [
        next(v["source"] for v in services[name]["volumes"] if v["target"] == "/var/lib/kafka/data")
        for name in CONTROLLERS
    ]
    require(len(set(volumes)) == 3, "KRaft controllers must not share storage")

    for service, node_id in BROKERS.items():
        definition = services[service]
        settings = definition["environment"]
        require(str(settings["KAFKA_NODE_ID"]) == node_id, f"{service}: broker node ID")
        require(settings["KAFKA_PROCESS_ROLES"] == "broker", f"{service}: broker role")
        require(settings["KAFKA_CONTROLLER_QUORUM_VOTERS"] == VOTERS, f"{service}: voters")
        require(str(settings["KAFKA_MIN_INSYNC_REPLICAS"]) == "2", f"{service}: minISR")
        require(str(settings["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"]) == "3",
                f"{service}: consumer offset replication")
        require(str(settings["KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE"]).lower() == "false",
                f"{service}: unclean election enabled")
        require("CONTROLLER://" not in settings["KAFKA_LISTENERS"],
                f"{service}: controller listener must not be bound")
        require(set(CONTROLLERS) <= definition["depends_on"].keys(), f"{service}: startup dependencies")
    require("kafka:9092,kafka2:9092,kafka3:9092" ==
            services["collector"]["environment"]["KAFKA_BOOTSTRAP_SERVERS"],
            "Collector must retain three bootstrap brokers")
    require("kafka:9092,kafka2:9092,kafka3:9092" ==
            services["worker"]["environment"]["KAFKA_BOOTSTRAP_SERVERS"],
            "Worker must retain three bootstrap brokers")
    print("KRaft static quorum contract: three private controllers, three replicated brokers, separate volumes")


if __name__ == "__main__":
    main()
