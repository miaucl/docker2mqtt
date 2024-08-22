#!/usr/bin/env python3
"""Listens to docker events and stats for containers and sends it to mqtt and supports discovery for home assistant."""

import datetime
import hashlib
import json
import logging
import platform
from queue import Empty, Queue
import re
import subprocess
from subprocess import PIPE, Popen
import sys
from threading import Thread
from time import sleep, time
from typing import Any, Dict, Tuple

import paho.mqtt.client

from .const import (
    ANSI_ESCAPE,
    DESTROYED_CONTAINER_TTL_DEFAULT,
    DOCKER_EVENTS_CMD,
    DOCKER_STATS_CMD,
    DOCKER_VERSION_CMD,
    EVENTS_DEFAULT,
    HOMEASSISTANT_PREFIX_DEFAULT,
    INVALID_HA_TOPIC_CHARS,
    LOG_LEVEL_DEFAULT,
    MAX_QUEUE_SIZE,
    MQTT_CLIENT_ID_DEFAULT,
    MQTT_PORT_DEFAULT,
    MQTT_QOS_DEFAULT,
    MQTT_TIMEOUT_DEFAULT,
    MQTT_TOPIC_PREFIX_DEFAULT,
    STATS_DEFAULT,
    STATS_RECORD_SECONDS_DEFAULT,
    STATS_REGISTRATION_ENTRIES,
    WATCHED_EVENTS,
)
from .type_definitions import (
    ContainerDeviceEntry,
    ContainerEvent,
    ContainerStats,
    ContainerStatsRef,
    Docker2MqttConfig,
)

# Default config

DEFAULT_CONFIG = Docker2MqttConfig(
    {
        "log_level": LOG_LEVEL_DEFAULT,
        "destroyedContainerTTL": DESTROYED_CONTAINER_TTL_DEFAULT,
        "homeassistant_prefix": HOMEASSISTANT_PREFIX_DEFAULT,
        "docker2mqtt_hostname": "docker2mqtt-host",
        "mqtt_client_id": MQTT_CLIENT_ID_DEFAULT,
        "mqtt_user": "",
        "mqtt_password": "",
        "mqtt_host": "",
        "mqtt_port": MQTT_PORT_DEFAULT,
        "mqtt_timeout": MQTT_TIMEOUT_DEFAULT,
        "mqtt_topic_prefix": MQTT_TOPIC_PREFIX_DEFAULT,
        "mqtt_qos": MQTT_QOS_DEFAULT,
        "enable_events": EVENTS_DEFAULT,
        "enable_stats": STATS_DEFAULT,
        "stars_recording_seconds": STATS_RECORD_SECONDS_DEFAULT,
    }
)


# Configure logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Loggers
main_logger = logging.getLogger("main")
events_logger = logging.getLogger("events")
stats_logger = logging.getLogger("main")


class Docker2Mqtt:
    """docker2mqtt class."""

    # Version
    __version__ = "2.0.0-rc.0"

    cfg: Docker2MqttConfig

    b_stats = False
    b_events = False

    docker_events: Queue[str] = Queue(maxsize=MAX_QUEUE_SIZE)
    docker_stats: Queue[str] = Queue(maxsize=MAX_QUEUE_SIZE)
    known_event_containers: Dict[str, ContainerEvent] = {}
    known_stat_containers: Dict[str, ContainerStatsRef] = {}
    last_stat_containers: Dict[str, ContainerStats | Dict[str, Any]] = {}
    pending_destroy_operations: Dict[str, float] = {}

    mqtt: paho.mqtt.client.Client

    docker_events_t: Thread
    docker_stats_t: Thread

    docker_version: str

    discovery_binary_sensor_topic: str
    discovery_sensor_topic: str
    status_topic: str
    version_topic: str
    stats_topic: str
    events_topic: str

    def __init__(self, cfg: Docker2MqttConfig):
        """Initialize the docker2mqtt."""

        self.cfg = cfg

        self.discovery_binary_sensor_topic = f"{cfg['homeassistant_prefix']}/binary_sensor/{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}_{{}}/config"
        self.discovery_sensor_topic = f"{cfg['homeassistant_prefix']}/sensor/{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}_{{}}/config"
        self.status_topic = (
            f"{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}/status"
        )
        self.version_topic = (
            f"{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}/version"
        )
        self.stats_topic = (
            f"{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}/{{}}/stats"
        )
        self.events_topic = (
            f"{cfg['mqtt_topic_prefix']}/{cfg['docker2mqtt_hostname']}/{{}}/events"
        )

        if self.cfg["enable_events"]:
            self.b_events = True
        if self.cfg["enable_stats"]:
            self.b_stats = True

        main_logger.setLevel(self.cfg["log_level"].upper())
        events_logger.setLevel(self.cfg["log_level"].upper())
        stats_logger.setLevel(self.cfg["log_level"].upper())

        self.docker_version = self.get_docker_version()

        main_logger.info("Events enabled: %d", self.b_events)
        main_logger.info("Stats enabled: %d", self.b_stats)

        # Setup MQTT
        self.mqtt = paho.mqtt.client.Client(
            paho.mqtt.client.CallbackAPIVersion.VERSION2,  # type: ignore
            client_id=self.cfg["mqtt_client_id"],
        )
        self.mqtt.username_pw_set(
            username=self.cfg["mqtt_user"], password=self.cfg["mqtt_password"]
        )
        self.mqtt.will_set(
            self.status_topic,
            "offline",
            qos=self.cfg["mqtt_qos"],
            retain=True,
        )
        self.mqtt.connect(
            self.cfg["mqtt_host"], self.cfg["mqtt_port"], self.cfg["mqtt_timeout"]
        )
        self.mqtt.loop_start()
        self.mqtt_send(self.status_topic, "online", retain=True)

        started = False
        if self.b_events:
            logging.info("Starting Events thread")
            self.docker_events_t = Thread(
                target=self.readline_events_thread, daemon=True, name="Events"
            )
            self.docker_events_t.start()
            started = True

        if self.b_stats:
            started = True
            logging.info("Starting Stats thread")
            self.docker_stats_t = Thread(
                target=self.readline_stats_thread, daemon=True, name="Stats"
            )
            self.docker_stats_t.start()

        if started is False:
            logging.critical("Nothing started, check your config!")
            sys.exit(1)

    def __del__(self) -> None:
        """Stop everything."""
        self.mqtt_disconnect()

    def loop(self) -> None:
        """Start the loop."""

        self.remove_destroyed_containers()

        self.handle_events_queue()

        self.handle_stats_queue()

        if self.b_events and not self.docker_events_t.is_alive():
            main_logger.warning("Restarting events thread")
            self.docker_events_t.start()

        if self.b_stats and not self.docker_stats_t.is_alive():
            main_logger.warning("Restarting stats thread")
            self.docker_stats_t.start()

    def loop_busy(self) -> None:
        """Start the loop (blocking)."""

        while True:
            self.loop()

            # Calculate next iteration between (~0.2s and 0.001s)
            sleep_time = 0.001 + 0.2 / MAX_QUEUE_SIZE * (
                MAX_QUEUE_SIZE
                - max(self.docker_events.qsize(), self.docker_stats.qsize())
            )
            main_logger.debug("Sleep for %f.5fs until next iteration", sleep_time)
            sleep(sleep_time)

    def mqtt_disconnect(self) -> None:
        """Make sure we send our last_will message with atexit."""
        self.mqtt.publish(
            self.status_topic,
            "offline",
            qos=self.cfg["mqtt_qos"],
            retain=True,
        )
        self.mqtt.publish(
            self.version_topic,
            self.__version__,
            qos=self.cfg["mqtt_qos"],
            retain=True,
        )
        self.mqtt.disconnect()
        sleep(1)
        self.mqtt.loop_stop()

    def get_docker_version(self) -> str:
        """Get the docker version and save it to a global value."""
        try:
            # Run the `docker --version` command
            result = subprocess.run(
                DOCKER_VERSION_CMD,
                capture_output=True,
                text=True,
                check=False,
            )

            # Check if the command was successful
            if result.returncode == 0:
                # Extract the version information from the output
                return result.stdout.strip()
            else:
                raise Exception(f"Error: {result.stderr.strip()}")
        except FileNotFoundError:
            return "Docker is not installed or not found in PATH."

    def mqtt_send(self, topic: str, payload: str, retain: bool = False) -> None:
        """Send a mqtt payload to for a topic."""
        try:
            main_logger.debug("Sending to MQTT: %s: %s", topic, payload)
            self.mqtt.publish(
                topic, payload=payload, qos=self.cfg["mqtt_qos"], retain=retain
            )

        except Exception as e:
            main_logger.error("MQTT Publish Failed: %s", str(e))

    def readline_events_thread(self) -> None:
        """Run docker events and continually read lines from it."""
        thread_logger = logging.getLogger("event-thread")
        thread_logger.setLevel(self.cfg["log_level"].upper())
        try:
            thread_logger.info("Starting events thread")
            thread_logger.debug("Command: %s", DOCKER_EVENTS_CMD)
            with Popen(DOCKER_EVENTS_CMD, stdout=PIPE, text=True) as process:
                while True:
                    if process.stdout:
                        line = ANSI_ESCAPE.sub("", process.stdout.readline())
                        if line == "" and process.poll() is not None:
                            break
                        if line:
                            thread_logger.debug("Read docker event line: %s", line)
                            self.docker_events.put(line.strip())
                        _rc = process.poll()
                    else:
                        raise ReferenceError("process stdout is undefined")
        except Exception as ex:
            thread_logger.debug("Error Running Events thread:  %s", str(ex))

    def readline_stats_thread(self) -> None:
        """Run docker events and continually read lines from it."""
        thread_logger = logging.getLogger("stats-thread")
        thread_logger.setLevel(self.cfg["log_level"].upper())
        try:
            thread_logger.info("Starting stats thread")
            thread_logger.debug("Command: %s", DOCKER_STATS_CMD)
            with Popen(DOCKER_STATS_CMD, stdout=PIPE, text=True) as process:
                while True:
                    if process.stdout:
                        line = ANSI_ESCAPE.sub("", process.stdout.readline())
                        if line == "" and process.poll() is not None:
                            break
                        if line:
                            thread_logger.debug("Read docker stat line: %s", line)
                            self.docker_stats.put(line.strip())
                        _rc = process.poll()
                    else:
                        raise ReferenceError("process stdout is undefined")
        except Exception as ex:
            thread_logger.debug("Error Running Stats thread: %s", str(ex))

    def device_definition(
        self, container_entry: ContainerEvent
    ) -> ContainerDeviceEntry:
        """Create device definition of a container for each entity for home assistant."""
        container = container_entry["name"]
        return {
            "identifiers": f"{self.cfg['docker2mqtt_hostname']}_{self.cfg['mqtt_topic_prefix']}_{container}",
            "name": f"{self.cfg['docker2mqtt_hostname']} {self.cfg['mqtt_topic_prefix'].title()} {container}",
            "model": f"{platform.system()} {platform.machine()} {self.docker_version}",
        }

    def register_container(self, container_entry: ContainerEvent) -> None:
        """Create discovery topics of container for all entities for home assistant."""
        container = container_entry["name"]
        self.known_event_containers[container] = container_entry

        # Events
        registration_topic = self.discovery_binary_sensor_topic.format(
            INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_events")
        )
        events_topic = self.events_topic.format(container)
        registration_packet = {
            "name": "Events",
            "unique_id": f"{self.cfg['mqtt_topic_prefix']}_{self.cfg['docker2mqtt_hostname']}_{registration_topic}",
            "availability_topic": f"{self.cfg['mqtt_topic_prefix']}/{self.cfg['docker2mqtt_hostname']}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
            "state_topic": events_topic,
            "value_template": '{{ value_json.state if value_json is not undefined and value_json.state is not undefined else "off" }}',
            "payload_on": "on",
            "payload_off": "off",
            "device": self.device_definition(container_entry),
            "device_class": "running",
            "json_attributes_topic": events_topic,
            "qos": self.cfg["mqtt_qos"],
        }
        self.mqtt_send(registration_topic, json.dumps(registration_packet), retain=True)
        self.mqtt_send(
            events_topic,
            json.dumps(container_entry),
            retain=True,
        )

        # Stats
        for label, field, device_class, unit, icon in STATS_REGISTRATION_ENTRIES:
            registration_topic = self.discovery_sensor_topic.format(
                INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_{field}_stats")
            )
            stats_topic = self.stats_topic.format(container)
            registration_packet = {
                "name": label,
                "unique_id": f"{self.cfg['mqtt_topic_prefix']}_{self.cfg['docker2mqtt_hostname']}_{registration_topic}",
                "availability_topic": f"{self.cfg['mqtt_topic_prefix']}/{self.cfg['docker2mqtt_hostname']}/status",
                "payload_available": "online",
                "payload_not_available": "offline",
                "state_topic": stats_topic,
                "value_template": f"{{{{ value_json.{ field } if value_json is not undefined and value_json.{ field } is not undefined else None }}}}",
                "unit_of_measurement": unit,
                "icon": icon,
                "device_class": device_class,
                "device": self.device_definition(container_entry),
                "qos": self.cfg["mqtt_qos"],
            }
            self.mqtt_send(
                registration_topic, json.dumps(registration_packet), retain=True
            )
            self.mqtt_send(
                stats_topic,
                json.dumps({}),
                retain=True,
            )

    def unregister_container(self, container: str) -> None:
        """Remove all discovery topics of container from home assistant."""

        # Events
        self.mqtt_send(
            self.discovery_binary_sensor_topic.format(
                INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_events")
            ),
            "",
            retain=True,
        )
        self.mqtt_send(
            self.events_topic.format(container),
            "",
            retain=True,
        )

        # Stats
        for _, field, _, _, _ in STATS_REGISTRATION_ENTRIES:
            self.mqtt_send(
                self.discovery_sensor_topic.format(
                    INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_{field}_stats")
                ),
                "",
                retain=True,
            )
        self.mqtt_send(
            self.stats_topic.format(container),
            "",
            retain=True,
        )

    def stat_to_value(
        self, stat: str, container: str, matches: re.Match[str] | None
    ) -> Tuple[float, float]:
        """Convert a regex matches to two values, i.e. used and limit for memory."""
        used_symbol = ""
        limit_symbol = ""
        used = 0.0
        limit = 0.0

        if matches is None:
            stats_logger.debug("%s: %s No matching regex, returning 0", stat, container)
            used = 0
            limit = 0
            return used, limit

        stats_logger.debug(
            "%s: %s Found matching regex, getting used symbol", stat, container
        )
        used_symbol = matches.group("used_symbol")
        stats_logger.debug("%s: %s Used Symbol %s", stat, container, used_symbol)
        limit_symbol = matches.group("limit_symbol")
        stats_logger.debug("%s: %s Limit Symbol %s", stat, container, limit_symbol)
        used = float(matches.group("used"))
        if used_symbol == "GiB":
            used = used * 1024
        if used_symbol == "KiB":
            used = used / 1024
        if used_symbol == "TB":
            used = used * 1024 * 1024
        if used_symbol == "GB":
            used = used * 1024
        if used_symbol == "kB":
            used = used / 1024
        if used_symbol == "B":
            used = used / 1024 / 1024
        if used_symbol == "B":
            used = used / 1024 / 1024
        stats_logger.debug("%s: %s Used %f Mb", stat, container, used)
        limit = float(matches.group("limit"))
        if limit_symbol == "GiB":
            limit = limit * 1024
        if limit_symbol == "KiB":
            limit = limit / 1024
        if limit_symbol == "GB":
            limit = limit * 1024
        if limit_symbol == "TB":
            limit = limit * 1024 * 1024
        if limit_symbol == "kB":
            limit = limit / 1024
        if limit_symbol == "B":
            limit = limit / 1024 / 1024
        stats_logger.debug("%s: %s Limit %f Mb", stat, container, limit)
        stats_logger.debug(
            "%s: Stat for container %s is %f.1f MB from %f.1f MB",
            stat,
            container,
            used,
            limit,
        )
        return used, limit

    def remove_destroyed_containers(self) -> None:
        """Remove any destroyed containers that have passed the TTL."""
        for container, destroyed_at in self.pending_destroy_operations.copy().items():
            if time() - destroyed_at > self.cfg["destroyedContainerTTL"]:
                main_logger.info("Removing container %s from MQTT.", container)
                self.unregister_container(container)
                del self.pending_destroy_operations[container]

    def handle_events_queue(self) -> None:
        """Check if any event is present in the queue and process it."""
        event_line = ""

        docker_events_qsize = self.docker_events.qsize()
        try:
            if self.b_events:
                event_line = self.docker_events.get(block=False)
            events_logger.info("Events queue length: %s", docker_events_qsize)
        except Empty:
            # No data right now, just move along.
            return

        if self.b_events and docker_events_qsize > 0:
            try:
                if event_line and len(event_line) > 0:
                    event = json.loads(event_line)
                    if event["status"] not in WATCHED_EVENTS:
                        events_logger.info("Not a watched event: %s", event["status"])
                        return

                    container: str = event["Actor"]["Attributes"]["name"]
                    events_logger.debug(
                        "Have an event to process for Container name: %s", container
                    )

                    if event["status"] == "create":
                        # Cancel any previous pending destroys and add this to known_event_containers.
                        events_logger.info("Container %s has been created.", container)
                        if container in self.pending_destroy_operations:
                            events_logger.debug(
                                "Removing pending delete for %s.", container
                            )
                            del self.pending_destroy_operations[container]

                        self.register_container(
                            {
                                "name": container,
                                "image": event["from"],
                                "status": "created",
                                "state": "off",
                            }
                        )

                    elif event["status"] == "destroy":
                        # Add this container to pending_destroy_operations.
                        events_logger.info(
                            "Container %s has been destroyed.", container
                        )
                        self.pending_destroy_operations[container] = time()
                        self.known_event_containers[container]["status"] = "destroyed"
                        self.known_event_containers[container]["state"] = "off"

                    elif event["status"] == "die":
                        events_logger.info("Container %s has stopped.", container)
                        self.known_event_containers[container]["status"] = "stopped"
                        self.known_event_containers[container]["state"] = "off"

                    elif event["status"] == "pause":
                        events_logger.info("Container %s has paused.", container)
                        self.known_event_containers[container]["status"] = "paused"
                        self.known_event_containers[container]["state"] = "off"

                    elif event["status"] == "rename":
                        old_name = event["Actor"]["Attributes"]["oldName"]
                        if old_name.startswith("/"):
                            old_name = old_name[1:]
                        events_logger.info(
                            "Container %s renamed to %s.", old_name, container
                        )
                        self.unregister_container(old_name)
                        self.register_container(
                            {
                                "name": container,
                                "image": self.known_event_containers[old_name]["image"],
                                "status": self.known_event_containers[old_name][
                                    "status"
                                ],
                                "state": self.known_event_containers[old_name]["state"],
                            }
                        )
                        del self.known_event_containers[old_name]

                    elif event["status"] == "start":
                        events_logger.info("Container %s has started.", container)
                        self.known_event_containers[container]["status"] = "running"
                        self.known_event_containers[container]["state"] = "on"

                    elif event["status"] == "unpause":
                        events_logger.info("Container %s has unpaused.", container)
                        self.known_event_containers[container]["status"] = "running"
                        self.known_event_containers[container]["state"] = "on"

                    events_logger.debug("Sending mqtt payload")
                    self.mqtt_send(
                        self.events_topic.format(container),
                        json.dumps(self.known_event_containers[container]),
                        retain=True,
                    )
            except Exception as ex:
                events_logger.error("Error parsing line: %s", event_line)
                events_logger.error("Error of parsed line: %s", str(ex))

    def handle_stats_queue(self) -> None:
        """Check if any event is present in the queue and process it."""
        stat_line = ""

        docker_stats_qsize = self.docker_stats.qsize()
        try:
            if self.b_stats:
                stat_line = self.docker_stats.get(block=False)
            stats_logger.info("Stats queue length: %s", docker_stats_qsize)
        except Empty:
            # No data right now, just move along.
            return

            #################################
            # Examples:
            # {"BlockIO":"408MB / 0B","CPUPerc":"0.03%","Container":"9460abca90f1","ID":"9460abca90f1","MemPerc":"22.84%","MemUsage":"9.137MiB / 40MiB","Name":"d2mqtt","NetIO":"882kB / 1.19MB","PIDs":"11"}
            # {"BlockIO":"--","CPUPerc":"--","Container":"b5ad8ff32144","ID":"b5ad8ff32144","MemPerc":"--","MemUsage":"-- / --","Name":"camera_events","NetIO":"--","PIDs":"--"}
            #################################

        if self.b_stats and docker_stats_qsize > 0:
            try:
                if stat_line:
                    stat_line = "".join(
                        [c for c in stat_line if ord(c) > 31 or ord(c) == 9]
                    )
                    stat_line = stat_line.lstrip("[2J[H")
                    # print(':'.join(hex(ord(x))[2:] for x in stat_line))
                    stat = json.loads(stat_line)
                    # print("loaded json")
                    # print(stat)
                    container: str = stat["Name"]
                    stats_logger.debug(
                        "Have a Stat to process for container: %s", container
                    )

                    stats_logger.debug("Generating stat key (hashed stat line)")
                    stat_key = hashlib.md5(json.dumps(stat).encode("utf-8")).hexdigest()

                    if container not in self.known_stat_containers:
                        self.known_stat_containers[container] = ContainerStatsRef(
                            {"key": "", "last": datetime.datetime(2020, 1, 1)}
                        )

                        self.last_stat_containers[container] = {}

                    stats_logger.debug("Current stat key: %s", stat_key)
                    existing_stat_key = self.known_stat_containers[container]["key"]
                    stats_logger.debug("Last stat key: %s", existing_stat_key)

                    check_date = datetime.datetime.now() - datetime.timedelta(
                        seconds=self.cfg["stars_recording_seconds"]
                    )
                    container_date = self.known_stat_containers[container]["last"]
                    stats_logger.debug(
                        "Compare dates %s %s", check_date, container_date
                    )

                    if stat_key != existing_stat_key and container_date <= check_date:
                        stats_logger.info("Processing %s stats", container)
                        self.known_stat_containers[container]["key"] = stat_key
                        self.known_stat_containers[container]["last"] = (
                            datetime.datetime.now()
                        )
                        delta_seconds = (
                            self.known_stat_containers[container]["last"]
                            - container_date
                        ).total_seconds()

                        # "61.13MiB / 2.86GiB"
                        # regex = r"(?P<used>\d+?\.?\d+?)(?P<used_symbol>[MG]iB)\s+\/\s(?P<limit>\d+?\.?\d+?)(?P<limit_symbol>[MG]iB)"
                        regex = r"(?P<used>.+?)(?P<used_symbol>[kKMGT]?i?B)\s+\/\s(?P<limit>.+?)(?P<limit_symbol>[kKMGT]?i?B)"
                        stats_logger.debug(
                            'Getting memory from "%s" with "%s"',
                            stat["MemUsage"],
                            regex,
                        )
                        matches = re.match(regex, stat["MemUsage"], re.MULTILINE)
                        mem_mb_used, mem_mb_limit = self.stat_to_value(
                            "MEMORY", container, matches
                        )

                        stats_logger.debug(
                            'Getting NETIO from "%s" with "%s"', stat["NetIO"], regex
                        )
                        matches = re.match(regex, stat["NetIO"], re.MULTILINE)
                        netinput, netoutput = self.stat_to_value(
                            "NETIO", container, matches
                        )
                        netinputrate = (
                            max(
                                0,
                                (
                                    netinput
                                    - self.last_stat_containers[container].get(
                                        "netinput", 0
                                    )
                                ),
                            )
                            / delta_seconds
                        )
                        netoutputrate = (
                            max(
                                0,
                                (
                                    netoutput
                                    - self.last_stat_containers[container].get(
                                        "netoutput", 0
                                    )
                                ),
                            )
                            / delta_seconds
                        )

                        stats_logger.debug(
                            'Getting BLOCKIO from "%s" with "%s"',
                            stat["BlockIO"],
                            regex,
                        )
                        matches = re.match(regex, stat["BlockIO"], re.MULTILINE)
                        blockinput, blockoutput = self.stat_to_value(
                            "BLOCKIO", container, matches
                        )
                        blockinputrate = (
                            max(
                                0,
                                (
                                    blockinput
                                    - self.last_stat_containers[container].get(
                                        "blockinput", 0
                                    )
                                ),
                            )
                            / delta_seconds
                        )
                        blockoutputrate = (
                            max(
                                0,
                                (
                                    blockoutput
                                    - self.last_stat_containers[container].get(
                                        "blockoutput", 0
                                    )
                                ),
                            )
                            / delta_seconds
                        )

                        container_stats = ContainerStats(
                            {
                                "name": container,
                                "host": self.cfg["docker2mqtt_hostname"],
                                "cpu": float(stat["CPUPerc"].strip("%")),
                                "memory": stat["MemUsage"],
                                "memoryused": mem_mb_used,
                                "memorylimit": mem_mb_limit,
                                "netio": stat["NetIO"],
                                "netinput": netinput,
                                "netinputrate": netinputrate,
                                "netoutput": netoutput,
                                "netoutputrate": netoutputrate,
                                "blockinput": blockinput,
                                "blockinputrate": blockinputrate,
                                "blockoutput": blockoutput,
                                "blockoutputrate": blockoutputrate,
                            }
                        )
                        stats_logger.debug(
                            "Printing container stats: %s", container_stats
                        )
                        stats_logger.debug("Sending mqtt payload")
                        self.mqtt_send(
                            self.stats_topic.format(container),
                            json.dumps(container_stats),
                            retain=False,
                        )
                        self.last_stat_containers[container] = container_stats
                    else:
                        stats_logger.debug(
                            "Not processing record as duplicate record or too young: %s ",
                            container,
                        )

            except IndexError as ex:
                raise ex
            except ValueError as ex:
                raise ex
            except ReferenceError as ex:
                raise ex
            except TypeError as ex:
                raise ex
            except Exception as ex:
                stats_logger.error("Error parsing line: %s", stat_line)
                stats_logger.error("Error of parsed line: %s", str(ex))
                stats_logger.info(":".join(hex(ord(x))[2:] for x in stat_line))
