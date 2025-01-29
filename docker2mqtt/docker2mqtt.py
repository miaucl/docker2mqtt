#!/usr/bin/env python3
"""Listens to docker events and stats for containers and sends it to mqtt and supports discovery for home assistant."""

import argparse
import datetime
import hashlib
import json
import logging
import platform
from queue import Empty, Queue
import re
import signal
import socket
import subprocess
from subprocess import PIPE, Popen
import sys
from threading import Thread
from time import sleep, time
from typing import Any

import paho.mqtt.client

from docker2mqtt.helpers import clean_for_discovery

from . import __version__
from .const import (
    ANSI_ESCAPE,
    DESTROYED_CONTAINER_TTL_DEFAULT,
    DOCKER_EVENTS_CMD,
    DOCKER_PS_CMD,
    DOCKER_STATS_CMD,
    DOCKER_VERSION_CMD,
    HOMEASSISTANT_PREFIX_DEFAULT,
    HOMEASSISTANT_SINGLE_DEVICE_DEFAULT,
    INVALID_HA_TOPIC_CHARS,
    MAX_QUEUE_SIZE,
    MQTT_CLIENT_ID_DEFAULT,
    MQTT_PORT_DEFAULT,
    MQTT_QOS_DEFAULT,
    MQTT_TIMEOUT_DEFAULT,
    MQTT_TOPIC_PREFIX_DEFAULT,
    STATS_RECORD_SECONDS_DEFAULT,
    STATS_REGISTRATION_ENTRIES,
    WATCHED_EVENTS,
)
from .exceptions import (
    Docker2MqttConfigException,
    Docker2MqttConnectionException,
    Docker2MqttEventsException,
    Docker2MqttException,
    Docker2MqttStatsException,
)
from .type_definitions import (
    ContainerDeviceEntry,
    ContainerEntry,
    ContainerEvent,
    ContainerEventStateType,
    ContainerEventStatusType,
    ContainerStats,
    ContainerStatsRef,
    Docker2MqttConfig,
)

# Configure logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Loggers
main_logger = logging.getLogger("main")
events_logger = logging.getLogger("events")
stats_logger = logging.getLogger("stats")


class Docker2Mqtt:
    """docker2mqtt class.

    Attributes
    ----------
    version
        The version of docker2mqtt
    cfg
        The config for docker2mqtt
    b_stats
        Activate the stats
    b_events
        Activate the events
    docker_events
        Queue with docker events
    docker_stats
        Queue with docker stats

    known_event_containers
        The dict with the known container events
    known_stat_containers
        The dict with the known container stats references
    last_stat_containers
        The dict with the last container stats
    mqtt
        The mqtt client
    docker_events_t
        The thread to collect events from docker
    docker_stats_t
        The thread to collect stats from docker
    docker_version
        The docker version
    discovery_binary_sensor_topic
        Topic template for a binary sensor
    discovery_sensor_topic
        Topic template for a nary sensor
    status_topic
        Topic template for a status value
    version_topic
        Topic template for a version value
    stats_topic
        Topic template for stats
    events_topic
        Topic template for an events
    do_not_exit
        Prevent exit from within docker2mqtt, when handled outside

    """

    # Version
    version: str = __version__

    cfg: Docker2MqttConfig

    b_stats: bool = False
    b_events: bool = False

    docker_events: Queue[str] = Queue(maxsize=MAX_QUEUE_SIZE)
    docker_stats: Queue[str] = Queue(maxsize=MAX_QUEUE_SIZE)
    known_event_containers: dict[str, ContainerEvent] = {}
    known_stat_containers: dict[str, ContainerStatsRef] = {}
    last_stat_containers: dict[str, ContainerStats | dict[str, Any]] = {}
    pending_destroy_operations: dict[str, float] = {}

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

    do_not_exit: bool

    def __init__(self, cfg: Docker2MqttConfig, do_not_exit: bool = False):
        """Initialize the docker2mqtt.

        Parameters
        ----------
        cfg
            The configuration object for docker2mqtt
        do_not_exit
            Prevent exit from within docker2mqtt, when handled outside

        """

        self.cfg = cfg
        self.do_not_exit = do_not_exit

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

        try:
            self.docker_version = self._get_docker_version()
        except FileNotFoundError as e:
            raise Docker2MqttConfigException("Could not get docker version") from e

        if not self.do_not_exit:
            main_logger.info("Register signal handlers for SIGINT and SIGTERM")
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)

        main_logger.info("Events enabled: %d", self.b_events)
        main_logger.info("Stats enabled: %d", self.b_stats)

        try:
            # Setup MQTT
            self.mqtt = paho.mqtt.client.Client(
                callback_api_version=paho.mqtt.client.CallbackAPIVersion.VERSION2,  # type: ignore[attr-defined, call-arg]
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
            self._mqtt_send(self.status_topic, "online", retain=True)
            self._mqtt_send(self.version_topic, self.version, retain=True)

        except paho.mqtt.client.WebsocketConnectionError as ex:
            main_logger.exception("Error while trying to connect to MQTT broker.")
            main_logger.debug(ex)
            raise Docker2MqttConnectionException from ex

        # Register containers with HA
        docker_ps = subprocess.run(
            DOCKER_PS_CMD, capture_output=True, text=True, check=False
        )
        for line in docker_ps.stdout.splitlines():
            container_status = json.loads(line)

            if self._filter_container(container_status["Names"]):
                status_str: ContainerEventStatusType
                state_str: ContainerEventStateType

                if "Paused" in container_status["Status"]:
                    status_str = "paused"
                    state_str = "off"
                elif "Up" in container_status["Status"]:
                    status_str = "running"
                    state_str = "on"
                else:
                    status_str = "stopped"
                    state_str = "off"

                if self.b_events:
                    self._register_container(
                        {
                            "name": container_status["Names"],
                            "image": container_status["Image"],
                            "status": status_str,
                            "state": state_str,
                        }
                    )

        started = False
        try:
            if self.b_events:
                logging.info("Starting Events thread")
                self._start_readline_events_thread()
                started = True
        except Exception as ex:
            main_logger.exception("Error while trying to start events thread.")
            main_logger.debug(ex)
            raise Docker2MqttConfigException from ex

        try:
            if self.b_stats:
                started = True
                logging.info("Starting Stats thread")
                self._start_readline_stats_thread()
        except Exception as ex:
            main_logger.exception("Error while trying to start stats thread.")
            main_logger.debug(ex)
            raise Docker2MqttConfigException from ex

        if started is False:
            logging.critical("Nothing started, check your config!")
            sys.exit(1)

    def __del__(self) -> None:
        """Destroy the class."""
        self._cleanup()

    def _signal_handler(self, _signum: Any, _frame: Any) -> None:
        """Handle a signal for SIGINT or SIGTERM on the process.

        Parameters
        ----------
        _signum : Any
            (Unused)

        _frame : Any
            (Unused)

        """
        self._cleanup()
        sys.exit(0)

    def _cleanup(self) -> None:
        """Cleanup the docker2mqtt."""
        main_logger.warning("Shutting down gracefully.")
        try:
            self._mqtt_disconnect()
        except Docker2MqttConnectionException as ex:
            main_logger.exception("MQTT Cleanup Failed")
            main_logger.debug(ex)
            main_logger.info("Ignoring cleanup error and exiting...")

    def loop(self) -> None:
        """Start the loop.

        Raises
        ------
        Docker2MqttEventsException
            If anything goes wrong in the processing of the events
        Docker2MqttStatsException
            If anything goes wrong in the processing of the stats
        Docker2MqttException
            If anything goes wrong outside of the known exceptions

        """

        self._remove_destroyed_containers()

        self._handle_events_queue()

        self._handle_stats_queue()

        try:
            if self.b_events and not self.docker_events_t.is_alive():
                main_logger.warning("Restarting events thread")
                self._start_readline_events_thread()
        except Exception as ex:
            main_logger.exception("Error while trying to restart events thread.")
            main_logger.debug(ex)
            raise Docker2MqttConfigException from ex

        try:
            if self.b_stats and not self.docker_stats_t.is_alive():
                main_logger.warning("Restarting stats thread")
                self._start_readline_stats_thread()
        except Exception as ex:
            main_logger.exception("Error while trying to restart stats thread.")
            main_logger.debug(ex)
            raise Docker2MqttConfigException from ex

    def loop_busy(self, raise_known_exceptions: bool = False) -> None:
        """Start the loop (blocking).

        Parameters
        ----------
        raise_known_exceptions
            Should any known processing exception be raised or ignored

        Raises
        ------
        Docker2MqttEventsException
            If anything goes wrong in the processing of the events
        Docker2MqttStatsException
            If anything goes wrong in the processing of the stats
        Docker2MqttException
            If anything goes wrong outside of the known exceptions

        """

        while True:
            try:
                self.loop()
            except Docker2MqttEventsException as ex:
                if raise_known_exceptions:
                    raise ex  # noqa: TRY201
                else:
                    main_logger.warning(
                        "Do not raise due to raise_known_exceptions=False: %s", str(ex)
                    )
            except Docker2MqttStatsException as ex:
                if raise_known_exceptions:
                    raise ex  # noqa: TRY201
                else:
                    main_logger.warning(
                        "Do not raise due to raise_known_exceptions=False: %s", str(ex)
                    )

            # Calculate next iteration between (~0.2s and 0.001s)
            sleep_time = 0.001 + 0.2 / MAX_QUEUE_SIZE * (
                MAX_QUEUE_SIZE
                - max(self.docker_events.qsize(), self.docker_stats.qsize())
            )
            main_logger.debug("Sleep for %.5fs until next iteration", sleep_time)
            sleep(sleep_time)

    def _get_docker_version(self) -> str:
        """Get the docker version and save it to a global value.

        Returns
        -------
        str
            The docker version as string

        Raises
        ------
        FileNotFoundError
            If docker socket is not accessible.

        """
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
                raise Docker2MqttException(f"Error: {result.stderr.strip()}")
        except FileNotFoundError:
            return "Docker is not installed or not found in PATH."

    def _mqtt_send(self, topic: str, payload: str, retain: bool = False) -> None:
        """Send a mqtt payload to for a topic.

        Parameters
        ----------
        topic
            The topic to send a payload to
        payload
            The payload to send to the topic
        retain
            Whether the payload should be retained by the mqtt server

        Raises
        ------
        Docker2MqttConnectionError
            If the mqtt client could not send the data

        """
        try:
            main_logger.debug("Sending to MQTT: %s: %s", topic, payload)
            self.mqtt.publish(
                topic, payload=payload, qos=self.cfg["mqtt_qos"], retain=retain
            )

        except paho.mqtt.client.WebsocketConnectionError as ex:
            main_logger.exception("MQTT Publish Failed")
            main_logger.debug(ex)
            raise Docker2MqttConnectionException() from ex

    def _mqtt_disconnect(self) -> None:
        """Make sure we send our last_will message.

        Raises
        ------
        Docker2MqttConnectionError
            If the mqtt client could not send the data

        """
        try:
            self.mqtt.publish(
                self.status_topic,
                "offline",
                qos=self.cfg["mqtt_qos"],
                retain=True,
            )
            self.mqtt.publish(
                self.version_topic,
                self.version,
                qos=self.cfg["mqtt_qos"],
                retain=True,
            )
            self.mqtt.disconnect()
            sleep(1)
            self.mqtt.loop_stop()
        except paho.mqtt.client.WebsocketConnectionError as ex:
            main_logger.exception(
                "MQTT Disconnect",
            )
            main_logger.debug(ex)
            raise Docker2MqttConnectionException() from ex

    def _start_readline_events_thread(self) -> None:
        """Start the events thread."""
        self.docker_events_t = Thread(
            target=self._run_readline_events_thread, daemon=True, name="Events"
        )
        self.docker_events_t.start()

    def _run_readline_events_thread(self) -> None:
        """Run docker events and continually read lines from it."""
        thread_logger = logging.getLogger("event-thread")
        thread_logger.setLevel(self.cfg["log_level"].upper())
        try:
            thread_logger.info("Starting events thread")
            thread_logger.debug("Command: %s", DOCKER_EVENTS_CMD)
            with Popen(DOCKER_EVENTS_CMD, stdout=PIPE, text=True) as process:
                while True:
                    assert process.stdout
                    line = ANSI_ESCAPE.sub("", process.stdout.readline())
                    if line == "" and process.poll() is not None:
                        break
                    if line:
                        thread_logger.debug("Read docker event line: %s", line)
                        self.docker_events.put(line.strip())
                    _rc = process.poll()
        except Exception as ex:
            thread_logger.exception("Error Running Events thread")
            thread_logger.debug(ex)
            thread_logger.debug("Waiting for main thread to restart this thread")

    def _start_readline_stats_thread(self) -> None:
        """Start the stats thread."""
        self.docker_stats_t = Thread(
            target=self._run_readline_stats_thread, daemon=True, name="Stats"
        )
        self.docker_stats_t.start()

    def _run_readline_stats_thread(self) -> None:
        """Run docker events and continually read lines from it."""
        thread_logger = logging.getLogger("stats-thread")
        thread_logger.setLevel(self.cfg["log_level"].upper())
        try:
            thread_logger.info("Starting stats thread")
            thread_logger.debug("Command: %s", DOCKER_STATS_CMD)
            with Popen(DOCKER_STATS_CMD, stdout=PIPE, text=True) as process:
                while True:
                    assert process.stdout
                    line = ANSI_ESCAPE.sub("", process.stdout.readline())
                    if line == "" and process.poll() is not None:
                        break
                    if line:
                        thread_logger.debug("Read docker stat line: %s", line)
                        self.docker_stats.put(line.strip())
                    _rc = process.poll()
        except Exception as ex:
            thread_logger.exception("Error Running Stats thread")
            thread_logger.debug(ex)
            thread_logger.debug("Waiting for main thread to restart this thread")

    def _device_definition(
        self, container_entry: ContainerEvent
    ) -> ContainerDeviceEntry:
        """Create device definition of a container for each entity for home assistant.

        Parameters
        ----------
        container_entry : ContainerEvent
            The container event with the data to build a device entry config

        Returns
        -------
        ContainerDeviceEntry
            The device entry config

        """
        container = container_entry["name"]
        if not self.cfg["homeassistant_single_device"]:
            return {
                "identifiers": f"{self.cfg['docker2mqtt_hostname']}_{self.cfg['mqtt_topic_prefix']}_{container}",
                "name": f"{self.cfg['docker2mqtt_hostname']} {self.cfg['mqtt_topic_prefix'].title()} {container}",
                "model": f"{platform.system()} {platform.machine()} {self.docker_version}",
            }
        return {
            "identifiers": f"{self.cfg['docker2mqtt_hostname']}_{self.cfg['mqtt_topic_prefix']}",
            "name": f"{self.cfg['docker2mqtt_hostname']} {self.cfg['mqtt_topic_prefix'].title()}",
            "model": f"{platform.system()} {platform.machine()} {self.docker_version}",
        }

    def _register_container(self, container_entry: ContainerEvent) -> None:
        """Create discovery topics of container for all entities for home assistant.

        Parameters
        ----------
        container_entry : ContainerEvent
            The container event with the data to register a container

        Raises
        ------
        Docker2MqttConnectionError
            If the mqtt client could not send the data

        """
        container = container_entry["name"]
        self.known_event_containers[container] = container_entry

        # Events
        registration_topic = self.discovery_binary_sensor_topic.format(
            INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_events")
        )
        events_topic = self.events_topic.format(container)
        registration_packet = ContainerEntry(
            {
                "name": "Events",
                "unique_id": f"{self.cfg['mqtt_topic_prefix']}_{self.cfg['docker2mqtt_hostname']}_{registration_topic}",
                "availability_topic": f"{self.cfg['mqtt_topic_prefix']}/{self.cfg['docker2mqtt_hostname']}/status",
                "payload_available": "online",
                "payload_not_available": "offline",
                "state_topic": events_topic,
                "value_template": '{{ value_json.state if value_json is not undefined and value_json.state is not undefined else "off" }}',
                "payload_on": "on",
                "payload_off": "off",
                "icon": None,
                "unit_of_measurement": None,
                "device": self._device_definition(container_entry),
                "device_class": "running",
                "json_attributes_topic": events_topic,
                "qos": self.cfg["mqtt_qos"],
            }
        )
        self._mqtt_send(
            registration_topic,
            json.dumps(clean_for_discovery(registration_packet)),
            retain=True,
        )
        self._mqtt_send(
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
            registration_packet = ContainerEntry(
                {
                    "name": label,
                    "unique_id": f"{self.cfg['mqtt_topic_prefix']}_{self.cfg['docker2mqtt_hostname']}_{registration_topic}",
                    "availability_topic": f"{self.cfg['mqtt_topic_prefix']}/{self.cfg['docker2mqtt_hostname']}/status",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                    "state_topic": stats_topic,
                    "value_template": f"{{{{ value_json.{ field } if value_json is not undefined and value_json.{ field } is not undefined else None }}}}",
                    "unit_of_measurement": unit,
                    "icon": icon,
                    "payload_on": None,
                    "payload_off": None,
                    "json_attributes_topic": None,
                    "device_class": device_class,
                    "device": self._device_definition(container_entry),
                    "qos": self.cfg["mqtt_qos"],
                }
            )
            self._mqtt_send(
                registration_topic,
                json.dumps(clean_for_discovery(registration_packet)),
                retain=True,
            )
            self._mqtt_send(
                stats_topic,
                json.dumps({}),
                retain=True,
            )

    def _unregister_container(self, container: str) -> None:
        """Remove all discovery topics of container from home assistant.

        Parameters
        ----------
        container
            The container name unregister a container

        Raises
        ------
        Docker2MqttConnectionError
            If the mqtt client could not send the data

        """
        # Events
        self._mqtt_send(
            self.discovery_binary_sensor_topic.format(
                INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_events")
            ),
            "",
            retain=True,
        )
        self._mqtt_send(
            self.events_topic.format(container),
            "",
            retain=True,
        )

        # Stats
        for _, field, _, _, _ in STATS_REGISTRATION_ENTRIES:
            self._mqtt_send(
                self.discovery_sensor_topic.format(
                    INVALID_HA_TOPIC_CHARS.sub("_", f"{container}_{field}_stats")
                ),
                "",
                retain=True,
            )
        self._mqtt_send(
            self.stats_topic.format(container),
            "",
            retain=True,
        )

    def _match_container(self, container: str, to_check: str) -> bool:
        """Match a container to a value.

        Parameters
        ----------
        container
            The container to match
        to_check
            The string to check it with

        Returns
        -------
        bool
            Whether the container matches the string

        """
        return (
            container in (to_check) or re.compile(to_check).match(container) is not None
        )

    def _filter_container(self, container: str) -> bool:
        """Filter a container to whitelist and blacklist.

        Parameters
        ----------
        container
            The container to match

        Returns
        -------
        bool
            Whether the container should be considered

        """
        if len(self.cfg["container_whitelist"]) > 0:
            for to_check in self.cfg["container_whitelist"]:
                if self._match_container(container, to_check):
                    events_logger.debug(
                        "Match container '%s' with whitelist entry: %s",
                        container,
                        to_check,
                    )
                    break
            else:
                return False
        for to_check in self.cfg["container_blacklist"]:
            if self._match_container(container, to_check):
                return False
        return True

    def _stat_to_value(
        self, stat: str, container: str, matches: re.Match[str] | None
    ) -> tuple[float, float]:
        """Convert a regex matches to two values, i.e. used and limit for memory.

        Parameters
        ----------
        stat
            The stat string received from the docker stat command to parse
        container
            The container of the stat string
        matches
            The matches for the values to filter from the stat string

        Returns
        -------
        Tuple[float, float]
            The used and limit values extracted from the stat string


        """
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

    def _remove_destroyed_containers(self) -> None:
        """Remove any destroyed containers that have passed the TTL.

        Raises
        ------
        Docker2MqttEventsException
            If anything goes wrong in the processing of the events

        """
        try:
            for (
                container,
                destroyed_at,
            ) in self.pending_destroy_operations.copy().items():
                if time() - destroyed_at > self.cfg["destroyed_container_ttl"]:
                    main_logger.info("Removing container %s from MQTT.", container)
                    self._unregister_container(container)
                    del self.pending_destroy_operations[container]
        except Exception as e:
            raise Docker2MqttEventsException(
                "Could not remove destroyed containers"
            ) from e

    def _handle_events_queue(self) -> None:
        """Check if any event is present in the queue and process it.

        Raises
        ------
        Docker2MqttEventsException
            If anything goes wrong in the processing of the events

        """
        event_line = ""

        docker_events_qsize = self.docker_events.qsize()
        try:
            if self.b_events:
                event_line = self.docker_events.get(block=False)
            events_logger.debug("Events queue length: %s", docker_events_qsize)
        except Empty:
            # No data right now, just move along.
            pass

        if self.b_events and docker_events_qsize > 0:
            if event_line and len(event_line) > 0:
                try:
                    event = json.loads(event_line)
                    if event["status"] not in WATCHED_EVENTS:
                        events_logger.info("Not a watched event: %s", event["status"])
                        return

                    container: str = event["Actor"]["Attributes"]["name"]
                    if not self._filter_container(container):
                        events_logger.debug("Skip container: %s", container)
                        return

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

                        if self._filter_container(container):
                            self._register_container(
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
                        self._unregister_container(old_name)
                        if self._filter_container(container):
                            self._register_container(
                                {
                                    "name": container,
                                    "image": self.known_event_containers[old_name][
                                        "image"
                                    ],
                                    "status": self.known_event_containers[old_name][
                                        "status"
                                    ],
                                    "state": self.known_event_containers[old_name][
                                        "state"
                                    ],
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
                    else:
                        events_logger.debug("Unknown event: %s", event["status"])

                except Exception as ex:
                    events_logger.exception("Error parsing line: %s", event_line)
                    events_logger.exception("Error of parsed line")
                    events_logger.debug(ex)
                    raise Docker2MqttEventsException(
                        f"Error parsing line: {event_line}"
                    ) from ex

                events_logger.debug("Sending mqtt payload")
                self._mqtt_send(
                    self.events_topic.format(container),
                    json.dumps(self.known_event_containers[container]),
                    retain=True,
                )

    def _handle_stats_queue(self) -> None:
        """Check if any event is present in the queue and process it.

        Raises
        ------
        Docker2MqttStatsException
            If anything goes wrong in the processing of the stats

        """
        stat_line = ""
        send_mqtt = False

        docker_stats_qsize = self.docker_stats.qsize()
        try:
            if self.b_stats:
                stat_line = self.docker_stats.get(block=False)
            stats_logger.debug("Stats queue length: %s", docker_stats_qsize)
        except Empty:
            # No data right now, just move along.
            return

            #################################
            # Examples:
            # {"BlockIO":"408MB / 0B","CPUPerc":"0.03%","Container":"9460abca90f1","ID":"9460abca90f1","MemPerc":"22.84%","MemUsage":"9.137MiB / 40MiB","Name":"d2mqtt","NetIO":"882kB / 1.19MB","PIDs":"11"}
            # {"BlockIO":"--","CPUPerc":"--","Container":"b5ad8ff32144","ID":"b5ad8ff32144","MemPerc":"--","MemUsage":"-- / --","Name":"camera_events","NetIO":"--","PIDs":"--"}
            #################################

        if self.b_stats and docker_stats_qsize > 0:
            if stat_line and len(stat_line) > 0:
                try:
                    stat_line = "".join(
                        [c for c in stat_line if ord(c) > 31 or ord(c) == 9]
                    )
                    stat_line = stat_line.lstrip("[2J[H")
                    # print(':'.join(hex(ord(x))[2:] for x in stat_line))
                    stat = json.loads(stat_line)
                    # print("loaded json")
                    # print(stat)
                    container: str = stat["Name"]
                    if not self._filter_container(container):
                        stats_logger.debug("Skip container: %s", container)
                        return

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
                        seconds=self.cfg["stats_record_seconds"]
                    )
                    container_date = self.known_stat_containers[container]["last"]
                    stats_logger.debug(
                        "Compare dates %s %s", check_date, container_date
                    )

                    if stat_key != existing_stat_key and container_date <= check_date:
                        send_mqtt = True
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
                        mem_mb_used, mem_mb_limit = self._stat_to_value(
                            "MEMORY", container, matches
                        )

                        stats_logger.debug(
                            'Getting NETIO from "%s" with "%s"', stat["NetIO"], regex
                        )
                        matches = re.match(regex, stat["NetIO"], re.MULTILINE)
                        netinput, netoutput = self._stat_to_value(
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
                        blockinput, blockoutput = self._stat_to_value(
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
                        self.last_stat_containers[container] = container_stats
                    else:
                        stats_logger.debug(
                            "Not processing record as duplicate record or too young: %s ",
                            container,
                        )

                except Exception as ex:
                    stats_logger.exception("Error parsing line: %s", stat_line)
                    stats_logger.exception("Error of parsed line")
                    stats_logger.debug(ex)
                    stats_logger.info(":".join(hex(ord(x))[2:] for x in stat_line))
                    raise Docker2MqttStatsException(
                        f"Error parsing line: {stat_line}"
                    ) from ex

                if send_mqtt:
                    stats_logger.debug("Sending mqtt payload")
                    self._mqtt_send(
                        self.stats_topic.format(container),
                        json.dumps(self.last_stat_containers[container]),
                        retain=False,
                    )


def main() -> None:
    """Run main entry for the docker2mqtt executable.

    Raises
    ------
    Docker2MqttConfigException
        Bad config
    Docker2MqttConnectionException
        If anything with the mqtt connection goes wrong

    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "--name",
        default=socket.gethostname(),
        help="A descriptive name for the docker being monitored (default: hostname)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Hostname or IP address of the MQTT broker (default: localhost)",
    )
    parser.add_argument(
        "--port",
        default=MQTT_PORT_DEFAULT,
        type=int,
        help="Port or IP address of the MQTT broker (default: 1883)",
    )
    parser.add_argument(
        "--client",
        default=f"{socket.gethostname()}_{MQTT_CLIENT_ID_DEFAULT}",
        help=f"Client Id for MQTT broker client (default: <hostname>_{MQTT_CLIENT_ID_DEFAULT})",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Username for MQTT broker authentication (default: None)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Password for MQTT broker authentication (default: None)",
    )
    parser.add_argument(
        "--qos",
        default=MQTT_QOS_DEFAULT,
        type=int,
        help="QOS for MQTT broker authentication (default: 1)",
        choices=range(0, 3),
    )
    parser.add_argument(
        "--timeout",
        default=MQTT_TIMEOUT_DEFAULT,
        type=int,
        help=f"The timeout for the MQTT connection. (default: {MQTT_TIMEOUT_DEFAULT}s)",
    )
    parser.add_argument(
        "--ttl",
        default=DESTROYED_CONTAINER_TTL_DEFAULT,
        type=int,
        help=f"How long, in seconds, before destroyed containers are removed from Home Assistant. Containers won't be removed if the service is restarted before the TTL expires. (default: {DESTROYED_CONTAINER_TTL_DEFAULT}s)",
    )
    parser.add_argument(
        "--homeassistant-prefix",
        default=HOMEASSISTANT_PREFIX_DEFAULT,
        help=f"MQTT discovery topic prefix (default: {HOMEASSISTANT_PREFIX_DEFAULT})",
    )
    parser.add_argument(
        "--homeassistant-single-device",
        action="store_true",
        help=f"Group all entities by a single device in Home Assistant instead of one device per entity (default: {HOMEASSISTANT_SINGLE_DEVICE_DEFAULT})",
    )
    parser.add_argument(
        "--topic-prefix",
        default=MQTT_TOPIC_PREFIX_DEFAULT,
        help=f"MQTT topic prefix (default: {MQTT_TOPIC_PREFIX_DEFAULT})",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Log verbosity (default: 0 (log output disabled))",
    )
    parser.add_argument(
        "--whitelist",
        help="Container whitelist",
        type=str,
        action="append",
        nargs="?",
        const="",
        default=None,
        metavar="CONTAINER",
    )
    parser.add_argument(
        "--blacklist",
        help="Container blacklist",
        type=str,
        action="append",
        nargs="?",
        const="",
        default=None,
        metavar="CONTAINER",
    )
    parser.add_argument(
        "--events",
        help="Publish Events",
        action="store_true",
    )
    parser.add_argument(
        "--stats",
        help="Publish Stats",
        action="store_true",
    )
    parser.add_argument(
        "--interval",
        help=f"The number of seconds to record state and make an average (default: {STATS_RECORD_SECONDS_DEFAULT})",
        type=int,
        default=STATS_RECORD_SECONDS_DEFAULT,
    )

    try:
        args = parser.parse_args()
    except argparse.ArgumentError as e:
        raise Docker2MqttConfigException("Cannot start due to bad config") from e
    except argparse.ArgumentTypeError as e:
        raise Docker2MqttConfigException(
            "Cannot start due to bad config data type"
        ) from e
    log_level = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "DEBUG"][
        args.verbosity
    ]
    cfg = Docker2MqttConfig(
        {
            "log_level": log_level,
            "destroyed_container_ttl": args.ttl,
            "homeassistant_prefix": args.homeassistant_prefix,
            "homeassistant_single_device": args.homeassistant_single_device,
            "docker2mqtt_hostname": args.name,
            "mqtt_client_id": args.client,
            "mqtt_user": args.username,
            "mqtt_password": args.password,
            "mqtt_host": args.host,
            "mqtt_port": args.port,
            "mqtt_timeout": args.timeout,
            "mqtt_topic_prefix": args.topic_prefix,
            "mqtt_qos": args.qos,
            "container_whitelist": args.whitelist or [],
            "container_blacklist": args.blacklist or [],
            "enable_events": args.events,
            "enable_stats": args.stats,
            "stats_record_seconds": args.interval,
        }
    )

    docker2mqtt = Docker2Mqtt(
        cfg,
        do_not_exit=False,
    )

    docker2mqtt.loop_busy()
