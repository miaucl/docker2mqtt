#!/usr/bin/env python3
"""Listens to docker events and stats for containers and sends it to mqtt and supports discovery for home assistant."""

import logging
from os import environ
from socket import gethostname

from .const import (
    DESTROYED_CONTAINER_TTL_DEFAULT,
    EVENTS_DEFAULT,
    HOMEASSISTANT_PREFIX_DEFAULT,
    LOG_LEVEL_DEFAULT,
    MQTT_CLIENT_ID_DEFAULT,
    MQTT_PORT_DEFAULT,
    MQTT_QOS_DEFAULT,
    MQTT_TIMEOUT_DEFAULT,
    MQTT_TOPIC_PREFIX_DEFAULT,
    STATS_DEFAULT,
    STATS_RECORD_SECONDS_DEFAULT,
)
from .docker2mqtt import Docker2Mqtt
from .type_definitions import Docker2MqttConfig

# Configure logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Loggers
main_logger = logging.getLogger("main")
events_logger = logging.getLogger("events")
stats_logger = logging.getLogger("main")


if __name__ == "__main__":
    # Env config

    cfg = Docker2MqttConfig(
        {
            "log_level": environ.get("LOG_LEVEL", LOG_LEVEL_DEFAULT),
            "destroyedContainerTTL": int(
                environ.get("DESTROYED_CONTAINER_TTL", DESTROYED_CONTAINER_TTL_DEFAULT)
            ),
            "homeassistant_prefix": environ.get(
                "HOMEASSISTANT_PREFIX", HOMEASSISTANT_PREFIX_DEFAULT
            ),
            "docker2mqtt_hostname": environ.get("DOCKER2MQTT_HOSTNAME", gethostname()),
            "mqtt_client_id": environ.get("MQTT_CLIENT_ID", MQTT_CLIENT_ID_DEFAULT),
            "mqtt_user": environ.get("MQTT_USER", ""),
            "mqtt_password": environ.get("MQTT_PASSWD", ""),
            "mqtt_host": environ.get("MQTT_HOST", ""),
            "mqtt_port": int(environ.get("MQTT_PORT", MQTT_PORT_DEFAULT)),
            "mqtt_timeout": int(environ.get("MQTT_TIMEOUT", MQTT_TIMEOUT_DEFAULT)),
            "mqtt_topic_prefix": environ.get(
                "MQTT_TOPIC_PREFIX", MQTT_TOPIC_PREFIX_DEFAULT
            ),
            "mqtt_qos": int(environ.get("MQTT_QOS", MQTT_QOS_DEFAULT)),
            "enable_events": bool(environ.get("EVENTS", EVENTS_DEFAULT)),
            "enable_stats": bool(environ.get("STATS", STATS_DEFAULT)),
            "stars_recording_seconds": int(
                environ.get("STATS_RECORD_SECONDS", STATS_RECORD_SECONDS_DEFAULT)
            ),
        }
    )

    try:
        docker2mqtt = Docker2Mqtt(cfg)
        docker2mqtt.loop_busy()

    except Exception as ex:
        main_logger.error("Error processing: %s", str(ex))
        print(ex)
