"""docker2mqtt type definitions."""

from datetime import datetime
from typing import Literal, TypedDict

ContainerEventStateType = Literal["on", "off"]

ContainerEventStatusType = Literal[
    "paused", "running", "stopped", "destroyed", "created"
]


class Docker2MqttConfig(TypedDict):
    """A config object."""

    log_level: str
    destroyedContainerTTL: int
    homeassistant_prefix: str
    docker2mqtt_hostname: str
    mqtt_client_id: str
    mqtt_user: str
    mqtt_password: str
    mqtt_host: str
    mqtt_port: int
    mqtt_timeout: int
    mqtt_topic_prefix: str
    mqtt_qos: int
    enable_events: bool
    enable_stats: bool
    stars_recording_seconds: int


class ContainerEvent(TypedDict):
    """A container event object to send to an mqtt topic."""

    name: str
    image: str
    status: ContainerEventStatusType
    state: ContainerEventStateType


class ContainerStatsRef(TypedDict):
    """A container stats ref object compare between current and past stats."""

    key: str
    last: datetime


class ContainerStats(TypedDict):
    """A container stats object to send to an mqtt topic."""

    name: str
    host: str
    memory: str
    memoryused: float
    memorylimit: float
    netio: str
    netinput: float
    netinputrate: float
    netoutput: float
    netoutputrate: float
    blockinput: float
    blockinputrate: float
    blockoutput: float
    blockoutputrate: float
    cpu: float


class ContainerDeviceEntry(TypedDict):
    """A container device entry object for discovery in home assistant."""

    identifiers: str
    name: str
    model: str


class ContainerEntry(TypedDict):
    """A container entry object for discovery in home assistant."""

    name: str
    unique_id: str
    availability_topic: str
    payload_available: str
    payload_not_available: str
    state_topic: str
    value_template: str
    payload_on: str
    payload_off: str
    device: ContainerDeviceEntry
    device_class: str
    json_attributes_topic: str
    qos: int
