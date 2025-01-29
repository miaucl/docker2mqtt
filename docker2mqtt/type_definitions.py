"""docker2mqtt type definitions."""

from datetime import datetime
from typing import Literal, TypedDict

ContainerEventStateType = Literal["on", "off"]
"""Container event state"""

ContainerEventStatusType = Literal[
    "paused", "running", "stopped", "destroyed", "created"
]
"""Container event docker status"""


class Docker2MqttConfig(TypedDict):
    """A config object.

    Attributes
    ----------
    log_level
        Log verbosity
    destroyed_container_ttl
        How long, in seconds, before destroyed containers are removed from Home Assistant. Containers won't be removed if the service is restarted before the TTL expires.
    homeassistant_prefix
        MQTT discovery topic prefix
    homeassistant_single_device
        Group all entities by a single device in Home Assistant instead of one device per entity
    docker2mqtt_hostname
        A descriptive name for the docker being monitored
    mqtt_client_id
        Client Id for MQTT broker client
    mqtt_user
        Username for MQTT broker authentication
    mqtt_password
        Password for MQTT broker authentication
    mqtt_host
        Hostname or IP address of the MQTT broker
    mqtt_port
        Port or IP address of the MQTT broker
    mqtt_timeout
        Timeout for MQTT messages
    mqtt_topic_prefix
        MQTT topic prefix
    mqtt_qos
        QOS for standard MQTT messages
    container_whitelist
        Whitelist the containers to monitor, if empty, everything is monitored. The entries are either match as literal strings or as regex.
    container_blacklist
        Blacklist the containers to monitor, takes priority over whitelist. The entries are either match as literal strings or as regex.
    enable_events
        Flag to enable event monitoring
    enable_stats
        Flag to enable stat monitoring
    stats_record_seconds
        Interval every how many seconds the stats are published via MQTT

    """

    log_level: str
    destroyed_container_ttl: int
    homeassistant_prefix: str
    homeassistant_single_device: bool
    docker2mqtt_hostname: str
    mqtt_client_id: str
    mqtt_user: str
    mqtt_password: str
    mqtt_host: str
    mqtt_port: int
    mqtt_timeout: int
    mqtt_topic_prefix: str
    mqtt_qos: int
    container_whitelist: list[str]
    container_blacklist: list[str]
    enable_events: bool
    enable_stats: bool
    stats_record_seconds: int


class ContainerEvent(TypedDict):
    """A container event object to send to an mqtt topic.

    Attributes
    ----------
    name
        The name of the container
    image
        The image the container is running
    status
        The docker status the container is in
    state
        The state of the container

    """

    name: str
    image: str
    status: ContainerEventStatusType
    state: ContainerEventStateType


class ContainerStatsRef(TypedDict):
    """A container stats ref object compare between current and past stats.

    Attributes
    ----------
    key
        The reference key of a stat rotation
    last
        When the last stat rotation happened

    """

    key: str
    last: datetime


class ContainerStats(TypedDict):
    """A container stats object to send to an mqtt topic.

    Attributes
    ----------
    name
        The name of the container
    host
        The docker host
    memory
        Human-readable memory information from docker
    memoryused
        Used memory in MB
    memorylimit
        Memory limit in MB
    netio
        Human-readable network information from docker
    netinput
        Network input in MB
    netinputrate
        Network input rate in MB/s
    netoutput
        Network output in MB
    netoutputrate
        Network output rate in MB/s
    blockinput
        Block (to disk) input in MB
    blockinputrate
        Block (to disk) input rate in MB/s
    blockoutput
        Block (to disk) output in MB
    blockoutputrate
        Block (to disk) output rate in MB/s
    cpu
        The cpu usage by the container in cpu-% (ex.: a docker with 4 cores has 400% cpu available)

    """

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
    """A container device entry object for discovery in home assistant.

    Attributes
    ----------
    identifiers
        A unique str to identify the device in home assistant
    name
        The name of the device to display in home assistant
    model
        The model of the device as additional info

    """

    identifiers: str
    name: str
    model: str


class ContainerEntry(TypedDict):
    """A container entry object for discovery in home assistant.

    Attributes
    ----------
    name
        The name of the sensor to display in home assistant
    unique_id
        The unique id of the sensor in home assistant
    icon
        The icon of the sensor to display
    availability_topic
        The topic to check the availability of the sensor
    payload_available
        The payload of availability_topic of the sensor when available
    payload_unavailable
        The payload of availability_topic of the sensor when unavailable
    state_topic
        The topic containing all information for the state of the sensor
    value_template
        The jinja2 template to extract the state value from the state_topic for the sensor
    unit_of_measurement
        The unit of measurement of the sensor
    payload_on
        When a binary sensor: The value of extracted state of the sensor to be considered 'on'
    payload_off
        When a binary sensor: The value of extracted state of the sensor to be considered 'off'
    device
        The device the sensor is attributed to
    device_class
        The device class of the sensor
    state_topic
        The topic containing all information for the attributes of the sensor
    qos
        The QOS of the discovery message

    """

    name: str
    unique_id: str
    icon: str | None
    availability_topic: str
    payload_available: str
    payload_not_available: str
    state_topic: str
    value_template: str
    unit_of_measurement: str | None
    payload_on: str | None
    payload_off: str | None
    device: ContainerDeviceEntry
    device_class: str | None
    json_attributes_topic: str | None
    qos: int
