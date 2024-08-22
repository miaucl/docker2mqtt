"""docker2mqtt const."""

# Env config
import re

LOG_LEVEL_DEFAULT = "INFO"
DESTROYED_CONTAINER_TTL_DEFAULT = 24 * 60 * 60  # s
HOMEASSISTANT_PREFIX_DEFAULT = "homeassistant"
MQTT_CLIENT_ID_DEFAULT = "docker2mqtt"
MQTT_PORT_DEFAULT = 1883
MQTT_TIMEOUT_DEFAULT = 30  # s
MQTT_TOPIC_PREFIX_DEFAULT = "docker"
MQTT_QOS_DEFAULT = 1
EVENTS_DEFAULT = False
STATS_DEFAULT = False
STATS_RECORD_SECONDS_DEFAULT = 30  # s

# Const
WATCHED_EVENTS = (
    "create",
    "destroy",
    "die",
    "pause",
    "rename",
    "start",
    "stop",
    "unpause",
)
MAX_QUEUE_SIZE = 100
DOCKER_EVENTS_CMD = [
    "docker",
    "events",
    "-f",
    "type=container",
    "--format",
    "{{json .}}",
]
DOCKER_PS_CMD = ["docker", "ps", "-a", "--format", "{{json .}}"]
DOCKER_STATS_CMD = ["docker", "stats", "--format", "{{json .}}"]
DOCKER_VERSION_CMD = ["docker", "--version"]
INVALID_HA_TOPIC_CHARS = re.compile(r"[^a-zA-Z0-9_-]")
ANSI_ESCAPE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
# fmt: off
STATS_REGISTRATION_ENTRIES = [
    # label,field,device_class,unit,icon
    ('CPU',                     'cpu',              None,           '%',    'mdi:cpu-64-bit'),
    ('Memory',                  'memoryused',       'data_size',    'MB',   'mdi:memory'),
    ('Network Input',           'netinput',         'data_size',    'MB',   'mdi:download-network'),
    ('Network Output',          'netoutput',        'data_size',    'MB',   'mdi:upload-network'),
    ('Network Input Rate',      'netinputrate',     'data_rate',    'MB/s', 'mdi:download-network-outline'),
    ('Network Output Rate',     'netoutputrate',    'data_rate',    'MB/s', 'mdi:upload-network-outline'),
    ('Block Input',             'blockinput',       'data_size',    'MB',   'mdi:database-arrow-up'),
    ('Block Output',            'blockoutput',      'data_size',    'MB',   'mdi:database-arrow-down'),
    ('Block Input Rate',        'blockinputrate',   'data_rate',    'MB/s', 'mdi:database-arrow-up-outline'),
    ('Block Output Rate',       'blockoutputrate',  'data_rate',    'MB/s', 'mdi:database-arrow-down-outline'),
]
# fmt: on