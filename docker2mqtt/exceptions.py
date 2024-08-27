"""docker2mqtt exceptions."""


class Docker2MqttException(Exception):
    """General processing exception occurred."""


class Docker2MqttConfigException(Docker2MqttException):
    """Config exception occurred."""


class Docker2MqttConnectionException(Docker2MqttException):
    """Connection exception occurred."""


class Docker2MqttEventsException(Docker2MqttException):
    """Events processing exception occurred."""


class Docker2MqttStatsException(Docker2MqttException):
    """Stats processing exception occurred."""
