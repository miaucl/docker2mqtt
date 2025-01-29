# docker2mqtt - Deliver docker status information over MQTT

[![Mypy](https://github.com/miaucl/docker2mqtt/actions/workflows/mypy.yaml/badge.svg)](https://github.com/miaucl/docker2mqtt/actions/workflows/mypy.yaml)
[![Ruff](https://github.com/miaucl/docker2mqtt/actions/workflows/ruff.yml/badge.svg)](https://github.com/miaucl/docker2mqtt/actions/workflows/ruff.yml)
[![Markdownlint](https://github.com/miaucl/docker2mqtt/actions/workflows/markdownlint.yml/badge.svg)](https://github.com/miaucl/docker2mqtt/actions/workflows/markdownlint.yml)
[![Publish](https://github.com/miaucl/docker2mqtt/actions/workflows/publish.yml/badge.svg)](https://github.com/miaucl/docker2mqtt/actions/workflows/publish.yml)

This program uses `docker events` to watch for changes in your docker containers, and `docker stats` for metrics about those containers, and delivers current status to MQTT. It will also publish Home Assistant MQTT Discovery messages so that (binary) sensors automatically show up in Home Assistant.

_This is part of a family of similar tools:_

* [miaucl/linux2mqtt](https://github.com/miaucl/linux2mqtt)
* [miaucl/docker2mqtt](https://github.com/miaucl/docker2mqtt)
* [miaucl/systemctl2mqtt](https://github.com/miaucl/systemctl2mqtt)

## Installation and Deployment

It is available as python package on [pypi/docker2mqtt](https://pypi.org/p/docker2mqtt) or as a docker image on [ghcr.io/docker2mqtt](https://github.com/miaucl/docker2mqtt/pkgs/container/docker2mqtt).

### Pypi package

[![PyPI version](https://badge.fury.io/py/docker2mqtt.svg)](https://pypi.org/p/docker2mqtt)

```bash
pip install docker2mqtt
docker2mqtt --name MyDockerName --events -vvvvv
```

Usage

```python
from docker2mqtt import Docker2Mqtt, DEFAULT_CONFIG

cfg = Docker2MqttConfig({ 
  **DEFAULT_CONFIG,
  "host": "mosquitto",
  "enable_events": True
})

try:
  docker2mqtt = Docker2Mqtt(cfg)
  docker2mqtt.loop_busy()

except Exception as ex:
  # Do something
```

### Docker image

[![1] ![2] ![3]](https://github.com/miaucl/docker2mqtt/pkgs/container/docker2mqtt)

[1]: <https://ghcr-badge.egpl.dev/miaucl/docker2mqtt/tags?color=%23B8860B&ignore=latest&n=1&label=image&trim=>
[2]: <https://ghcr-badge.egpl.dev/miaucl/docker2mqtt/tags?color=%2344cc11&ignore=latest,*-rc*&n=3&label=image&trim=>
[3]: <https://ghcr-badge.egpl.dev/miaucl/docker2mqtt/size?color=%231E90FF&tag=latest&label=image+size&trim=>

Use docker to launch this. Please note that you must give it access to your docker socket, which is typically located at `/var/run/docker.sock`. A typical invocation is:

`docker run --network mqtt -e MQTT_HOST=mosquitto -v /var/run/docker.sock:/var/run/docker.sock ghcr.io/miaucl/docker2mqtt`

You can also use docker compose:

```yaml
services:
  docker2mqtt:
    container_name: docker2mqtt
    image: ghcr.io/miaucl/docker2mqtt
    environment:
      - DOCKER2MQTT_HOSTNAME=my_docker_host
      - MQTT_HOST=mosquitto
      - MQTT_USER=username
      - MQTT_PASSWD=password
      - EVENTS=1
      - STATS=1
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
```

## Default Configuration

You can use environment variables to control the behavior.

| Config | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `log_level`| `LOG_LEVEL` | `INFO` | Set to `DEBUG,INFO,WARN,ERROR,CRITICAL` to enable different levels of verbosity. |
| `docker2mqtt_hostname`| `DOCKER2MQTT_HOSTNAME` | docker2mqtt Container Hostname | The hostname of your docker host. This will be the container's hostname by default, you probably want to override it. |
| `homeassistant_prefix`| `HOMEASSISTANT_PREFIX` | `homeassistant` | The prefix for Home Assistant discovery. Must be the same as `discovery_prefix` in your Home Assistant configuration. |
| `homeassistant_single_device` | `HOMEASSISTANT_SINGLE_DEVICE` | `false` | Group all entities by a single device in Home Assistant instead of one device per entity. |
| `mqtt_client_id`| `MQTT_CLIENT_ID` | `mqtt2discord` | The client id to send to the MQTT broker. |
| `mqtt_host`| `MQTT_HOST` | `localhost` | The MQTT broker to connect to. |
| `mqtt_port`| `MQTT_PORT` | `1883` | The port on the broker to connect to. |
| `mqtt_user`| `MQTT_USER` | | The user to send to the MQTT broker. Leave unset to disable authentication. |
| `mqtt_password`| `MQTT_PASSWD` | | The password to send to the MQTT broker. Leave unset to disable authentication. |
| `mqtt_timeout`| `MQTT_TIMEOUT` | `30` | The timeout for the MQTT connection. |
| `mqtt_topic_prefix`| `MQTT_TOPIC_PREFIX` | `docker` | The MQTT topic prefix. With the default data will be published to `docker/<hostname>`. |
| `mqtt_qos`| `MQTT_QOS` | `1` | The MQTT QOS level |
| `container_whitelist` | `CONTAINER_WHITELIST` | | Define a whitelist for containers to consider, if empty, everything is monitored. The entries are either match as literal strings or as regex. |
| `container_blacklist` | `CONTAINER_BLACKLIST` | | Define a blacklist for containers to consider, takes priority over whitelist. The entries are either match as literal strings or as regex. |
| `destroyed_container_ttl`| `DESTROYED_CONTAINER_TTL` | `86400` | How long, in seconds, before destroyed containers are removed from Home Assistant. Containers won't be removed if the service is restarted before the TTL expires. |
| `stats_record_seconds`| `STATS_RECORD_SECONDS` | `30` | The number of seconds to record state and make an average |
| `enable_events`| `EVENTS` | `0` | 1 Or 0 for processing events |
| `enable_stats`| `STATS` | `0` | 1 Or 0 for processing statistics |

## Consuming The Data

Data is published to the topic `docker/<DOCKER2MQTT_HOSTNAME>/<container>/events` using JSON serialization. It will arrive whenever a change happens and its type can be inspected in [type_definitions.py](https://github.com/miaucl/docker2mqtt/blob/master/docker2mqtt/type_definitions.py) or the documentation.

Data is also published to the topic `docker/<DOCKER2MQTT_HOSTNAME>/<container>/stats` using JSON serialization. It will arrive every `STATS_RECORD_SECONDS` seconds or so and its type can be inspected in [type_definitions.py](https://github.com/miaucl/docker2mqtt/blob/master/docker2mqtt/type_definitions.py) or the documentation.

## Home Assistant

Once `docker2mqtt` is collecting data and publishing it to MQTT, it's rather trivial to use the data in Home Assistant.

A few assumptions:

* **Home Assistant is already configured to use a MQTT broker.** Setting up MQTT and HA is beyond the scope of this documentation. However, there are a lot of great tutorials on YouTube. An external broker (or as add-on) like [Mosquitto](https://mosquitto.org/) will need to be installed and the HA MQTT integration configured.
* **The HA MQTT integration is configured to use `homeassistant` as the MQTT autodiscovery prefix.** This is the default for the integration and also the default for `docker2mqtt`. If you have changed this from the default, use the `--prefix` parameter to specify the correct one.
* **You're not using TLS to connect to the MQTT broker.** Currently `docker2mqtt` only works with unencrypted connections. Username / password authentication can be specified with the `--username` and `--password` parameters, but TLS encryption is not yet supported.

After you start the service (binary) sensors should show up in Home Assistant immediately. Look for sensors that start with `(binary_)sensor.docker`. Metadata about the container will be available as attributes for events, which you can then expose using template sensors if you wish.

![Screenshot of Home Assistant sensor showing status and attributes.](https://raw.githubusercontent.com/miaucl/docker2mqtt/master/media/ha_screenshot.png)

## Documentation

Using `mkdocs`, the documentation and reference is generated and available on [github pages](https://miaucl.github.io/docker2mqtt/).

## Dev

Setup the dev environment using VSCode, it is highly recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_dev.txt
```

Install [pre-commit](https://pre-commit.com)

```bash
pre-commit install

# Run the commit hooks manually
pre-commit run --all-files
```

Following VSCode integrations may be helpful:

* [ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
* [mypy](https://marketplace.visualstudio.com/items?itemName=matangover.mypy)
* [markdownlint](https://marketplace.visualstudio.com/items?itemName=DavidAnson.vscode-markdownlint)

### Releasing

It is only possible to release a _final version_ on the `master` branch. For it to pass the gates of the `publish` workflow, it must have the same version in the `tag`, the `setup.cfg`, the `bring_api/__init__.py` and an entry in the `CHANGELOG.md` file.

To release a prerelease version, no changelog entry is required, but it can only happen on a feature branch (**not** `master` branch). Also, prerelease versions are marked as such in the github release page.

## Credits

This is a detached fork from the repo <https://github.com/skullydazed/docker2mqtt>, which does not seem to get evolved anymore.
