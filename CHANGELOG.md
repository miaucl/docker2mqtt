# CHANGELOG

## 2.0.3

* Transform the mqtt port cli arg to int as a str is not accepted by the paho.mqtt library
* Fix the container filter not only at startup but also at runtime

## 2.0.2

* Add version cli options to display package version
* Separate the entrypoints for cli (using cli arguments) and docker (using env vars)

## 2.0.1

* Fix white- and blacklist config via docker env where and empty string resulted in a pass-all regex overwriting the blacklist.

## 2.0.0

* Rework of the complete structure, but no functional changes.
