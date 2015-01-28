# Verspätung

[![Build
Status](https://travis-ci.org/lookout/verspaetung.svg)](https://travis-ci.org/lookout/verspaetung)

[ ![Download](https://api.bintray.com/packages/lookout/systems/verspaetung/images/download.svg) ](https://bintray.com/lookout/systems/verspaetung/_latestVersion)

Verspätung is a small utility which aims to help identify delay of
[Kafka](http://kafka.apache.org) consumers.

Verspätung monitors the topics and their latest offsets by talking to Kafka, it
will also keep track of how far along consumers are by monitoring the offsets
that they have committed to Zookeeper. Using both of these pieces of
information, Verspätung computs the delta for each of the consumer groups and
reports it to statsd.


### Hacking

* *Running tests:* `./gradlew check`
* *Running the app locally:* `./gradlew run -PzookeeperHosts=localhost:2181`
* *Building the app for distribution:* `./gradlew shadowJar`
