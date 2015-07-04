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

### Using

    % java -jar verspaetung-*-all.jar --help
    usage: verspaetung
     -H,--statsd-host <STATSD>   Hostname for a statsd instance (defaults to
                                 localhost)
     -n,--dry-run                Disable reporting to a statsd host
     -p,--statsd-port <PORT>     Port for the statsd instance (defaults to
                                 8125)
     -s,--storm                  Watch Storm KafkaSpout offsets (under
                                 /kafka_spout)
     -z,--zookeeper <HOSTS>      Comma separated list of Zookeeper hosts (e.g.
                                 localhost:2181)

Running Verspätung is rather easy, by default the daemon will monitor the
standard Kafka high-level consumer offset path of `/consumers` and start
reporting deltas automatically.

### Hacking

* *Running tests:* `./gradlew check`
* *Running the app locally:* `./gradlew run -PzookeeperHosts=localhost:2181`
* *Building the app for distribution:* `./gradlew assemble`


### Releasing

This is mostly meant for the developer team, but currently releases can be
produced by simply pushing a Git tag to this GitHub repository. This will cause
Travis CI to build and test the tag, which if it is successful, will
automatically publish to
[Bintray](https://bintray.com/lookout/systems/verspaetung).
