# Verspätung

Verspätung is a small utility which aims to help identify delay of
[Kafka](http://kafka.apache.org) consumers.

Verspätung monitors the topics and their latest offsets by talking to Kafka, it
will also keep track of how far along consumers are by monitoring the offsets
that they have committed to Zookeeper. Using both of these pieces of
information, Verspätung computs the delta for each of the consumer groups and
reports it to statsd.



