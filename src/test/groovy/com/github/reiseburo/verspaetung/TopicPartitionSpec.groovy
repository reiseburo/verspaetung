package com.github.reiseburo.verspaetung

import spock.lang.*


class TopicPartitionSpec extends Specification {
    String topic = 'spock-topic'
    Integer partition = 12

    def "the constructor should set the properties"() {
        given:
        TopicPartition tp = new TopicPartition(topic, partition)

        expect:
        tp.topic == topic
        tp.partition == partition
    }

    def "equals() is true with identical topic/partions"() {
        given:
        TopicPartition left = new TopicPartition(topic, partition)
        TopicPartition right = new TopicPartition(topic, partition)

        expect:
        left == right
    }

    def "equals() is false with two different objects"() {
        given:
        TopicPartition left = new TopicPartition(topic, partition)

        expect:
        left != 'not even'
    }

    def "hashCode() returns an identical hash for identical topic/partitions"() {
        given:
        TopicPartition left = new TopicPartition(topic, partition)
        TopicPartition right = new TopicPartition(topic, partition)

        expect:
        left.hashCode() == right.hashCode()
    }

    def "hashCode() returns a different hash for different topic/partitions"() {
        given:
        TopicPartition left = new TopicPartition(topic, partition)
        TopicPartition right = new TopicPartition('bok', 1)

        expect:
        left.hashCode() != right.hashCode()
    }
}
