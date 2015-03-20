package com.github.lookout.verspaetung

import spock.lang.*

class KafkaConsumerSpec extends Specification {
    String topic = 'spock-topic'
    Integer partition = 2
    String consumerName = 'spock-consumer'

    def "the constructor should set the properties properly"() {
        given:
        KafkaConsumer consumer = new KafkaConsumer(topic, partition, consumerName)

        expect:
        consumer instanceof KafkaConsumer
        consumer.topic == topic
        consumer.partition == partition
        consumer.name == consumerName
    }

    def "equals() is true with identical source material"() {
        given:
        KafkaConsumer consumer1 = new KafkaConsumer(topic, partition, consumerName)
        KafkaConsumer consumer2 = new KafkaConsumer(topic, partition, consumerName)

        expect:
        consumer1 == consumer2
    }

    def "equals() is false with differing source material"() {
        given:
        KafkaConsumer consumer1 = new KafkaConsumer(topic, partition, consumerName)
        KafkaConsumer consumer2 = new KafkaConsumer(topic, partition, "i am different")

        expect:
        consumer1 != consumer2
    }
}
