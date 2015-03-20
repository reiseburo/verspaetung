package com.github.lookout.verspaetung.metrics

import spock.lang.*

import com.github.lookout.verspaetung.KafkaConsumer
import com.github.lookout.verspaetung.TopicPartition

class ConsumerGaugeSpec extends Specification {
    private KafkaConsumer consumer
    private TopicPartition tp

    def setup() {
        this.tp = new TopicPartition('spock-topic', 1)
        this.consumer = new KafkaConsumer(tp.topic, tp.partition, 'spock-consumer')
    }

    def "constructor should work"() {
        given:
        ConsumerGauge gauge = new ConsumerGauge(consumer, [:], [:])

        expect:
        gauge.consumer instanceof KafkaConsumer
        gauge.consumers instanceof AbstractMap
    }

    def "getValue() should source a value from the map"() {
        given:
        ConsumerGauge gauge = new ConsumerGauge(this.consumer,
                                                [(this.consumer) : 2],
                                                [(this.tp) : 3])

        expect:
        gauge.value == 1
    }

    def "getValue() should return zero for a consumer not in the map"() {
        given:
        ConsumerGauge gauge = new ConsumerGauge(consumer, [:], [:])

        expect:
        gauge.value == 0
    }
}
