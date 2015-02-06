package com.github.lookout.verspaetung.metrics

import java.util.AbstractMap
import com.codahale.metrics.Gauge
import groovy.transform.TypeChecked

import com.github.lookout.verspaetung.KafkaConsumer
import com.github.lookout.verspaetung.TopicPartition


/**
 * Dropwizard Metrics Gauge for reporting the value of a given KafkaConsumer
 */
@TypeChecked
class ConsumerGauge implements Gauge<Integer> {
    protected KafkaConsumer consumer
    protected AbstractMap<KafkaConsumer, Integer> consumers
    protected AbstractMap<TopicPartition, Long> topics
    private TopicPartition topicPartition

    ConsumerGauge(KafkaConsumer consumer,
                  AbstractMap<KafkaConsumer, Integer> consumers,
                  AbstractMap<TopicPartition, Long> topics) {
        this.consumer = consumer
        this.consumers = consumers
        this.topics = topics

        this.topicPartition = new TopicPartition(consumer.topic, consumer.partition)
    }

    @Override
    Integer getValue() {
        if ((!this.consumers.containsKey(consumer)) ||
            (!this.topics.containsKey(topicPartition))) {
            return 0
        }
        return ((Integer)this.topics[topicPartition]) - this.consumers[consumer]
    }

    String getName() {
        return "verspaetung.${this.consumer.topic}.${this.consumer.partition}.${this.consumer.name}"
    }
}
