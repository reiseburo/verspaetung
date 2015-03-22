package com.github.lookout.verspaetung.metrics

import java.util.AbstractMap
import com.codahale.metrics.Gauge
import groovy.transform.TypeChecked
import org.coursera.metrics.datadog.Tagged

import com.github.lookout.verspaetung.KafkaConsumer
import com.github.lookout.verspaetung.TopicPartition

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Dropwizard Metrics Gauge for reporting the value of a given KafkaConsumer
 */
@TypeChecked
class ConsumerGauge implements Gauge<Integer>, Tagged {
    protected KafkaConsumer consumer
    protected AbstractMap<KafkaConsumer, Integer> consumers
    protected AbstractMap<TopicPartition, Long> topics
    private TopicPartition topicPartition

    private Logger logger = LoggerFactory.getLogger(ConsumerGauge.class)

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

        /*
         * Returning the maximum value of the computation and zero, there are
         * some cases where we can be "behind" on the Kafka latest offset
         * polling and this could result in an erroneous negative value. See:
         * <https://github.com/lookout/verspaetung/issues/25> for more details
         */
        return Math.max(0,
                        ((Integer)this.topics[topicPartition]) - this.consumers[consumer])
    }

    @Override
    List<String> getTags() {
        return ["partition:${this.consumer.partition}",
                "topic:${this.consumer.topic}",
                "consumer-group:${this.consumer.name}"
                ].collect { s -> s.toString() }
    }

    /**
     * return a unique name for this gauge
     */
    String getNameForRegistry() {
        return "${this.consumer.topic}.${this.consumer.partition}.${this.consumer.name}"
    }

    @Override
    String getName() {
        return this.consumer.topic

        /* need to return this if we're just using the console or statsd
         * reporters

        return "${this.consumer.topic}.${this.consumer.partition}.${this.consumer.name}"

         */
    }
}
