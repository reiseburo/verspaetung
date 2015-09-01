package com.github.lookout.verspaetung

/**
 * Simple container for Kafka topic names and partition IDs
 */
class TopicPartition {
    private final String topic
    private final Integer partition

    TopicPartition(String topic, Integer partition) {
        this.topic = topic
        this.partition = partition
    }

    /**
     * Return true for any two TopicPartition instances that have equal topic
     * and partition properties
     */
    @Override
    boolean equals(Object compared) {
        /* bail early for object identity */
        if (this.is(compared)) {
            return true
        }

        if (!(compared instanceof TopicPartition)) {
            return false
        }

        return (this.topic == compared.topic) &&
            (this.partition == compared.partition)
    }

    @Override
    int hashCode() {
        return Objects.hash(this.topic, this.partition)
    }

    @Override
    String toString() {
        return "${this.topic}:${this.partition}"
    }
}
