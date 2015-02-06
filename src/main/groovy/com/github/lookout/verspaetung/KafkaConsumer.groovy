package com.github.lookout.verspaetung

/**
 * POJO containing the necessary information to model a Kafka consumers
 */
class KafkaConsumer {
    String topic
    Integer partition
    String name

    KafkaConsumer(String topic, Integer partition, String name) {
        this.topic = topic
        this.partition = partition
        this.name = name
    }

    @Override
    String toString() {
        return "KafkaConsumer<${topic}:${partition} - ${name}>"
    }

    @Override
    int hashCode() {
        return Objects.hash(this.topic, this.partition, this.name)
    }

    /**
     * Return true for any two KafkaConsumer instances which have the same
     * topic, partition and name properties
     */
    @Override
    boolean equals(Object compared) {
        /* bail early for object identity */
        if (this.is(compared)) {
            return true
        }

        if (!(compared instanceof KafkaConsumer)) {
            return false
        }

        if ((this.topic == compared.topic) &&
            (this.partition == compared.partition) &&
            (this.name == compared.name)) {
            return true
        }

        return false
    }

}
