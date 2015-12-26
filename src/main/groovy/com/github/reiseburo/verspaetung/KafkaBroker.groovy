package com.github.reiseburo.verspaetung

/**
 * POJO containing the necessary information to model a Kafka broker
 */
class KafkaBroker {
    final private String host
    final private Integer port
    final private Integer brokerId

    KafkaBroker(String host, Integer port, Integer brokerId) {
        this.host = host
        this.port = port
        this.brokerId = brokerId
    }

    @Override
    int hashCode() {
        return this.brokerId
    }

    @Override
    boolean equals(Object compared) {
        if (this.is(compared)) {
            return true
        }

        if (!(compared instanceof KafkaBroker)) {
            return false
        }

        return compared.brokerId == brokerId
    }

    @Override
    String toString() {
        return "broker<${this.brokerId}>@${this.host}:${this.port}"
    }
}
