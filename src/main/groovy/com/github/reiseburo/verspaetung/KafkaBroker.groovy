package com.github.reiseburo.verspaetung

/**
 * POJO containing the necessary information to model a Kafka broker
 */
class KafkaBroker {
    final private String host
    final private Integer port
    final private Integer brokerId

    KafkaBroker(Object jsonObject, Integer brokerId) {
        this.host = jsonObject.host
        this.port = jsonObject.port
        this.brokerId = brokerId
    }

    @Override
    String toString() {
        return "broker<${this.brokerId}>@${this.host}:${this.port}"
    }
}
