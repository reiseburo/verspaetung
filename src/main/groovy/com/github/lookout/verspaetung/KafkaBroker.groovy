package com.github.lookout.verspaetung

/**
 * POJO containing the necessary information to model a Kafka broker
 */
class KafkaBroker {
    private String host
    private Integer port
    private Integer brokerId

    public KafkaBroker(Object jsonObject, Integer brokerId) {
        this.host = jsonObject.host
        this.port = jsonObject.port
        this.brokerId = brokerId
    }

    @Override
    String toString() {
        return "broker<${this.brokerId}>@${this.host}:${this.port}"
    }
}
