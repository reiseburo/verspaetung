package com.github.lookout.verspaetung.zk

/**
 * POJO representing data from Zookeeper for a consumer, topic and offset
 */
class ConsumerOffset {
    String topic
    String groupName
    Integer offset
    Integer partition

    ConsumerOffset() {
    }

    ConsumerOffset(String topic, Integer partition, Integer offset) {
        this.topic = topic
        this.partition = partition
        this.offset = offset
    }

    String toString() {
        return "ConsumerOffset[${hashCode()}] ${topic}:${partition} ${groupName} is at ${offset}".toString()
    }
}

