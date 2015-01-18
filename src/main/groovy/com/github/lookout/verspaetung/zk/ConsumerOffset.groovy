package com.github.lookout.verspaetung.zk

import org.apache.curator.framework.recipes.cache.ChildData

/**
 * POJO representing data from Zookeeper for a consumer, topic and offset
 */
class ConsumerOffset {
    private String topic
    private String groupName
    private Integer offset
    private Integer partition
    private ChildData rawData

    ConsumerOffset() {
    }

    ConsumerOffset(String topic, Integer partition, Integer offset) {
        this.topic = topic
        this.partition = partition
        this.offset = offset
    }

    String toString() {
        return "ConsumerOffset[${hashCode()}] ${topic}:${partition} ${groupName} is at ${offset}"
    }
}

