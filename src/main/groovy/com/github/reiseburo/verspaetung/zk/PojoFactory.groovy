package com.github.reiseburo.verspaetung.zk

import com.github.reiseburo.verspaetung.KafkaBroker

import groovy.json.JsonSlurper

import org.apache.curator.framework.recipes.cache.TreeCacheEvent

/**
 * factory creating Pojo's from TreeCacheEvents
 */
class PojoFactory {

    private final JsonSlurper json

    PojoFactory(JsonSlurper json) {
        this.json = json
    }

    /**
     * converts an treeCacheEvent into a KafkaBroker. the caller
     * is responsible for calling the factory method with the
     * right data. i.e. path starts with /brokers/ids. the implementation
     * just uses whatever it is given to create a KafkaBroker object
     */
    KafkaBroker createKafkaBroker(TreeCacheEvent event) {
        EventData data = new EventData(event)
        // We're expecting paths like: /brokers/ids/1231524312
        Integer id = data.getPathPartAsInteger(3)
        if (id == null) {
            return
        }
        String json = data.asString()
        if (json == null) {
            new KafkaBroker('', 0, id)
        }
        else {
            Object payload = this.json.parseText(json)
            new KafkaBroker(payload.host, payload.port, id)
        }
    }
}
