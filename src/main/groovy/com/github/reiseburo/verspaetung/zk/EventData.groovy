package com.github.reiseburo.verspaetung.zk

import groovy.transform.TypeChecked

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

/**
 * parsing TreeCacheEvents and provide helper method to access its data in typed manner
 */
@TypeChecked
class EventData {

    private final String data
    private final List<String> pathParts

    EventData(TreeCacheEvent event) {
        ChildData data = event.data
        this.pathParts = data.path == null ? null : (data.path.split('/') as List)
        this.data = data.data == null ? null : new String(data.data, 'UTF-8')
    }

    Integer asInteger() {
        new Integer(data)
    }

    String asString() {
        data
    }

    Integer pathPartsSize() {
        pathParts.size()
    }

    Integer getPathPartAsInteger(int pos) {
        if (pathParts.size() <= pos) {
            return null
        }
        new Integer(pathParts[pos])
    }
}

