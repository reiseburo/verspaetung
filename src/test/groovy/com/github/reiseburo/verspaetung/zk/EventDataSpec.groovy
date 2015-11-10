package com.github.reiseburo.verspaetung.zk

import spock.lang.*
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class EventDataSpec extends Specification {
    EventData data

    def "converts TreeCacheEvents to EventData"() {
        given:
        String path = "/brokers/ids/1337"
        data = new EventData(new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                new ChildData(path, null, "be happy".bytes)))

        expect:
        data.getPathPartAsInteger(3) == 1337
        data.asString() == 'be happy'
        data.pathPartsSize() == 4
        data.getPathPartAsInteger(4) == null
    }

    def "converts TreeCacheEvents to EventData with integer payload and no path"() {
        given:
        String path = "/brokers/ids/1337"
        data = new EventData(new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                new ChildData("/", null, "42".bytes)))

        expect:
        data.asString() == '42'
        data.asInteger() == 42
        data.pathPartsSize() == 0
        data.getPathPartAsInteger(0) == null
    }
 
    def "converts TreeCacheEvents to EventData with no payload"() {
        given:
        String path = "/brokers/ids/1337"
        data = new EventData(new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                new ChildData("/123", null, null)))

        expect:
        data.asString() == null
        data.pathPartsSize() == 2
        data.getPathPartAsInteger(1) == 123
    }
}
