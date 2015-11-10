package com.github.reiseburo.verspaetung.zk

import spock.lang.*

import groovy.json.JsonSlurper

import com.github.reiseburo.verspaetung.KafkaBroker

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class PojoFactorySpec extends Specification {
    PojoFactory factory = new PojoFactory(new JsonSlurper())

    def "create KafkaBroker from TreeCacheEvent"() {
        given:
        String path = "/brokers/ids/1337"
        String payload = "{\"host\":\"localhost\",\"port\":9092}"
        TreeCacheEvent event = new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                  new ChildData(path, null, payload.bytes))
        KafkaBroker broker = factory.createKafkaBroker(event)

        expect:
        broker.brokerId == 1337
        broker.host == 'localhost'
        broker.port == 9092
    }

    def "create KafkaBroker from TreeCacheEvent without brokerId on path"() {
        given:
        String path = "/brokers/ids"
        TreeCacheEvent event = new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                  new ChildData(path, null, ''.bytes))
        KafkaBroker broker = factory.createKafkaBroker(event)

        expect:
        broker == null
    }

    def "create KafkaBroker from TreeCacheEvent without payload"() {
        given:
        String path = "/brokers/ids/123"
        TreeCacheEvent event = new TreeCacheEvent(TreeCacheEvent.Type.INITIALIZED,
                                                  new ChildData(path, null, null))
        KafkaBroker broker = factory.createKafkaBroker(event)

        expect:
        broker.brokerId == 123
        broker.host == ''
        broker.port == 0
    }
}
