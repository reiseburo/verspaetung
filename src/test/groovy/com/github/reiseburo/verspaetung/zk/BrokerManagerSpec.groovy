package com.github.reiseburo.verspaetung.zk

import spock.lang.*

import groovy.json.JsonSlurper

import com.github.reiseburo.verspaetung.KafkaBroker

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class BrokerManagerSpec extends Specification {
    BrokerManager manager = new BrokerManager()

    def "new instance is offline, i.e. has empty list of brokers"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)

        expect:
        manager.list().size() == 0
    }

    def "new instance is offline after add broker"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        manager.add(broker)

        expect:
        manager.list().size() == 0
    }

    def "list shows brokers after getting online"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        KafkaBroker broker2 = new KafkaBroker('', 0, 123)
        manager.add(broker)
        manager.online()
        manager.remove(broker2)

        expect:
        manager.list().size() == 1
    }

    def "can remove brokers based on its id"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        KafkaBroker broker2 = new KafkaBroker('localhost', 0, 1)
        manager.add(broker)
        manager.online()
        manager.remove(broker2)

        expect:
        manager.list().size() == 0
    }

    def "can update brokers"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        KafkaBroker broker2 = new KafkaBroker('localhost', 0, 1)
        manager.online()
        manager.add(broker)
        manager.update(broker2)

        expect:
        manager.list().size() == 1
        manager.list().first().host == 'localhost'
    }

    def "can go offline anytime"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        KafkaBroker broker2 = new KafkaBroker('localhost', 0, 12)
        manager.online()
        manager.add(broker)
        manager.add(broker2)
        manager.offline()

        expect:
        manager.list().size() == 0
    }

    def "can go offline and online again"() {
        given:
        KafkaBroker broker = new KafkaBroker('', 0, 1)
        KafkaBroker broker2 = new KafkaBroker('localhost', 0, 12)
        manager.online()
        manager.add(broker)
        manager.add(broker2)
        manager.offline()
        manager.online()

        expect:
        manager.list().size() == 2
    }
}
