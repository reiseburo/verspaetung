package com.github.reiseburo.verspaetung.zk

import spock.lang.*

class BrokerTreeWatcherSpec extends Specification {
    BrokerTreeWatcher watcher

    def setup() {
        this.watcher = new BrokerTreeWatcher()
    }

    def "brokerIdFromPath() should return the right ID with a valid path"() {
        given:
        String path = "/brokers/ids/1337"

        expect:
        watcher.brokerIdFromPath(path) == 1337
    }

    def "brokerIdFromPath() should return -1 on null paths"() {
        expect:
        watcher.brokerIdFromPath(null) == -1
    }

    def "brokerIdFromPath() should return -1 on empty/invalid paths"() {
        expect:
        watcher.brokerIdFromPath('/spock/ed') == -1
    }
}
