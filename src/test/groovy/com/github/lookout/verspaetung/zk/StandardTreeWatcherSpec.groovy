package com.github.lookout.verspaetung.zk

import spock.lang.*

import org.apache.curator.framework.recipes.cache.ChildData

class StandardTreeWatcherSpec extends Specification {
    private StandardTreeWatcher watcher

    def setup() {
        this.watcher = new StandardTreeWatcher([:])
    }

    def "processChildData should return null if null is given"() {
        expect:
        watcher.processChildData(null) == null
    }

    def "processChildData should return null if the path is invalid"() {
        given:
        ChildData data = new ChildData("/consumers", null, (byte[])[48])
        ConsumerOffset offset = watcher.processChildData(data)

        expect:
        offset == null
    }

    def "processChildData should create a ConsumerOffset properly"() {
        given:
        String path = "/consumers/my-consumer-group/offsets/thetopic/3"
        ChildData data = new ChildData(path, null, (byte[])[48])
        ConsumerOffset offset = watcher.processChildData(data)

        expect:
        offset.groupName == 'my-consumer-group'
        offset.topic == 'thetopic'
        offset.partition == 3
        offset.offset == 0
    }
}