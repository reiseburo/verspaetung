package com.github.lookout.verspaetung.zk

import spock.lang.*

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData

class StandardTreeWatcherSpec extends Specification {
    private StandardTreeWatcher watcher
    private CuratorFramework mockCurator

    def setup() {
        this.mockCurator = Mock(CuratorFramework)
        this.watcher = new StandardTreeWatcher(this.mockCurator, [:])
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

    def "isOffsetSubtree should return true for a valid subtree path"() {
        given:
        String path = '/consumers/offtopic-spock-test/offsets/topic/7'

        expect:
        watcher.isOffsetSubtree(path) == true
    }

    def "isOffsetSubtree should return false for a non-offset subtree path"() {
        given:
        String path = '/consumers/offtopic-1025413624/owners/spock-hostname/0'

        expect:
        watcher.isOffsetSubtree(path) == false
    }
}
