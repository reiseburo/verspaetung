package com.github.lookout.verspaetung.zk

import spock.lang.*

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class AbstractTreeWatcherSpec extends Specification {
    private AbstractTreeWatcher watcher

    class MockWatcher extends AbstractTreeWatcher {
        ConsumerOffset processChildData(ChildData d) { }
    }

    def setup() {
        this.watcher = new MockWatcher()
    }

    def "isNodeEvent() returns false by default"() {
        expect:
        watcher.isNodeEvent(null) == false
    }

    def "isNodeEvent() return true for NODE_ADDED"() {
        given:
        def event = new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED, null)

        expect:
        watcher.isNodeEvent(event) == true
    }

    def "isNodeEvent() return true for NODE_UPDATED"() {
        given:
        def event = new TreeCacheEvent(TreeCacheEvent.Type.NODE_UPDATED, null)

        expect:
        watcher.isNodeEvent(event) == true
    }
}
