package com.github.reiseburo.verspaetung.zk

import spock.lang.*

import com.github.reiseburo.verspaetung.TopicPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class AbstractTreeWatcherSpec extends Specification {
    private AbstractTreeWatcher watcher

    class MockWatcher extends AbstractTreeWatcher {
        MockWatcher() {
            super(null, [:])
        }
        void childEvent(CuratorFramework c, TreeCacheEvent e) { }
        String zookeeperPath() { return '/zk/spock' }
    }

    def setup() {
        this.watcher = new MockWatcher()
    }
}
