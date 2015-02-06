package com.github.lookout.verspaetung.zk

import spock.lang.*

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData

class KafkaSpoutTreeWatcherSpec extends Specification {
    private KafkaSpoutTreeWatcher watcher
    private CuratorFramework mockCurator

    def setup() {
        this.mockCurator = Mock(CuratorFramework)
        this.watcher = new KafkaSpoutTreeWatcher(this.mockCurator, new HashSet(), [:])
    }

    def "consumerNameFromPath() should give the right name for a valid path"() {
        given:
        String path = '/kafka_spout/spock-topology/partition_1'

        expect:
        watcher.consumerNameFromPath(path) == 'spock-topology'
    }

    def "isOffsetSubtree should return true for a valid subtree path"() {
        given:
        String path = '/kafka_spout/spock-topology/partition_1'

        expect:
        watcher.isOffsetSubtree(path) == true
    }

    def "isOffsetSubtree should return false for a non-offset subtree path"() {
        given:
        String path = '/kafka_spout/spock-topology'

        expect:
        watcher.isOffsetSubtree(path) == false
    }
}
