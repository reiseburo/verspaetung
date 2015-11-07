package com.github.reiseburo.verspaetung.zk

import spock.lang.*

import com.github.reiseburo.verspaetung.TopicPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

class AbstractConsumerTreeWatcherSpec extends Specification {
    private AbstractConsumerTreeWatcher watcher

    class MockWatcher extends AbstractConsumerTreeWatcher {
        MockWatcher() {
            super(null, new HashSet(), [:])
        }
        ConsumerOffset  processChildData(ChildData d) { }
        String zookeeperPath() { return '/zk/spock' }
        Boolean isOffsetSubtree(String p) { return true }
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

    def "childEvent() not processChildData if the event is not to be processed"() {
        given:
        watcher = Spy(MockWatcher)
        1 * watcher.isNodeEvent(_) >> false
        0 * watcher.processChildData(_) >> null

        expect:
        watcher.childEvent(null, null)
    }

    def "trackConsumerOffset() should create a new list for new topics in the map"() {
        given:
        ConsumerOffset offset = new ConsumerOffset('spock-topic', 0, 1337)

        when:
        watcher.trackConsumerOffset(offset)

        then:
        watcher.consumerOffsets.size() == 1
        watcher.watchedTopics.size() == 1
    }

    def "trackConsumerOffset() append an offset but not a topic for different group names"() {
        given:
        String topic = 'spock-topic'
        TopicPartition mapKey = new TopicPartition(topic, 0)
        ConsumerOffset offset = new ConsumerOffset(topic, 0, 1337)
        offset.groupName = 'spock-1'
        ConsumerOffset secondOffset = new ConsumerOffset(topic, 0, 0)
        secondOffset.groupName = 'spock-2'

        when:
        watcher.trackConsumerOffset(offset)
        watcher.trackConsumerOffset(secondOffset)

        then:
        watcher.watchedTopics.size() == 1
        watcher.consumerOffsets.size() == 2
    }


    @Ignore
    def "removeConsumer() should remove a ConsumerOffset from the map"() {
        given:
        TopicPartition tp = new TopicPartition('spock', 1)
        ConsumerOffset consumer = new ConsumerOffset()
    }
}
