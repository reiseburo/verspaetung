package com.github.lookout.verspaetung.zk

import com.github.lookout.verspaetung.KafkaConsumer
import com.github.lookout.verspaetung.TopicPartition

import java.util.concurrent.CopyOnWriteArrayList
import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

@TypeChecked
abstract class AbstractConsumerTreeWatcher extends AbstractTreeWatcher {
    protected AbstractMap<KafkaConsumer, Integer> consumerOffsets
    protected AbstractSet<String> watchedTopics
    protected List<Closure> onConsumerData = []

    AbstractConsumerTreeWatcher(CuratorFramework client,
                                AbstractSet topics,
                                AbstractMap offsets) {
        super(client)
        this.watchedTopics = topics
        this.consumerOffsets = offsets
    }

    /**
     * Process the ChildData associated with an event
     */
    abstract ConsumerOffset processChildData(ChildData data)

    /**
     * Determine whether a given path is of interest, i.e. path which contains
     * offset data
     */
    abstract Boolean isOffsetSubtree(String path)
    /**
     * Primary TreeCache event processing callback
     */
    void childEvent(CuratorFramework client, TreeCacheEvent event) {
        if (event?.type == TreeCacheEvent.Type.INITIALIZED) {
            this.onInitComplete.each { Closure c ->
                c?.call()
            }
        }

        /* bail out early if we don't care about the event */
        if (!isNodeEvent(event)) {
            return
        }

        if ((event.data == null) ||
            (!isOffsetSubtree(event.data?.path))) {
            return
        }

        ConsumerOffset offset = processChildData(event?.data)

        if (offset != null) {
            trackConsumerOffset(offset)
        }
    }

    /**
     * Keep track of a ConsumerOffset in the consumersMap that was passed into
     * this class on instantiation
     */
    void trackConsumerOffset(ConsumerOffset offset) {
        if (this.consumerOffsets == null) {
            return
        }

        this.watchedTopics << offset.topic
        KafkaConsumer consumer = new KafkaConsumer(offset.topic,
                                                    offset.partition,
                                                    offset.groupName)
        this.consumerOffsets[consumer] = offset.offset

        this.onConsumerData.each { Closure c ->
            c.call(consumer)
        }
    }

    /**
     * Return true if the TreeCacheEvent received pertains to a node event that
     * we're interested in
     */
    Boolean isNodeEvent(TreeCacheEvent event) {
        if ((event?.type == TreeCacheEvent.Type.NODE_ADDED) ||
            (event?.type == TreeCacheEvent.Type.NODE_UPDATED)) {
            return true
        }
        return false
    }
}


