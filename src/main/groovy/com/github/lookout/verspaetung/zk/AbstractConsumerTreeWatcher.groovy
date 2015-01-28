package com.github.lookout.verspaetung.zk

import com.github.lookout.verspaetung.TopicPartition

import java.util.concurrent.CopyOnWriteArrayList
import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

@TypeChecked
abstract class AbstractConsumerTreeWatcher extends AbstractTreeWatcher {
    protected AbstractMap<TopicPartition, List<ConsumerOffset>> consumersMap

    AbstractConsumerTreeWatcher(CuratorFramework client,
                                AbstractMap consumersMap) {
        super(client)
        this.consumersMap = consumersMap
    }

    /**
     * Process the ChildData associated with an event
     */
    abstract ConsumerOffset processChildData(ChildData data)

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
        if (this.consumersMap == null) {
            return
        }

        TopicPartition key = new TopicPartition(offset.topic, offset.partition)

        if (this.consumersMap.containsKey(key)) {
            this.consumersMap[key] << offset
        }
        else {
            this.consumersMap[key] = new CopyOnWriteArrayList([offset])
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


