package com.github.lookout.verspaetung.zk

import com.github.lookout.verspaetung.TopicPartition

import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * AbstractTreeWatcher defines the contract and base components for the various
 * Zookeeper tree watchers Verspaetung needs. The responsibility of these
 * watchers is to process events from the TreeCache and emit processed events
 * further down the pipeline
 */
@TypeChecked
abstract class AbstractTreeWatcher implements TreeCacheListener {
    protected AbstractMap<TopicPartition, List<ConsumerOffset>> consumersMap
    protected List<Closure> onInitComplete
    protected Logger logger

    AbstractTreeWatcher(AbstractMap consumers) {
        this.consumersMap = consumers
        this.onInitComplete = []
        this.logger = LoggerFactory.getLogger(this.class)
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
            this.consumersMap[key] = [offset]
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
