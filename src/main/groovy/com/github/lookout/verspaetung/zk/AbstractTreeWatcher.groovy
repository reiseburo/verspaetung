package com.github.lookout.verspaetung.zk

import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

/**
 * AbstractTreeWatcher defines the contract and base components for the various
 * Zookeeper tree watchers Verspaetung needs. The responsibility of these
 * watchers is to process events from the TreeCache and emit processed events
 * further down the pipeline
 */
abstract class AbstractTreeWatcher implements TreeCacheListener {
    protected Map consumersMap = [:]

    AbstractTreeWatcher() { }
    AbstractTreeWatcher(Map consumers) {
        this.consumersMap = consumers
    }

    /**
     * Process the ChildData associated with an event
     */
    abstract ConsumerOffset processChildData(ChildData data)

    /**
     * Primary TreeCache event processing callback
     */
    void childEvent(CuratorFramework client, TreeCacheEvent event) {
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
     *
     */
    void trackConsumerOffset(ConsumerOffset offset) {
        if (this.consumersMap == null) {
            return
        }

        if (this.consumersMap.containsKey(offset.topic)) {
            this.consumersMap[offset.topic] << offset
        }
        else {
            this.consumersMap[offset.topic] = [offset]
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
