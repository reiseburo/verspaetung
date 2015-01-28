package com.github.lookout.verspaetung.zk

import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheListener
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
    protected List<Closure> onInitComplete
    protected Logger logger
    protected CuratorFramework client
    protected TreeCache cache

    AbstractTreeWatcher(CuratorFramework client) {
        this.logger = LoggerFactory.getLogger(this.class)
        this.client = client
        this.onInitComplete = []

        this.cache = new TreeCache(client, zookeeperPath())
        this.cache.listenable.addListener(this)
    }

    /**
     * Return the String of the path in Zookeeper this class should watch. This
     * method must be safe to call from the initializer of the class
     */
    abstract String zookeeperPath()

    /**
     * Start our internal cache
     */
    void start() {
        this.cache?.start()
    }

    abstract void childEvent(CuratorFramework client, TreeCacheEvent event)
}
