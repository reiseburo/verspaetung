package com.github.lookout.verspaetung

import java.util.concurrent.ConcurrentHashMap
import groovy.transform.TypeChecked

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener

@TypeChecked
class Main {
    static void main(String[] args) {
        println "Running ${args}"
        // XXX: Early exit until testing
        //return

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        ConcurrentHashMap<String, zk.ConsumerOffset> consumers = new ConcurrentHashMap()
        client.start()
        TreeCache cache = new TreeCache(client, '/consumers')
        println cache

        cache.listenable.addListener(new zk.StandardTreeWatcher(consumers))

        cache.start()
        println 'started..'

        Thread.sleep(9 * 1000)

        println 'exiting..'
        return
    }
}
