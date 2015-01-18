package com.github.lookout.verspaetung

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener

class Main {
    static void main(String[] args) {
        println "Running ${args}"
        // XXX: Early exit until testing
        return

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        client.start()
        TreeCache cache = new TreeCache(client, '/consumers')
        println cache

        cache.listenable.addListener(new zk.StandardTreeWatcher())

        cache.start()
        println 'started..'

        Thread.sleep(5 * 1000)

        println 'exiting..'
        return

        client.children.forPath('/consumers').each { path ->
            try {
                client.children.forPath("/consumers/${path}/offsets").each { topic ->
                    client.children.forPath("/consumers/${path}/offsets/${topic}").each { partition ->
                        String offset = new String(client.data.forPath("/consumers/${path}/offsets/${topic}/${partition}"))
                        println "${path}:${topic}:${partition} = ${offset}"
                    }
                }
            }
            catch (KeeperException ex) {
                println "no offsets for ${path}"
            }
        }

        println client
    }
}
