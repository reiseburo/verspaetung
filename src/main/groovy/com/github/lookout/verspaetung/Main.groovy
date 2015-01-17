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

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        client.start()
        TreeCache cache = new TreeCache(client, '/kafka_spout')
        println cache

        cache.listenable.addListener([
            childEvent: { cl, ev ->
                println "EV: ${ev}"
            }
        ] as TreeCacheListener)


        cache.start()
        println 'started..'

        Boolean foundChildren = false
        Map children = null

        while ((children == null) || children.isEmpty()) {
            children = cache.getCurrentChildren('/kafka_spout')
            println "CHILDREN: ${children}"

            Thread.sleep(100)
        }
        [1, 2, 3].each { Thread.sleep(300) }
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
