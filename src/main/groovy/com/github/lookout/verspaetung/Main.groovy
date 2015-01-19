package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher

import java.util.concurrent.ConcurrentHashMap
import groovy.transform.TypeChecked

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener

import kafka.client.ClientUtils

//@TypeChecked
class Main {
    static void main(String[] args) {
        println "Running ${args}"

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        ConcurrentHashMap<TopicPartition, List<zk.ConsumerOffset>> consumers = new ConcurrentHashMap()

        client.start()

        TreeCache cache = new TreeCache(client, '/consumers')
        cache.listenable.addListener(new zk.StandardTreeWatcher(consumers))

        KafkaPoller poller = new KafkaPoller()
        BrokerTreeWatcher brokerWatcher = new BrokerTreeWatcher(client)
        brokerWatcher.onBrokerUpdates = { brokers ->
            poller.refresh(brokers)
        }

        poller.start()

        brokerWatcher.start()
        cache.start()
        println 'started..'

        Thread.sleep(5 * 1000)

        println 'exiting..'
        poller.die()
        return
    }
}
