package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher
import com.github.lookout.verspaetung.zk.StandardTreeWatcher

import java.util.concurrent.ConcurrentHashMap
import groovy.transform.TypeChecked

import com.timgroup.statsd.StatsDClient
import com.timgroup.statsd.NonBlockingStatsDClient

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache

//@TypeChecked
class Main {
    private static final StatsDClient statsd = new NonBlockingStatsDClient('verspaetung', 'localhost', 8125)

    static void main(String[] args) {
        println "Running ${args}"

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        ConcurrentHashMap<TopicPartition, List<zk.ConsumerOffset>> consumers = new ConcurrentHashMap()

        client.start()

        TreeCache cache = new TreeCache(client, '/consumers')

        KafkaPoller poller = new KafkaPoller(consumers)
        StandardTreeWatcher consumerWatcher = new StandardTreeWatcher(consumers)
        consumerWatcher.onInitComplete << {
            println "standard consumers initialized to ${consumers.size()} (topic, partition) tuples"
        }

        BrokerTreeWatcher brokerWatcher = new BrokerTreeWatcher(client)
        brokerWatcher.onBrokerUpdates << { brokers ->
            poller.refresh(brokers)
        }

        cache.listenable.addListener(consumerWatcher)

        poller.onDelta << { String groupName, TopicPartition tp, Long delta ->
            statsd.recordGaugeValue("${tp.topic}.${tp.partition}.${groupName}", delta)
        }

        poller.start()
        brokerWatcher.start()
        cache.start()
        println 'started..'

        while (true) { Thread.sleep(1000) }

        println 'exiting..'
        poller.die()
        poller.join()
        return
    }
}
