package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher
import com.github.lookout.verspaetung.zk.StandardTreeWatcher

import java.util.concurrent.ConcurrentHashMap
import groovy.transform.TypeChecked

import com.timgroup.statsd.StatsDClient
import com.timgroup.statsd.NonBlockingDogStatsDClient

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.slf4j.Logger
import org.slf4j.LoggerFactory

//@TypeChecked
class Main {
    private static final StatsDClient statsd = new NonBlockingDogStatsDClient('verspaetung', '10.32.2.211', 8125)
    private static final Logger logger = LoggerFactory.getLogger(Main.class)

    static void main(String[] args) {
        logger.info("Running with: ${args}")

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retry)
        ConcurrentHashMap<TopicPartition, List<zk.ConsumerOffset>> consumers = new ConcurrentHashMap()

        client.start()

        TreeCache cache = new TreeCache(client, '/consumers')

        KafkaPoller poller = new KafkaPoller(consumers)
        StandardTreeWatcher consumerWatcher = new StandardTreeWatcher(consumers)
        consumerWatcher.onInitComplete << {
            logger.info("standard consumers initialized to ${consumers.size()} (topic, partition) tuples")
        }

        BrokerTreeWatcher brokerWatcher = new BrokerTreeWatcher(client)
        brokerWatcher.onBrokerUpdates << { brokers ->
            poller.refresh(brokers)
        }

        cache.listenable.addListener(consumerWatcher)

        poller.onDelta << { String groupName, TopicPartition tp, Long delta ->
            statsd.recordGaugeValue(tp.topic, delta, [
                                                        'topic' : tp.topic,
                                                        'partition' : tp.partition,
                                                        'consumer-group' : groupName
                ])
        }

        poller.start()
        brokerWatcher.start()
        cache.start()

        logger.info("Started wait loop...")

        while (true) { Thread.sleep(1000) }

        logger.info("exiting..")
        poller.die()
        poller.join()
        return
    }
}
