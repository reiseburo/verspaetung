package com.github.lookout.verspaetung.zk

import com.github.lookout.verspaetung.KafkaBroker

import groovy.json.JsonSlurper
import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

/**
 * The BrokerTreeWatcher is a kind of watcher whose sole purpose is
 * to watch the segment of the Zookeeper tree where Kafka stores broker
 * information
 */
@TypeChecked
class BrokerTreeWatcher extends AbstractTreeWatcher {
    static final Integer INVALID_BROKER_ID = -1
    private static final String BROKERS_PATH = '/brokers/ids'

    private Boolean isTreeInitialized = false
    private final JsonSlurper json
    private final List<Closure> onBrokerUpdates
    private final List<KafkaBroker> brokers

    BrokerTreeWatcher(CuratorFramework client) {
        super(client)

        this.json = new JsonSlurper()
        this.brokers = []
        this.onBrokerUpdates = []
    }

    String zookeeperPath() {
        return BROKERS_PATH
    }

    /**
     * Process events like NODE_ADDED and NODE_REMOVED to keep an up to date
     * list of brokers
     */
    @Override
    void childEvent(CuratorFramework client, TreeCacheEvent event) {
        /* If we're initialized that means we should have all our brokers in
         * our internal list already and we can fire an event
         */
        if (event.type == TreeCacheEvent.Type.INITIALIZED) {
            this.isTreeInitialized = true
            List threadsafeBrokers = Collections.synchronizedList(this.brokers)
            this.onBrokerUpdates.each { Closure c ->
                c?.call(threadsafeBrokers)
            }
            return
        }

        if (event.type != TreeCacheEvent.Type.NODE_ADDED) {
            return
        }

        ChildData nodeData = event.data
        Integer brokerId = brokerIdFromPath(nodeData.path)

        if (brokerId == INVALID_BROKER_ID) {
            return
        }

        Object brokerData = json.parseText(new String(nodeData.data, 'UTF-8'))

        this.brokers << new KafkaBroker(brokerData, brokerId)
    }

    /**
     * Process a path string from Zookeeper for the Kafka broker's ID
     *
     * We're expecting paths like: /brokers/ids/1231524312
     */
    Integer brokerIdFromPath(String path) {
        List<String> pathParts = path?.split('/') as List

        if ((pathParts == null) ||
            (pathParts.size() != 4)) {
            return INVALID_BROKER_ID
        }

        return new Integer(pathParts[-1])
    }
}
