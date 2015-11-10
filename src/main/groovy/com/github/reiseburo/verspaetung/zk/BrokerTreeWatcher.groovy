package com.github.reiseburo.verspaetung.zk

import com.github.reiseburo.verspaetung.KafkaBroker

import groovy.json.JsonSlurper
import groovy.transform.TypeChecked

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

/**
 * The BrokerTreeWatcher is a kind of watcher whose sole purpose is
 * to watch the segment of the Zookeeper tree where Kafka stores broker
 * information
 */
@TypeChecked
class BrokerTreeWatcher extends AbstractTreeWatcher {
    private static final String BROKERS_PATH = '/brokers/ids'

    private final List<Closure> onBrokerUpdates
    private final BrokerManager manager
    private final PojoFactory factory

    BrokerTreeWatcher(CuratorFramework client) {
        super(client)

        this.factory = new PojoFactory(new JsonSlurper())
        this.manager = new BrokerManager()
        this.onBrokerUpdates = []
    }

    String zookeeperPath() {
        return BROKERS_PATH
    }

    /**
     * Process events to keep an up to date list of brokers
     */
    @Override
    void childEvent(CuratorFramework client, TreeCacheEvent event) {
        switch (event.type) {
            case TreeCacheEvent.Type.INITIALIZED:
                this.manager.online()
                break
            case TreeCacheEvent.Type.NODE_ADDED:
                KafkaBroker broker = this.factory.createKafkaBroker(event)
                this.manager.add(broker)
                break
            case TreeCacheEvent.Type.NODE_UPDATED:
                KafkaBroker broker = this.factory.createKafkaBroker(event)
                this.manager.update(broker)
                break
            case TreeCacheEvent.Type.NODE_REMOVED:
                KafkaBroker broker = this.factory.createKafkaBroker(event)
                this.manager.remove(broker)
                break
            // TODO these 3 events might come with path which can be mapped
            //      to a specific broker
            case TreeCacheEvent.Type.CONNECTION_LOST:
            case TreeCacheEvent.Type.CONNECTION_SUSPENDED:
                this.manager.offline()
                break
            case TreeCacheEvent.Type.CONNECTION_RECONNECTED:
                this.manager.online()
                break
        }

        this.onBrokerUpdates.each { Closure c ->
            c?.call(this.manager.list())
        }
    }
}
