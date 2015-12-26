package com.github.reiseburo.verspaetung.zk

import com.github.reiseburo.verspaetung.KafkaBroker

import groovy.transform.TypeChecked

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * manage a list of brokers. can be online or offline. offline means the
 * internal list is hidden, i.e. the list() gives you an empty list.
 */
@TypeChecked
class BrokerManager {

    private static final Logger logger = LoggerFactory.getLogger(BrokerManager)

    private final List<KafkaBroker> brokers = Collections.synchronizedList([])

    // we start with being offline
    private boolean offline = true

    void add(KafkaBroker broker) {
        synchronized(brokers) {
            if (broker != null && this.brokers.indexOf(broker) == -1) {
                this.brokers.add(broker)
                logger.info('broker added: {}', broker)
            }
        }
    }

    void update(KafkaBroker broker) {
        synchronized(brokers) {
            if (broker == null) {
                return
            }
            if (this.brokers.indexOf(broker) != -1) {
                this.brokers.remove(broker)
            }
            this.brokers.add(broker)
            logger.info('broker updated: {}', broker)
        }
    }

    void remove(KafkaBroker broker) {
        synchronized(brokers) {
            if (broker != null && this.brokers.remove(broker)) {
                logger.info('broker removed: {}', broker)
            }
        }
    }

    // TODO not sure if this is correct - see BrokerTreeWatcher
    @SuppressWarnings('ConfusingMethodName')
    void offline() {
        this.offline = true
    }

    // TODO not sure if this is correct - see BrokerTreeWatcher
    void online() {
        this.offline = false
    }

    Collection<KafkaBroker> list() {
        if (this.offline) {
            []
        }
        else {
            this.brokers
        }
    }
}
