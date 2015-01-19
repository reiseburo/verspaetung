package com.github.lookout.verspaetung

import groovy.transform.TypeChecked

@TypeChecked
class KafkaPoller extends Thread {
    private Boolean keepRunning = true
    private List<KafkaBroker> brokers
    private Boolean shouldReconnect = false

    void run() {
        while (keepRunning) {
            println 'kafka poll'

            if (shouldReconnect) {
                reconnect()
            }

            Thread.sleep(1 * 1000)
        }
    }

    /**
     * Blocking reconnect to the Kafka brokers
     */
    void reconnect() {
        println "reconnecting"
    }

    /**
     * Signal the runloop to safely die after it's next iteration
     */
    void die() {
        this.keepRunning = false
    }

    /**
     * Store a new list of KafkaBroker objects and signal a reconnection
     */
    void refresh(List brokers) {
        println "refresh: ${brokers}"
        this.brokers = brokers
        this.shouldReconnect = true
    }
}
