package com.github.lookout.verspaetung

import groovy.transform.TypeChecked

import kafka.cluster.Broker
import kafka.client.ClientUtils
import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.javaapi.*
/* UGH */
import scala.collection.JavaConversions

/* Can't type check this because it makes the calls in and out of Scala an
 * atrocious pain in the ass
 */
//@TypeChecked
class KafkaPoller extends Thread {
    private final String KAFKA_CLIENT_ID = 'VerspaetungClient'
    private final Integer KAFKA_TIMEOUT = (5 * 1000)
    private final Integer KAFKA_BUFFER = (100 * 1024)

    private Boolean keepRunning = true
    private Boolean shouldReconnect = false
    private HashMap<Integer, SimpleConsumer> brokerConsumerMap = [:]
    private List<Broker> brokers = []
    private AbstractMap<TopicPartition, List<zk.ConsumerOffset>> consumersMap
    private List<Closure> onDelta = []

    KafkaPoller(AbstractMap map) {
        this.consumersMap = map
    }

    void run() {
        while (keepRunning) {
            println 'kafka poll'

            if (shouldReconnect) {
                reconnect()
            }

            if (this.consumersMap.size() > 0) {
                dumpMetadata()
            }

            Thread.sleep(1 * 1000)
        }
    }

    void dumpMetadata() {
        println 'dumping'
        def topics = this.consumersMap.keySet().collect { TopicPartition k -> k.topic }
        def metadata = ClientUtils.fetchTopicMetadata(toScalaSet(new HashSet(topics)),
                                                      brokersSeq,
                                                      KAFKA_CLIENT_ID,
                                                      KAFKA_TIMEOUT,
                                                      0)

        withScalaCollection(metadata.topicsMetadata).each { kafka.api.TopicMetadata f ->
            withScalaCollection(f.partitionsMetadata).each { p ->
                Long offset = latestFromLeader(p.leader.get()?.id, f.topic, p.partitionId)
                TopicPartition tp = new TopicPartition(f.topic, p.partitionId)
                print "Consumer for ${f.topic}:${p.partitionId}"
                println " latest: ${offset}"

                this.consumersMap[tp].each { zk.ConsumerOffset c ->
                    Long delta = offset - c.offset
                    if (delta > 0) {
                        this.onDelta.each { Closure callback ->
                            callback.call(c.groupName, tp, delta)
                        }
                    }
                }
            }
        }

        println 'dumped'
    }


    Long latestFromLeader(Integer leaderId, String topic, Integer partition) {
        SimpleConsumer consumer = this.brokerConsumerMap[leaderId]
        TopicAndPartition topicAndPart = new TopicAndPartition(topic, partition)
        /* XXX: A zero clientId into this method might not be right */
        return consumer.earliestOrLatestOffset(topicAndPart, -1, 0)
    }

    Iterable withScalaCollection(scala.collection.Iterable iter) {
        return JavaConversions.asJavaIterable(iter)
    }

    /**
     * Blocking reconnect to the Kafka brokers
     */
    void reconnect() {
        println "reconnecting"
        this.brokers.each { Broker b ->
            SimpleConsumer consumer = new SimpleConsumer(b.host,
                                                         b.port,
                                                         KAFKA_TIMEOUT,
                                                         KAFKA_BUFFER,
                                                         KAFKA_CLIENT_ID)
            consumer.connect()
            this.brokerConsumerMap[b.id] = consumer
        }
        this.shouldReconnect =false
    }

    /**
     * Return the brokers list as an immutable Seq collection for the Kafka
     * scala underpinnings
     */
    scala.collection.immutable.Seq getBrokersSeq() {
        return JavaConversions.asScalaBuffer(this.brokers).toList()
    }

    /**
     * Return scala.collection.mutable.Set for the given List
     */
    scala.collection.mutable.Set toScalaSet(Set set) {
        return JavaConversions.asScalaSet(set)
    }

    /**
     * Signal the runloop to safely die after it's next iteration
     */
    void die() {
        this.keepRunning = false
        this.brokerConsumerMap.each { Integer brokerId, SimpleConsumer client ->
            client.disconnect()
        }
    }

    /**
     * Store a new list of KafkaBroker objects and signal a reconnection
     */
    void refresh(List<KafkaBroker> brokers) {
        this.brokers = brokers.collect { KafkaBroker b ->
            new Broker(b.brokerId, b.host, b.port)
        }
        this.shouldReconnect = true
    }
}
