package com.github.lookout.verspaetung

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.cluster.Broker
import kafka.client.ClientUtils
import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.common.KafkaException
/* UGH */
import scala.collection.JavaConversions

/**
 * KafkaPoller is a relatively simple class which contains a runloop for periodically
 * contacting the Kafka brokers defined in Zookeeper and fetching the latest topic
 * meta-data for them
 */
class KafkaPoller extends Thread {

    private static final String KAFKA_CLIENT_ID = 'VerspaetungClient'
    private static final Integer KAFKA_TIMEOUT = (5 * 1000)
    private static final Integer KAFKA_BUFFER = (100 * 1024)
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPoller)

    private Boolean keepRunning = true
    private Boolean shouldReconnect = false
    private final AbstractMap<Integer, SimpleConsumer> brokerConsumerMap
    private final AbstractMap<TopicPartition, Long> topicOffsetMap
    private final List<Closure> onDelta
    private final AbstractSet<String> currentTopics
    private List<Broker> brokers

    KafkaPoller(AbstractMap map, AbstractSet topicSet) {
        this.topicOffsetMap = map
        this.currentTopics = topicSet
        this.brokerConsumerMap = [:]
        this.brokers = []
        this.onDelta = []
    }

    /* There are a number of cases where we intentionally swallow stacktraces
     * to ensure that we're not unnecessarily spamming logs with useless stacktraces
     *
     * For CatchException, we're explicitly gobbling up all potentially crashing exceptions
     * here
     */
    @SuppressWarnings(['LoggingSwallowsStacktrace', 'CatchException'])
    void run() {
        LOGGER.info('Starting wait loop')
        Delay delay = new Delay()
        LOGGER.error('polling ' + delay)
        while (keepRunning) {
            LOGGER.debug('poll loop')

            if (shouldReconnect) {
                reconnect()
            }

            /* Only makes sense to try to dump meta-data if we've got some
             * topics that we should keep an eye on
             */
            if (this.currentTopics.size() > 0) {
                try {
                    dumpMetadata()
                    if (delay.reset()) {
                        LOGGER.error('back to normal ' + delay)
                    }
                }
                catch (KafkaException kex) {
                    LOGGER.error('Failed to interact with Kafka: {}', kex.message)
                    slower(delay)
                }
                catch (Exception ex) {
                    LOGGER.error('Failed to fetch and dump Kafka metadata', ex)
                    slower(delay)
                }
            }

            Thread.sleep(delay.value())
        }
    }

    void slower(Delay delay) {
        if (delay.slower()) {
            LOGGER.error('using ' + delay)
        }
    }

    @SuppressWarnings(['CatchException'])
    void dumpMetadata() {
        LOGGER.debug('dumping meta-data')

        Object metadata = fetchMetadataForCurrentTopics()

        withTopicsAndPartitions(metadata) { tp, p ->
            try {
                captureLatestOffsetFor(tp, p)
            }
            catch (Exception ex) {
                LOGGER.error('Failed to fetch latest for {}:{}', tp.topic, tp.partition, ex)

            }
        }

        LOGGER.debug('finished dumping meta-data')
    }

    /**
     * Invoke the given closure with the TopicPartition and Partition meta-data
     * informationn for all of the topic meta-data that was passed in.
     *
     * The 'metadata' is the expected return from
     * kafka.client.ClientUtils.fetchTopicMetadata
     */
    void withTopicsAndPartitions(Object metadata, Closure closure) {
        withScalaCollection(metadata.topicsMetadata).each { kafka.api.TopicMetadata f ->
            withScalaCollection(f.partitionsMetadata).each { p ->
                TopicPartition tp = new TopicPartition(f.topic, p.partitionId)
                closure.call(tp, p)
            }
        }
    }

    /**
     * Fetch the leader metadata and update our data structures
     */
    void captureLatestOffsetFor(TopicPartition tp, Object partitionMetadata) {
        Integer leaderId = partitionMetadata.leader.get()?.id
        Integer partitionId = partitionMetadata.partitionId

        Long offset = latestFromLeader(leaderId, tp.topic, partitionId)

        this.topicOffsetMap[tp] = offset
    }

    Long latestFromLeader(Integer leaderId, String topic, Integer partition) {
        SimpleConsumer consumer = this.brokerConsumerMap[leaderId]

        /* If we don't have a proper SimpleConsumer instance (e.g. null) then
         * we might not have gotten valid data back from Zookeeper
         */
        if (!(consumer instanceof SimpleConsumer)) {
            LOGGER.warn('Attempted to the leaderId: {} ({}/{})', leaderId, topic, partition)
            return 0
        }
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
        disconnectConsumers()
        LOGGER.info('Creating SimpleConsumer connections for brokers')
        this.brokers.each { Broker b ->
            SimpleConsumer consumer = new SimpleConsumer(b.host,
                                                         b.port,
                                                         KAFKA_TIMEOUT,
                                                         KAFKA_BUFFER,
                                                         KAFKA_CLIENT_ID)
            consumer.connect()
            this.brokerConsumerMap[b.id] = consumer
        }
        this.shouldReconnect = false
    }

    /**
     * Signal the runloop to safely die after it's next iteration
     */
    void die() {
        this.keepRunning = false
        disconnectConsumers()
    }

    private void disconnectConsumers() {
        this.brokerConsumerMap.each { Integer brokerId, SimpleConsumer client ->
            LOGGER.info('Disconnecting {}', client)
            client?.disconnect()
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

    /**
     * Return the brokers list as an immutable Seq collection for the Kafka
     * scala underpinnings
     */
    private scala.collection.immutable.Seq getBrokersSeq() {
        return JavaConversions.asScalaBuffer(this.brokers).toList()
    }

    /**
     * Return scala.collection.mutable.Set for the given List
     */
    private scala.collection.mutable.Set toScalaSet(Set set) {
        return JavaConversions.asScalaSet(set)
    }

    private Object fetchMetadataForCurrentTopics() {
        return ClientUtils.fetchTopicMetadata(
                            toScalaSet(currentTopics),
                            brokersSeq,
                            KAFKA_CLIENT_ID,
                            KAFKA_TIMEOUT,
                            0)
    }
}
