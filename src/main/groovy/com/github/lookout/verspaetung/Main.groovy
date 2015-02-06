package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher
import com.github.lookout.verspaetung.zk.KafkaSpoutTreeWatcher
import com.github.lookout.verspaetung.zk.StandardTreeWatcher
import com.github.lookout.verspaetung.metrics.ConsumerGauge

import java.util.AbstractMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit
import groovy.transform.TypeChecked

import org.apache.commons.cli.*
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.codahale.metrics.*

class Main {
    private static final String METRICS_PREFIX = 'verspaetung'

    private static Logger logger
    private static ScheduledReporter reporter
    private static final MetricRegistry registry = new MetricRegistry()

    static void main(String[] args) {
        String statsdPrefix = METRICS_PREFIX
        String zookeeperHosts = 'localhost:2181'
        String statsdHost = 'localhost'
        Integer statsdPort = 8125

        CommandLine cli = parseCommandLine(args)

        if (cli.hasOption('z')) {
            zookeeperHosts = cli.getOptionValue('z')
        }

        if (cli.hasOption('H')) {
            statsdHost = cli.getOptionValue('H')
        }

        if (cli.hasOption('p')) {
            statsdPort = cli.getOptionValue('p')
        }

        logger = LoggerFactory.getLogger(Main.class)
        logger.info("Running with: ${args}")
        logger.warn("Using: zookeepers=${zookeeperHosts} statsd=${statsdHost}:${statsdPort}")

        if (cli.hasOption('prefix')) {
            statsdPrefix = "${cli.getOptionValue('prefix')}.${METRICS_PREFIX}"
        }


        registry.register(MetricRegistry.name(Main.class, 'heartbeat'),
                            new metrics.HeartbeatGauge())

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperHosts, retry)
        client.start()

        /* We need a good shared set of all the topics we should keep an eye on
         * for the Kafka poller. This will be written to by the tree watchers
         * and read from by the poller, e.g.
         *      Watcher --/write/--> watchedTopics --/read/--> KafkaPoller
         */
        ConcurrentSkipListSet<String> watchedTopics = new ConcurrentSkipListSet<>()

        /* consumerOffsets is where we will keep all the offsets from Zookeeper
         * from the Kafka consumers
         */
        ConcurrentHashMap<KafkaConsumer, Integer> consumerOffsets = new ConcurrentHashMap<>()

        /* topicOffsets is where the KafkaPoller should be writing all of it's
         * latest offsets from querying the Kafka brokers
         */
        ConcurrentHashMap<TopicPartition, Long> topicOffsets = new ConcurrentHashMap<>()

        /* Hash map for keeping track of KafkaConsumer to ConsumerGauge
         * instances. We're only really doing this because the MetricRegistry
         * doesn't do a terrific job of exposing this for us
         */
        ConcurrentHashMap<KafkaConsumer, ConsumerGauge> consumerGauges = new ConcurrentHashMap<>()


        KafkaPoller poller = new KafkaPoller(topicOffsets, watchedTopics)
        BrokerTreeWatcher brokerWatcher = new BrokerTreeWatcher(client).start()
        brokerWatcher.onBrokerUpdates << { brokers -> poller.refresh(brokers) }

        poller.start()

        /* Need to reuse this closure for the KafkaSpoutTreeWatcher if we have
         * one
         */
        Closure gaugeRegistrar = { KafkaConsumer consumer ->
            registerMetricFor(consumer, consumerGauges, consumerOffsets, topicOffsets)
        }

        StandardTreeWatcher consumerWatcher = new StandardTreeWatcher(client,
                                                                      watchedTopics,
                                                                      consumerOffsets)
        consumerWatcher.onConsumerData << gaugeRegistrar
        consumerWatcher.start()


        /* Assuming that most people aren't needing to run Storm-based watchers
         * as well
         */
        if (cli.hasOption('s')) {
            KafkaSpoutTreeWatcher stormWatcher = new KafkaSpoutTreeWatcher(client,
                                                                           watchedTopics,
                                                                           consumerOffsets)
            stormWatcher.onConsumerData << gaugeRegistrar
            stormWatcher.start()
        }


        if (cli.hasOption('n')) {
            reporter = ConsoleReporter.forRegistry(registry)
                                      .convertRatesTo(TimeUnit.SECONDS)
                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                      .build()
        }

        /* Start the reporter if we've got it */
        reporter?.start(1, TimeUnit.SECONDS)

        logger.info("Starting wait loop...")
        while (true) {
            Thread.sleep(1 * 1000)
        }

        logger.info("exiting..")
        poller.die()
        poller.join()

        return
    }

    static void registerMetricFor(KafkaConsumer consumer,
                                  ConcurrentHashMap<KafkaConsumer, ConsumerGauge> consumerGauges,
                                  ConcurrentHashMap<KafkaConsumer, Integer> consumerOffsets,
                                  ConcurrentHashMap<TopicPartition, Long> topicOffsets) {
        if (consumerGauges.containsKey(consumer)) {
            return
        }

        ConsumerGauge gauge = new ConsumerGauge(consumer,
                                                consumerOffsets,
                                                topicOffsets)
        this.registry.register(gauge.name, gauge)
    }


    /**
     * Create the Options option necessary for verspaetung to have CLI options
     */
    static Options createCLI() {
        Options options = new Options()

        Option zookeeper = OptionBuilder.withArgName('HOSTS')
                                        .hasArg()
                                        .withDescription('Comma separated list of Zookeeper hosts (e.g. localhost:2181)')
                                        .withLongOpt('zookeeper')
                                        .withValueSeparator(',' as char)
                                        .create('z')

        Option statsdHost = OptionBuilder.withArgName('STATSD')
                                         .hasArg()
                                         .withType(String)
                                         .withDescription('Hostname for a statsd instance (defaults to localhost)')
                                         .withLongOpt('statsd-host')
                                         .create('H')

        Option statsdPort = OptionBuilder.withArgName('PORT')
                                         .hasArg()
                                         .withType(Integer)
                                         .withDescription('Port for the statsd instance (defaults to 8125)')
                                         .withLongOpt('statsd-port')
                                         .create('p')

        Option dryRun = OptionBuilder.withDescription('Disable reporting to a statsd host')
                                     .withLongOpt('dry-run')
                                     .create('n')

        Option stormSpouts = OptionBuilder.withDescription('Watch Storm KafkaSpout offsets (under /kafka_spout)')
                                          .withLongOpt('storm')
                                          .create('s')

        Option statsdPrefix = OptionBuilder.withArgName('PREFIX')
                                           .hasArg()
                                           .withType(String)
                                           .withDescription("Prefix all metrics with PREFIX before they're reported (e.g. PREFIX.verspaetung.mytopic)")
                                           .withLongOpt('prefix')
                                           .create()

        options.addOption(zookeeper)
        options.addOption(statsdHost)
        options.addOption(statsdPort)
        options.addOption(statsdPrefix)
        options.addOption(dryRun)
        options.addOption(stormSpouts)

        return options
    }


    /**
     * Parse out all the command line options from the array of string
     * arguments
     */
    static CommandLine parseCommandLine(String[] args) {
        Options options = createCLI()
        PosixParser parser = new PosixParser()

        try {
            return parser.parse(options, args)
        }
        catch (MissingOptionException|UnrecognizedOptionException ex) {
            HelpFormatter formatter = new HelpFormatter()
            println ex.message
            formatter.printHelp('verspaetung', options)
            System.exit(1)
        }
    }
}
