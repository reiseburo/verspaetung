package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher
import com.github.lookout.verspaetung.zk.KafkaSpoutTreeWatcher
import com.github.lookout.verspaetung.zk.StandardTreeWatcher
import com.github.lookout.verspaetung.metrics.ConsumerGauge
import com.github.lookout.verspaetung.metrics.HeartbeatGauge

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit

import org.apache.commons.cli.*
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.coursera.metrics.datadog.DatadogReporter
import org.coursera.metrics.datadog.transport.UdpTransport
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.codahale.metrics.*

/**
 * Main entry point for running the verspaetung application
 */
@SuppressWarnings
class Main {
    private static final String METRICS_PREFIX = 'verspaetung'

    private static final Logger logger = LoggerFactory.getLogger(Main)
    private static final MetricRegistry registry = new MetricRegistry()
    private static ScheduledReporter reporter

    static void main(String[] args) {
        String statsdPrefix = METRICS_PREFIX
        String zookeeperHosts = 'localhost:2181'
        String statsdHost = 'localhost'
        Integer statsdPort = 8125
        Integer delayInSeconds = 5
        String[] excludeGroups = []

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

        if (cli.hasOption('d')) {
            delayInSeconds = cli.getOptionValue('d').toInteger()
        }

        if (cli.hasOption('x')) {
            excludeGroups = cli.getOptionValues('x')
        }

        logger.info("Running with: ${args}")
        logger.warn('Using: zookeepers={} statsd={}:{}', zookeeperHosts, statsdHost, statsdPort)
        logger.info('Reporting every {} seconds', delayInSeconds)

        if (cli.hasOption('prefix')) {
            statsdPrefix = "${cli.getOptionValue('prefix')}.${METRICS_PREFIX}"
        }

        registry.register(MetricRegistry.name(Main.class, 'heartbeat'),
                            new HeartbeatGauge())

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
            if (!shouldExcludeConsumer(excludeGroups, consumer)) {
                registerMetricFor(consumer, consumerGauges, consumerOffsets, topicOffsets)
            }
        }

        StandardTreeWatcher consumerWatcher = new StandardTreeWatcher(client,
                                                                      watchedTopics,
                                                                      consumerOffsets)
        consumerWatcher.onConsumerData << gaugeRegistrar
        consumerWatcher.start()


        /* Assuming that most people aren't needing to run Storm-based watchers
         * as well
         */
        KafkaSpoutTreeWatcher stormWatcher = null
        if (cli.hasOption('s')) {
            stormWatcher = new KafkaSpoutTreeWatcher(client,
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
        else {
            UdpTransport transport = new UdpTransport.Builder()
                                                     .withPrefix(statsdPrefix)
                                                     .build()

            reporter = DatadogReporter.forRegistry(registry)
                                      .withEC2Host()
                                      .withTransport(transport)
                                      .build()
        }

        /* Start the reporter if we've got it */
        reporter?.start(delayInSeconds, TimeUnit.SECONDS)

        // shutdown threads
        Runtime.getRuntime().addShutdownHook(new Thread() {
                                                 public void run() {
                                                     Main.logger.info("showdown threads")
                                                     poller.die()
                                                     consumerWatcher.close()
                                                     if (stormWatcher != null) {
                                                         stormWatcher.close()
                                                     }
                                                     poller.join()
                                                 }
                                             });
        logger.info('Starting wait loop...')
        synchronized(this) {
            wait()
        }
    }

    static void registerMetricFor(KafkaConsumer consumer,
                                  ConcurrentHashMap<KafkaConsumer, ConsumerGauge> consumerGauges,
                                  ConcurrentHashMap<KafkaConsumer, Integer> consumerOffsets,
                                  ConcurrentHashMap<TopicPartition, Long> topicOffsets) {
        /*
         * Bail early if we already ahve our Consumer registered
         */
        if (consumerGauges.containsKey(consumer)) {
            return
        }

        ConsumerGauge gauge = new ConsumerGauge(consumer,
                                                consumerOffsets,
                                                topicOffsets)
        consumerGauges.put(consumer, gauge)
        this.registry.register(gauge.nameForRegistry, gauge)
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

        Option excludeGroups = OptionBuilder.withArgName('EXCLUDES')
                                            .hasArgs()
                                            .withDescription('Regular expression for consumer groups to exclude from reporting (can be declared multiple times)')
                                            .withLongOpt('exclude')
                                            .create('x')

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

        Option delaySeconds = OptionBuilder.withArgName('DELAY')
                                           .hasArg()
                                           .withType(Integer)
                                           .withDescription("Seconds to delay between reporting metrics to the metrics receiver (defaults: 5s)")
                                           .withLongOpt('delay')
                                           .create('d')

        options.addOption(zookeeper)
        options.addOption(statsdHost)
        options.addOption(statsdPort)
        options.addOption(statsdPrefix)
        options.addOption(dryRun)
        options.addOption(stormSpouts)
        options.addOption(delaySeconds)
        options.addOption(excludeGroups)

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

    /**
     * Return true if we should exclude the given KafkaConsumer from reporting
     */

    static boolean shouldExcludeConsumer(String[] excludeGroups, KafkaConsumer consumer) {
        return null != excludeGroups?.find { String excludeRule -> consumer?.name.matches(excludeRule) }
    }
}
