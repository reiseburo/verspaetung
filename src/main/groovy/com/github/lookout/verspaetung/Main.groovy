package com.github.lookout.verspaetung

import com.github.lookout.verspaetung.zk.BrokerTreeWatcher
import com.github.lookout.verspaetung.zk.KafkaSpoutTreeWatcher
import com.github.lookout.verspaetung.zk.StandardTreeWatcher

import java.util.AbstractMap
import java.util.concurrent.ConcurrentHashMap
import groovy.transform.TypeChecked

import com.timgroup.statsd.StatsDClient
import com.timgroup.statsd.NonBlockingDogStatsDClient

import org.apache.commons.cli.*
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Main {
    private static final String METRICS_PREFIX = 'verspaetung'

    private static StatsDClient statsd
    private static Logger logger

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

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperHosts, retry)
        ConcurrentHashMap<TopicPartition, List<zk.ConsumerOffset>> consumers = new ConcurrentHashMap()

        statsd = new NonBlockingDogStatsDClient(statsdPrefix, statsdHost, statsdPort)

        client.start()

        KafkaPoller poller = setupKafkaPoller(consumers, statsd, cli.hasOption('n'))
        BrokerTreeWatcher brokerWatcher = new BrokerTreeWatcher(client).start()
        StandardTreeWatcher consumerWatcher = new StandardTreeWatcher(client, consumers).start()

        /* Assuming that most people aren't needing to run Storm-based watchers
         * as well
         */
        if (cli.hasOption('s')) {
            KafkaSpoutTreeWatcher stormWatcher = new KafkaSpoutTreeWatcher(client, consumers)
            stormWatcher.start()
        }

        consumerWatcher.onInitComplete << {
            logger.info("standard consumers initialized to ${consumers.size()} (topic, partition) tuples")
        }

        brokerWatcher.onBrokerUpdates << { brokers ->
            poller.refresh(brokers)
        }

        logger.info("Started wait loop...")

        while (true) {
            statsd?.recordGaugeValue('heartbeat', 1)
            Thread.sleep(1 * 1000)
        }

        logger.info("exiting..")
        poller.die()
        poller.join()
        return
    }


    /**
     * Create and start a KafkaPoller with the given statsd and consumers map
     */
    static KafkaPoller setupKafkaPoller(AbstractMap consumers,
                                        NonBlockingDogStatsDClient statsd,
                                        Boolean dryRun)  {
        KafkaPoller poller = new KafkaPoller(consumers)
        Closure deltaCallback = { String name, TopicPartition tp, Long delta ->
            println "${tp.topic}:${tp.partition}-${name} = ${delta}"
        }

        if (!dryRun) {
            deltaCallback = { String name, TopicPartition tp, Long delta ->
            statsd.recordGaugeValue(tp.topic, delta, [
                                                    'topic' : tp.topic,
                                                    'partition' : tp.partition,
                                                    'consumer-group' : name
                ])
            }
        }

        poller.onDelta << deltaCallback
        poller.start()
        return poller
    }

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
