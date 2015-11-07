package com.github.reiseburo.verspaetung


import spock.lang.*

/**
 */
class MainSpec extends Specification {
    def "shouldExcludeGroups() shuold return false by default"() {
        expect:
        Main.shouldExcludeConsumer(new String[0], null) == false
    }

    def "shouldExcludeGroups() with a matching exclude should return true"() {
        given:
        final String[] groups = ['foo'] as String[]
        final KafkaConsumer consumer = new KafkaConsumer('spock-topic', 0, 'foo')

        expect:
        Main.shouldExcludeConsumer(groups, consumer)
    }

    def "shouldExcludeGroups() with a matching regex should return true"() {
        given:
        final String[] groups = ['.*foo'] as String[]
        final KafkaConsumer consumer = new KafkaConsumer('spock-topic', 0, 'spockfoo')

        expect:
        Main.shouldExcludeConsumer(groups, consumer)
    }

    def "shouldExcludeGroups() with multiple regexes that don't match should return false"() {
        given:
        final String[] groups = ['.*foo', 'bar(.*)'] as String[]
        final KafkaConsumer consumer = new KafkaConsumer('spock-topic', 0, 'spock')

        expect:
        Main.shouldExcludeConsumer(groups, consumer) == false
    }

    def "shouldExcludeGroups() with multiple regexes which match should return true"() {
        given:
        final String[] groups = ['.*foo', 'bar(.*)'] as String[]
        final KafkaConsumer consumer = new KafkaConsumer('spock-topic', 0, 'barstool')

        expect:
        Main.shouldExcludeConsumer(groups, consumer)
    }
}
