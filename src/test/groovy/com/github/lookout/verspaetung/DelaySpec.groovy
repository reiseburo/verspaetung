package com.github.lookout.verspaetung

import spock.lang.*

class DelaySpec extends Specification {
    Delay delay = new Delay()
    
    def "it should give default on first use"() {
        given:

        expect:
        delay.value() == Delay.POLLER_DELAY_MIN
    }

    def "slower has an upper bound"() {
        given:
        for(int i = 1; i < 20; i++) { delay.slower() }
        def firstLast = delay.value()
        def result = delay.slower()
        def secondLast = delay.value()
        
        expect:
        firstLast == secondLast
        firstLast == Delay.POLLER_DELAY_MAX
        result == false
    }

    def "increasing delay gives true"() {
        def result = true
        for(int i = 1; delay.value() < Delay.POLLER_DELAY_MAX; i++) {
            result = result && delay.slower()
        }
        def last = delay.slower()

        expect:
        result == true
        last == false
    }

    def "reset on min value gives false"() {
        given:
        def result = delay.reset()
        
        expect:
        result == false
    }
    def "reset on none min value gives true"() {
        given:
        delay.slower()
        def result = delay.reset()
        
        expect:
        result == true
    }
}
