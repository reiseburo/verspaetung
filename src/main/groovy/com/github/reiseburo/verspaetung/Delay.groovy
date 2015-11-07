package com.github.reiseburo.verspaetung

/**
 * abstract the logic on how to reduce the polling speed and get back to
 * to full speed.
 */
class Delay {
    static final Integer POLLER_DELAY_MIN = (1 * 1000)
    static final Integer POLLER_DELAY_MAX = (2048 * 1000) // about half an hour

    private Integer delay = POLLER_DELAY_MIN

    boolean reset() {
        if (delay != POLLER_DELAY_MIN) {
            delay = POLLER_DELAY_MIN
            true
        }
        else {
            false
        }
    }

    boolean slower() {
        if (delay < POLLER_DELAY_MAX) {
            delay += delay
            true
        }
        else {
            false
        }
    }

    Integer value() {
        delay
    }

    String toString() {
        "Delay[ ${delay / 1000} sec ]"
    }
}
