package com.github.lookout.verspaetung.metrics

import com.codahale.metrics.*

/**
 * A simple gauge that will always just return 1 indicating that the process is
 * alive
 */
class HeartbeatGauge implements Gauge<Integer> {
    @Override
    Integer getValue() {
        return 1
    }
}
