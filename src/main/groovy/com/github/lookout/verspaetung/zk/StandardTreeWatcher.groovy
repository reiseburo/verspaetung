package com.github.lookout.verspaetung.zk

import org.apache.curator.framework.recipes.cache.ChildData

/**
 * StandardTreeWatcher processes Zookeeper paths for standard high-level Kafka
 * consumers
 */
class StandardTreeWatcher extends AbstractTreeWatcher {

    /**
     * Extract the necessary information from a standard (i.e. high-level Kafka
     * consumer) tree of offsets
     */
    ConsumerOffset processChildData(ChildData data) {
        if (data == null) {
            return null
        }
        /*
ChildData{path='/consumers/offtopic-spock-test/offsets/topic/7', stat=8595174473,8595174478,1416808804928,1416808805262,1,0,0,0,1,0,8595174473, data=[48]}
        */
        ConsumerOffset offset = new ConsumerOffset()

        List pathParts = data.path.split(/\//)

        if (pathParts.size != 6) {
            return null
        }

        offset.groupName = pathParts[2]
        offset.topic = pathParts[4]
        offset.partition = new Integer(pathParts[5])
        offset.offset = new Integer(new String(data.data))

        return offset
    }
}
