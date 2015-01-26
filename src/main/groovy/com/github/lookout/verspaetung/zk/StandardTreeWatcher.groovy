package com.github.lookout.verspaetung.zk

import groovy.transform.TypeChecked
import groovy.transform.InheritConstructors
import org.apache.curator.framework.recipes.cache.ChildData

/**
 * StandardTreeWatcher processes Zookeeper paths for standard high-level Kafka
 * consumers
 */
@TypeChecked
@InheritConstructors
class StandardTreeWatcher extends AbstractTreeWatcher {

    /**
     * Extract the necessary information from a standard (i.e. high-level Kafka
     * consumer) tree of offsets
     */
    ConsumerOffset processChildData(ChildData data) {
        if (data == null) {
            return null
        }

        /* There are non-offset related subtrees in /consumers that we don't
         * care about, let's just skip over them
         */
        if (!isOffsetSubtree(data.path)) {
            return null
        }

        /*
ChildData{path='/consumers/offtopic-spock-test/offsets/topic/7', stat=8595174473,8595174478,1416808804928,1416808805262,1,0,0,0,1,0,8595174473, data=[48]}
        */
        ConsumerOffset offset = new ConsumerOffset()

        List<String> pathParts = data.path.split(/\//) as List<String>

        if (pathParts.size() != 6) {
            return null
        }

        offset.groupName = pathParts[2]
        offset.topic = pathParts[4]

        try {
            offset.partition = new Integer(pathParts[5])
            offset.offset = new Integer(new String(data.data))
        }
        catch (NumberFormatException ex) {
            logger.error("Failed to parse an Integer: ${data}")
            return null
        }

        return offset
    }


    Boolean isOffsetSubtree(String path) {
        return (path =~ /\/consumers\/(.*)\/offsets\/(.*)/)
    }

}
