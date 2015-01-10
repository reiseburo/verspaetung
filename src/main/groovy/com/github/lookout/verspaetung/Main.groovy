package com.github.lookout.verspaetung

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.KeeperException

class Main {
    static void main(String[] args) {
        println "Running!"

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3)
        def client = CuratorFrameworkFactory.newClient('lookout-zk-stage1-0.flexilis.org:2181', retry)
        client.start()

        client.children.forPath('/consumers').each { path ->
            try {
                client.children.forPath("/consumers/${path}/offsets").each { topic ->
                    client.children.forPath("/consumers/${path}/offsets/${topic}").each { partition ->
                        String offset = new String(client.data.forPath("/consumers/${path}/offsets/${topic}/${partition}"))
                        println "${path}:${topic}:${partition} = ${offset}"
                    }
                }
            }
            catch (KeeperException ex) {
                println "no offsets for ${path}"
            }
        }

        println client
    }
}
