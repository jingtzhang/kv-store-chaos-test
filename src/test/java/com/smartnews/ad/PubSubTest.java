package com.smartnews.ad;
import io.lettuce.core.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PubSubTest {

    RedisClusterClient clusterClient;
    StatefulRedisClusterPubSubConnection<String, String> connection;

    @Before
    public void setUp() {
        List<RedisURI> uris = new ArrayList<RedisURI>(){{
            add(RedisURI.create("10.1.128.179", 6379));
            add(RedisURI.create("10.1.129.233", 6379));
            add(RedisURI.create("10.1.131.181", 6379));
            add(RedisURI.create("10.1.130.120", 6379));
            add(RedisURI.create("10.1.129.21", 6379));
            add(RedisURI.create("10.1.131.39", 6379));
        }};
        clusterClient = RedisClusterClient.create(uris);
        connection = clusterClient.connectPubSub();
    }

    @Test
    public void test() {
        connection.addListener(new RedisClusterPubSubListener<String, String>() {
            @Override
            public void message(RedisClusterNode redisClusterNode, String s, String s2) {
                System.out.println("Receive message: " + s2 + " from channel: " + s);
            }

            @Override
            public void message(RedisClusterNode redisClusterNode, String s, String k1, String s2) {

            }

            @Override
            public void subscribed(RedisClusterNode redisClusterNode, String s, long l) {
                System.out.println("Subscribe to " + s);
            }

            @Override
            public void psubscribed(RedisClusterNode redisClusterNode, String s, long l) {

            }

            @Override
            public void unsubscribed(RedisClusterNode redisClusterNode, String s, long l) {

            }

            @Override
            public void punsubscribed(RedisClusterNode redisClusterNode, String s, long l) {

            }
        });
        RedisPubSubCommands<String, String> sync = connection.sync();
        sync.subscribe("test-channel");
    }

    @After
    public void tearDown() {
        connection.close();
        clusterClient.shutdown();
    }
}
