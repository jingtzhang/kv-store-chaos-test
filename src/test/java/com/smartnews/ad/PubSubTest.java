package com.smartnews.ad;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.lettuce.core.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;

public class PubSubTest {

    KubernetesClient k8sClient;
    StatefulRedisClusterPubSubConnection<String, String> connection;
    StatefulRedisClusterPubSubConnection<String, String> asyncConnection;
    RedisClusterPubSubCommands<String, String> sync;
    RedisClusterPubSubAsyncCommands<String, String> async;


    @Before
    public void setUp() {
        // Retrieve IP Addresses of Redis Cluster Pods
        k8sClient = new DefaultKubernetesClient();
        Endpoints redisEndpoints = k8sClient.endpoints().inNamespace("dynamic-ads").withName("redis-cluster-stg-headless").get();
        List<EndpointAddress> addresses = redisEndpoints.getSubsets().get(0).getAddresses();
        List<RedisURI> uris = new ArrayList<>();
        System.out.println("Redis Cluster IP Addresses:");
        addresses.forEach((address) -> {
            System.out.println(address.getIp());
            uris.add(RedisURI.create(address.getIp(), 6379));
        });

        // Cannot subscribe and publish at the same connection!
        // Create a connection for subscriber
        connection = RedisClusterClient.create(uris).connectPubSub();
        sync = connection.sync();

        // Create a connection for publisher
        asyncConnection = RedisClusterClient.create(uris).connectPubSub();
        async = asyncConnection.async();

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
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        sync.subscribe("test-channel");

        while(true) {
            async.publish("test-channel", UUID.randomUUID().toString()).get();
            sleep(2000);
        }
    }

    @After
    public void tearDown() {
        connection.close();
        asyncConnection.close();
        k8sClient.close();
    }
}
