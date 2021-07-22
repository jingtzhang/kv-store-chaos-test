package com.smartnews.ad;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.lettuce.core.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
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
    RedisClusterClient clusterClient;
    RedisClusterClient asyncClient;
    StatefulRedisClusterPubSubConnection<String, String> connection;
    StatefulRedisClusterPubSubConnection<String, String> asyncConnection;


    @Before
    public void setUp() {
        k8sClient = new DefaultKubernetesClient();
//        Endpoints endpoints = k8sClient.endpoints().inNamespace("dynamic-ads").withName("etcd").get();
//        System.out.println(endpoints.getSubsets().get(0).getAddresses().get(0).getIp());
//        Service myService = k8sClient.services().inNamespace("dynamic-ads").withName("etcd-client").get();
//        System.out.println(myService);
//        String etcdHostName = myService.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
//        Integer port = myService.getSpec().getPorts().get(0).getPort();
//        System.out.println(etcdHostName+":"+port);
        Endpoints redisEndpoints = k8sClient.endpoints().inNamespace("dynamic-ads").withName("redis-cluster-stg-headless").get();
        List<EndpointAddress> addresses = redisEndpoints.getSubsets().get(0).getAddresses();
        addresses.forEach((address) ->
            System.out.println(address.getIp()));
//        List<String> redisSeeds = new ArrayList<>();
//        redisSeeds.add(redisEndpoints.getSubsets().get(0).getAddresses().get(0).getIp());
//        redisSeeds.add(redisEndpoints.getSubsets().get(0).getAddresses().get(1).getIp());
//        redisSeeds.add(redisEndpoints.getSubsets().get(0).getAddresses().get(2).getIp());
//        RedisURI node1 = RedisURI.create(redisSeeds.get(0), 6379);
//        RedisURI node2 = RedisURI.create(redisSeeds.get(1), 6379);
//        RedisURI node3 = RedisURI.create(redisSeeds.get(2), 6379);

//        List<RedisURI> uris = new ArrayList<RedisURI>(){{
//            add(RedisURI.create("10.1.131.101", 6379));
//            add(RedisURI.create("10.1.129.21", 6379));
//            add(RedisURI.create("10.1.131.39", 6379));
//            add(RedisURI.create("10.1.128.179", 6379));
//            add(RedisURI.create("10.1.131.181", 6379));
//            add(RedisURI.create("10.1.129.233", 6379));
//        }};
//        clusterClient = RedisClusterClient.create(uris);
//        asyncClient = RedisClusterClient.create(uris);
//        connection = clusterClient.connectPubSub();
//        asyncConnection = asyncClient.connectPubSub();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
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

        RedisClusterPubSubAsyncCommands<String, String> async = asyncConnection.async();
        while(true) {
            async.publish("test-channel", UUID.randomUUID().toString()).get();
            sleep(2000);
        }
    }

    @After
    public void tearDown() {
        connection.close();
        clusterClient.shutdown();
    }
}
