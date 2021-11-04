package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.KvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ChaosTest {

    private static Chaos chaos;

    @BeforeAll
    public static void setUp() throws KvStoreClient.ClientInitializationException {
        KvStoreClient kvStoreClient = new KvStoreClient("jingtong_test",
                "kv-stg-read-proxy-nlb.dynamic-ads.smartnews.net", 9000,
                "kv-stg-write-proxy-nlb.dynamic-ads.smartnews.net", 9001);
        chaos = new Chaos(kvStoreClient, 60);
    }

    @Test
    public void write() throws SNKVStoreException {
        chaos.batchWriteKkv(36000, 100);
    }

    @Test
    public void query() throws InterruptedException {
        chaos.keepQuerying(100, 100, 1000, 16);
    }

    @Test
    public void single() throws SNKVStoreException {
        chaos.singleReadKKV();
    }

}
