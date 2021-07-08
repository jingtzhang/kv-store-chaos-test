package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.KvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ChaosTest {

    private Chaos chaos;

    @Before
    public void setUp() throws KvStoreClient.ClientInitializationException {
        KvStoreClient kvStoreClient = new KvStoreClient("jingtong_test",
                "kv-stg-read-proxy-nlb.dynamic-ads.smartnews.net", 9000,
                "kv-stg-write-proxy-nlb.dynamic-ads.smartnews.net", 9001);
        chaos = new Chaos(kvStoreClient, 20);
    }

    @Test
    public void write() throws SNKVStoreException {
        chaos.batchWriteKkv(3600 * 12);
    }

    @Test
    public void query() throws InterruptedException {
        chaos.keepQuerying(200, 500, 5, 6);
    }

}
