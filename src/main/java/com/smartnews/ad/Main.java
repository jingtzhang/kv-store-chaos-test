package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.KvStoreClient;

public class Main {
    public static void main(String[] args) throws KvStoreClient.ClientInitializationException, InterruptedException {
        KvStoreClient kvStoreClient = new KvStoreClient("jingtong_test",
                "kv-stg-read-proxy-nlb.dynamic-ads.smartnews.net", 9000,
                "kv-stg-write-proxy-nlb.dynamic-ads.smartnews.net", 9001);
        Chaos chaos = new Chaos(kvStoreClient, 60);
        chaos.keepQuerying(50, 500, 0, 4);
    }
}
