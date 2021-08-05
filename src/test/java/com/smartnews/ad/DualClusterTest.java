package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.ProxyKvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import com.smartnews.ad.dynamic.kvstore.proto.proxy.Proxy;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;

public class DualClusterTest {
    private ProxyKvStoreClient client;

    @Before
    public void setUp() throws ProxyKvStoreClient.ProxyClientInitializationException {
        client = ProxyKvStoreClient.ProxyKvStoreClientBuilder.builder().withRedisProxyHost("kv-i2i-pure-proxy-nlb.dynamic-ads.smartnews.net").withRedisProxyPort(9000).build();
    }

    @Test
    public void write() {
        for (int k = 0; k < 50; k++) {
            Map<String, byte[]> mockData = new HashMap<>();
            for(int i = 0; i < 2000; i++) {
                mockData.put("jingtong_test" + (i + k * 2000), RandomStringUtils.random(400, true, true).getBytes(StandardCharsets.UTF_8));
            }
            try {
                Proxy.BatchWriteRsp batchWriteRsp = client.batchWrite(mockData, 3600*12);
                assert batchWriteRsp.getStatus() == Proxy.BatchWriteRsp.Status.SUCCESS;
            } catch (SNKVStoreException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void query() throws InterruptedException {
        int intervalNum = 100;
        int batchSize = 50;
        int intervalMs = 50;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5000000), new DiscardOldestPolicyImpl());

        AtomicLong seq = new AtomicLong();
        Random random = new Random(System.currentTimeMillis());
        AtomicLong successNum = new AtomicLong();
        AtomicLong errorNum = new AtomicLong();
        while (true) {
            for (int i = 0; i < intervalNum; i++) {
                List<String> list = new ArrayList<>();
                for (int j = 0; j < batchSize; j++) {
                    list.add("jingtong_test" + random.nextInt(100000));
                }
                executor.submit(() -> {
                    List<byte[]> bytes = null;
                    try {
                        bytes = client.batchRead(list, 1000);
                        successNum.getAndIncrement();
                    } catch (Exception e) {
                        errorNum.getAndIncrement();
                    }
                    seq.getAndIncrement();
                    if (seq.get() == 10000) {
                        System.out.println("Error rate in this 10000 request is: " + errorNum.get() / 10000.);
                        seq.getAndSet(0);
                        successNum.getAndSet(0);
                        errorNum.getAndSet(0);
                    }
                    assert !bytes.isEmpty();
                    return bytes;
                });
            }
            if (intervalMs > 0)
                sleep(intervalMs);
        }
    }

    private static class DiscardOldestPolicyImpl implements RejectedExecutionHandler {
        public DiscardOldestPolicyImpl() {
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                executor.getQueue().poll();
                System.out.println("Executor discard oldest task...");
                executor.execute(r);
            } else {
                System.out.println("Executor shutdown...");
            }
        }
    }
}
