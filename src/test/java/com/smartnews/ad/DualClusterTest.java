package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.ProxyKvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import com.smartnews.ad.dynamic.kvstore.proto.proxy.Proxy;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;

public class DualClusterTest {
    static private ProxyKvStoreClient client;

    @BeforeAll
    static void setUp() throws ProxyKvStoreClient.ProxyClientInitializationException {
        client = ProxyKvStoreClient.ProxyKvStoreClientBuilder.builder().withRedisProxyHost("kv-i2i-pure-proxy-nlb.dynamic-ads.smartnews.net").withRedisProxyPort(9000).build();
    }

    @Test
    public void writekv() {
        int writeBatchSize = 2000;
        int writeBatchNum = 50;
        int ttl = 3600*12;
        for (int k = 0; k < writeBatchNum; k++) {
            Map<String, byte[]> mockData = new HashMap<>();
            for(int i = 0; i < writeBatchSize; i++) {
                mockData.put("jingtong_test" + (i + k * writeBatchSize), RandomStringUtils.random(400, true, true).getBytes(StandardCharsets.UTF_8));
            }
            try {
                Proxy.BatchWriteRsp batchWriteRsp = client.batchWrite(mockData, ttl);
                assert batchWriteRsp.getStatus() == Proxy.BatchWriteRsp.Status.SUCCESS;
            } catch (SNKVStoreException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Batch write kv " + writeBatchSize * writeBatchNum + " finished.");
    }

    @Test
    public void readkv() throws InterruptedException {
        int intervalNum = 100;
        int batchSize = 100;
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
                    if (bytes.size() != batchSize) {
                        System.out.println(bytes.size());
                        errorNum.getAndIncrement();
                        return null;
                    }
                    boolean isError = false;
                    for (byte[] byteArray: bytes) {
                        if (byteArray == null || byteArray.length <= 0) {
                            isError = true;
                        }
                    }
                    if (isError) errorNum.getAndIncrement();
                    seq.getAndIncrement();
                    if (seq.get() == 10000) {
                        System.out.println("Error rate in this 10000 request is: " + errorNum.get() / 10000.);
                        seq.getAndSet(0);
                        successNum.getAndSet(0);
                        errorNum.getAndSet(0);
                    }
                    return bytes;
                });
            }
            if (intervalMs > 0)
                sleep(intervalMs);
        }
    }

    @Test
    public void writekkv() throws SNKVStoreException {
        int batchNum = 60;
        int batchSize = 10000;
        int embeddingSize = 100;
        int ttl = 3600*12;
        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");
        for (int i = 0; i < batchNum; i++) {
            Map<String, Map<String, byte[]>> kkvs = new HashMap<>();
            for (int j = i * batchSize; j < (i + 1) * batchSize; j++) {
                Map<String, byte[]> kvs = new HashMap<>();
                int size = new Random().nextInt(5);
                for (String field : fields) {
                    if (!field.equals("embedding")) {
                        kvs.put(field, (field + RandomStringUtils.random(size + 5, true, true)).getBytes(StandardCharsets.UTF_8));
                    } else {
                        kvs.put(field, RandomStringUtils.random(embeddingSize, true, true).getBytes(StandardCharsets.UTF_8));
                    }
                }
                kkvs.put("jingtong_test_kkv_" + "item" + j, kvs);
            }
            Proxy.BatchWriteRsp batchWriteRsp = client.batchWriteKKV(kkvs, ttl);
            if (batchWriteRsp.getStatus() == Proxy.BatchWriteRsp.Status.SUCCESS) {
                System.out.println("Finished upload batch kkv size: " + kkvs.size());
            } else {
                throw new SNKVStoreException("Return status is abnormal: " + batchWriteRsp.getStatus());
            }
        }
        System.out.println("Batch write kkv " + batchNum * batchNum + " finished.");
    }

    @Test
    public void readkkv() throws InterruptedException {
        int readKkvBatchSize = 100;
        int intervalNum = 100;
        int interval = 150;
        int threadNum = 16;
        int num = 600000;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5000000), new DiscardOldestPolicyImpl());

        AtomicLong seq = new AtomicLong();
        Random random = new Random(System.currentTimeMillis());

        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");

        AtomicLong successNum = new AtomicLong();
        AtomicLong errorNum = new AtomicLong();

        while (true) {
            for (int i = 0; i < intervalNum; i++) {
                Set<String> set = new HashSet<>();
                for (int j = 0; j < readKkvBatchSize; j++) {
                    set.add("jingtong_test_kkv_" + "item" + random.nextInt(num));
                }
                List<String> list = new ArrayList<>(set);
                executor.submit(() -> {
                    Map<String, Map<String, byte[]>> stringMapMap = null;
                    try {
                        stringMapMap = client.batchReadKKV(list, fields, 100);
                        successNum.getAndIncrement();
                    } catch (SNKVStoreException e) {
                        errorNum.getAndIncrement();
                    }
                    if (stringMapMap.size() != list.size()) {
                        System.out.println(stringMapMap.size());
                        errorNum.getAndIncrement();
                        return null;
                    }
                    boolean isError = false;
                    for (Map<String, byte[]> innerMap : stringMapMap.values()) {
                        if (innerMap == null || innerMap.size() != 3 || innerMap.get("cvr") == null || innerMap.get("ctr") == null || innerMap.get("embedding") == null) {
                            isError = true;
                        }
                    }
                    if (isError) errorNum.getAndIncrement();
                    seq.getAndIncrement();
                    if (seq.get() == 10000) {
                        System.out.println("Error rate in this 10000 request is: " + errorNum.get() / 10000.);
                        seq.getAndSet(0);
                        successNum.getAndSet(0);
                        errorNum.getAndSet(0);
                    }
                    return stringMapMap;
                });
            }
            if (interval > 0)
                sleep(interval);
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
