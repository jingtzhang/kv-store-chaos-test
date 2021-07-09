package com.smartnews.ad;

import com.smartnews.ad.dynamic.kvstore.client.Key;
import com.smartnews.ad.dynamic.kvstore.client.KvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import com.smartnews.ad.dynamic.kvstore.proto.Write;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;


public class Chaos {
    private final int batchNum;

    private static final int BATCH_SIZE = 10000;

//    private static final int BATCH_NUM = 10;

    private static final int RETRY_TIME = 3;

    private final KvStoreClient kvStoreClient;

    public Chaos(KvStoreClient kvStoreClient, int batchNum) {
        this.kvStoreClient = kvStoreClient;
        this.batchNum = batchNum;
    }

    private void retryableBatchWriteKKV(Map<Key, Map<String, byte[]>> kkvs, String pid, int ts, int ttl, boolean last_batch) throws SNKVStoreException {
        String errMsg = "";
        int retry = RETRY_TIME;
        while (retry > 0) {
            try {
                Write.BatchWriteRsp batchWriteRsp = kvStoreClient.batchWriteKKV(kkvs, pid, ts, ttl, last_batch);
                if (batchWriteRsp.getStatus() == Write.BatchWriteRsp.Status.SUCCESS) {
                    System.out.println("Finished upload batch kkv size: " + kkvs.size());
                    return;
                } else {
                    throw new SNKVStoreException("Return status is abnormal: " + batchWriteRsp.getStatus());
                }
            } catch (Exception e) {
                retry--;
                errMsg = e.getMessage();
                System.out.println(errMsg);
                System.out.println("Update kv-store batch kkv failed... Retry " + retry);
            }
        }
        throw new SNKVStoreException("KV-Store update batch kkv error " + errMsg);
    }

    public void batchWriteKkv(int ttl, int embeddingSize) throws SNKVStoreException {
        assert embeddingSize < 6 && embeddingSize >= 0;
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        Date date = new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);

        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");

        for (int i = 0; i < batchNum; i++) {
            // kkvs = {
            // ...
            // class{"jingtong", "item66", ""}: {"cvr": cvr66.toBytes(), "ctr": ctr66.toBytes(), "embedding": embedding66..toBytes()},
            // class{"jingtong", "item67", ""}: {"cvr": cvr67.toBytes(), "ctr": ctr67.toBytes(), "embedding": embedding67..toBytes()}
            // class{"jingtong", "item68", ""}: {"cvr": cvr68.toBytes(), "ctr": ctr68.toBytes(), "embedding": embedding68..toBytes()}
            // ...
            // }
            Map<Key, Map<String, byte[]>> kkvs = new HashMap<>();
            for (int j = i * BATCH_SIZE; j < (i + 1) * BATCH_SIZE; j++) {
                Map<String, byte[]> kvs = new HashMap<>();
                for (String field : fields) {
                    if (!field.equals("embedding")) {
                        kvs.put(field, (field + j + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
                    } else {
                        StringBuilder sb = new StringBuilder(field).append(j);
                        for (int k = 0; k < embeddingSize; k++) sb.append(UUID.randomUUID());
                        kvs.put(field, sb.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                kkvs.put(new Key("jingtong_test", "item" + j, ""), kvs);
            }
            retryableBatchWriteKKV(kkvs, pid, (int) ts.getTime(), ttl, i == batchNum - 1);
        }
        System.out.println("Batch write kkv " + BATCH_SIZE * batchNum + " finished.");
    }

    public void keepQuerying(int batchSize, int intervalNum, int interval, int threadNum) throws InterruptedException {
        ExecutorService executor = new ThreadPoolExecutor(threadNum, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        AtomicLong seq = new AtomicLong();
        Random random = new Random(System.currentTimeMillis());

        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");

        AtomicLong successNum = new AtomicLong();
        AtomicLong errorNum = new AtomicLong();
//        long timeSpent = 0;

        while (true) {
            for (int i = 0; i < intervalNum; i++) {
                List<Key> list = new ArrayList<>();
                for (int j = 0; j < batchSize; j++) {
                    list.add(new Key("jingtong_test", "item" + random.nextInt(BATCH_SIZE * batchNum), ""));
                }
                executor.submit(() -> {
                    Map<String, Map<String, byte[]>> stringMapMap = null;
                    try {
                        stringMapMap = kvStoreClient.batchReadKKV(list, fields, 100);
                        successNum.getAndIncrement();
                    } catch (SNKVStoreException e) {
                        errorNum.getAndIncrement();
                        System.out.println("Batch read kkv failed with " + e.getMessage());
                    }
                    seq.getAndIncrement();
                    if (seq.get() % 10000 == 0) {
                        System.out.println("Error rate in this 10000 request is: " + errorNum.get() / 1000.);
                        seq.getAndSet(0);
                        successNum.getAndSet(0);
                        errorNum.getAndSet(0);
                    }
                    return stringMapMap;
                });
//                try {
////                    long startTime = System.nanoTime();
////                    kvStoreClient.batchReadKKV(list, fields, 100);
////                    long elapsedTime = System.nanoTime() - startTime;
////                    long elapsedTimeConvert = TimeUnit.MILLISECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
////                    timeSpent += elapsedTimeConvert;
//                } catch (Exception e) {
//                    errorNum.getAndIncrement();
//                    System.out.println(seq + " Time batch read kkv failed with " + e.getMessage());
//                }
//                seq++;
            }
//            if (seq % 1000 == 0) {
//                System.out.println("Time spent for each request is: " + (double) timeSpent / (successNum.get() + 1) + " ms.");
//                System.out.println("Error rate in this 10000 request is: " + errorNum.get() / 1000.);
//                timeSpent = 0;
//                successNum.set(0);
//                errorNum.set(0);
//            }
            if (interval > 0)
                sleep(interval);
        }

    }

}
