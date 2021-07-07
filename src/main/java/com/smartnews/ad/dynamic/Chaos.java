package com.smartnews.ad.dynamic;

import com.smartnews.ad.dynamic.kvstore.client.Key;
import com.smartnews.ad.dynamic.kvstore.client.KvStoreClient;
import com.smartnews.ad.dynamic.kvstore.client.SNKVStoreException;
import com.smartnews.ad.dynamic.kvstore.proto.Write;
import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

import static java.lang.Thread.sleep;

@Slf4j
public class Chaos {
    private static final int BATCH_SIZE = 10000;

    private static final int BATCH_NUM = 10;

    private static final int RETRY_TIME = 3;

    private final KvStoreClient kvStoreClient;

    public Chaos(KvStoreClient kvStoreClient) {
        this.kvStoreClient = kvStoreClient;
    }

    private void retryableBatchWriteKKV(Map<Key, Map<String, byte[]>> kkvs, String pid, int ts, int ttl, boolean last_batch) throws SNKVStoreException {
        String errMsg = "";
        int retry = RETRY_TIME;
        while (retry > 0) {
            try {
                Write.BatchWriteRsp batchWriteRsp = kvStoreClient.batchWriteKKV(kkvs, pid, ts, ttl, last_batch);
                if (batchWriteRsp.getStatus() == Write.BatchWriteRsp.Status.SUCCESS) {
                    log.info("Finished upload batch kkv size: " + kkvs.size());
                    return;
                } else {
                    throw new SNKVStoreException("Return status is abnormal: " + batchWriteRsp.getStatus());
                }
            } catch (Exception e) {
                retry--;
                errMsg = e.getMessage();
                log.error(errMsg);
                log.warn("Update kv-store batch kkv failed... Retry " + retry);
            }
        }
        throw new SNKVStoreException("KV-Store update batch kkv error " + errMsg);
    }

    public void batchWriteKkv() throws SNKVStoreException {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        Date date = new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);

        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");

        for (int i = 0; i < BATCH_NUM; i++) {
            // kkvs = {
            // ...
            // class{"jingtong", "item66", ""}: {"cvr": cvr66.toBytes(), "ctr": ctr66.toBytes(), "embedding": embedding66..toBytes()},
            // class{"jingtong", "item66", ""}: {"cvr": cvr66.toBytes(), "ctr": ctr66.toBytes(), "embedding": embedding66..toBytes()}
            // class{"jingtong", "item66", ""}: {"cvr": cvr66.toBytes(), "ctr": ctr66.toBytes(), "embedding": embedding66..toBytes()}
            // ...
            // }
            Map<Key, Map<String, byte[]>> kkvs = new HashMap<>();
            for (int j = i * BATCH_SIZE; j < (i + 1) * BATCH_SIZE; j++) {
                Map<String, byte[]> kvs = new HashMap<>();
                for (String field : fields) {
                    kvs.put(field, (field + j).getBytes(StandardCharsets.UTF_8));
                }
                kkvs.put(new Key("jingtong_test", "item" + j, ""), kvs);
            }
            retryableBatchWriteKKV(kkvs, pid, (int) ts.getTime(), 100, i != BATCH_NUM - 1);
        }
        log.info("Batch write kkv " + BATCH_SIZE * BATCH_NUM + " finished.");
    }

    public void keepQuerying(int batchSize, int intervalNum, int interval) throws InterruptedException {
        long seq = 0;
        Random random = new Random();

        List<String> fields = new ArrayList<>();
        fields.add("cvr");
        fields.add("ctr");
        fields.add("embedding");

        while (true) {
            for (int i = 0; i < intervalNum; i++) {
                List<Key> list = new ArrayList<>();
                for (int j = 0; j < batchSize; j++) {
                    list.add(new Key("jingtong_test", "item" + random.nextInt(BATCH_SIZE * BATCH_NUM), ""));
                }
                try {
                    long startTime = System.nanoTime();
                    Map<String, Map<String, byte[]>> stringMapMap = kvStoreClient.batchReadKKV(list, fields, 100);
                    long elapsedTime = System.nanoTime() - startTime;
                    assert stringMapMap.size() == batchSize;
                    log.info("{} Time batch read kkv successfully with time elapse {}", seq, elapsedTime);
                    if (seq >= Long.MAX_VALUE) {
                        log.error("Time to terminate");
                        return;
                    }
                } catch (Exception e) {
                    log.error("{} Time batch read kkv failed", seq);
                }
                seq++;
            }
            sleep(interval);
        }

    }

}
