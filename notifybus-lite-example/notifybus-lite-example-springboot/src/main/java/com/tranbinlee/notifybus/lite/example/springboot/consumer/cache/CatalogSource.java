package com.tranbinlee.notifybus.lite.example.springboot.consumer.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟"源头数据"——真实场景下这里应该是一次 DB 查询、一次配置中心读取、一次 RPC 调用等。
 * NotifyBus Lite 本身不传输业务数据，只广播"变了"，消费方必须自己有能力读到最新状态，
 * {@link CatalogCacheListener} 收到触发信号后就是靠这个类"回源"。
 */
public class CatalogSource {

    private final ConcurrentHashMap<String, AtomicLong> versionBySku = new ConcurrentHashMap<>();

    /** 读取某个 SKU 的最新快照，用一个递增的 version + 随机价格模拟"读到了最新状态"。 */
    public String fetchLatest(String sku) {
        long version = versionBySku.computeIfAbsent(sku, key -> new AtomicLong()).incrementAndGet();
        int price = ThreadLocalRandom.current().nextInt(100, 999);
        return "CatalogSnapshot{sku=" + sku + ", version=" + version + ", price=" + price + "}";
    }
}
