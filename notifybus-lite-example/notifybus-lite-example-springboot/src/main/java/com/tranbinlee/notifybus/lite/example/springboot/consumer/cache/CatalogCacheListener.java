package com.tranbinlee.notifybus.lite.example.springboot.consumer.cache;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.starter.listener.NotifyBusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 两个 {@code @NotifyBusListener} 方法，对照展示订阅粒度：
 * <ul>
 *     <li>{@link #onAnyCatalogChanged}：{@code topic = "catalog.changed"}，整个 topic 下任何
 *     SKU 变化都会收到；</li>
 *     <li>{@link #onWatchedSkuChanged}：{@code topic + resourceKey}，只关心 {@link #WATCHED_SKU}
 *     这一个具体 SKU。</li>
 * </ul>
 * 两个方法都只拿到 {@code changeType}/{@code timestamp}，不知道商品具体变成了什么样——
 * 拿到最新状态要靠自己调用 {@link CatalogSource#fetchLatest(String)}，这正是 Lite 版本
 * "SDK 只负责触发，业务自己决定怎么应对"的设计核心。
 */
public class CatalogCacheListener {

    private static final Logger log = LoggerFactory.getLogger(CatalogCacheListener.class);
    private static final String WATCHED_SKU = "sku-1001";

    private final CatalogSource catalogSource;
    private final Map<String, String> cache = new ConcurrentHashMap<>();

    public CatalogCacheListener(CatalogSource catalogSource) {
        this.catalogSource = catalogSource;
    }

    @NotifyBusListener(topic = "catalog.changed")
    public void onAnyCatalogChanged(NotifyBusEvent event) {
        refresh(event);
    }

    @NotifyBusListener(topic = "catalog.changed", resourceKey = WATCHED_SKU)
    public void onWatchedSkuChanged(NotifyBusEvent event) {
        log.info("[watched-sku listener] sku={} got a dedicated notification too (changeType={})",
                WATCHED_SKU, event.getChangeType());
    }

    private void refresh(NotifyBusEvent event) {
        String sku = event.getResourceKey();
        if (event.getChangeType() == ChangeType.DELETED) {
            cache.remove(sku);
            log.info("cache INVALIDATE sku={}", sku);
            return;
        }
        String snapshot = catalogSource.fetchLatest(sku);
        cache.put(sku, snapshot);
        log.info("cache RELOAD sku={} changeType={} snapshot={}", sku, event.getChangeType(), snapshot);
    }

    /** 供 {@link CacheStateReporter} 定时打印当前缓存状态。 */
    public Map<String, String> snapshot() {
        return Collections.unmodifiableMap(cache);
    }
}
