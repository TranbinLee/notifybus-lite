package com.tranbinlee.notifybus.lite.example.springboot.consumer.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;

/**
 * 定时把 {@link CatalogCacheListener} 当前的缓存快照打印出来。纯粹是给人肉眼看的调试手段——
 * 多开几个 consumer 实例时，可以对照各自的日志确认它们都独立收到了同一次广播、缓存最终收敛一致。
 */
public class CacheStateReporter {

    private static final Logger log = LoggerFactory.getLogger(CacheStateReporter.class);

    private final CatalogCacheListener catalogCacheListener;

    public CacheStateReporter(CatalogCacheListener catalogCacheListener) {
        this.catalogCacheListener = catalogCacheListener;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void report() {
        Map<String, String> snapshot = catalogCacheListener.snapshot();
        if (snapshot.isEmpty()) {
            log.info("[cache-state] (empty, waiting for the first trigger)");
            return;
        }
        log.info("[cache-state] {} sku(s) cached:", snapshot.size());
        snapshot.forEach((sku, value) -> log.info("[cache-state]   {} -> {}", sku, value));
    }
}
