package com.tranbinlee.notifybus.lite.example.springboot.publisher.catalog;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 包一层 {@link NotifyBusPublisher}：只广播"商品目录变了"，不携带任何商品数据本身——
 * 这正是 NotifyBus Lite 的核心原则，消费方收到信号后要自己去读最新状态。
 * <p>
 * 额外维护一份最近发布记录，纯粹是为了让 {@code GET /catalog/changes} 有内容可看，
 * 属于示例自身的展示逻辑，不是 SDK 的一部分。
 */
@Component
public class CatalogChangePublisher {

    private static final String TOPIC = "catalog.changed";
    private static final int MAX_HISTORY = 20;

    private final NotifyBusPublisher publisher;
    private final List<PublishedChange> history = new CopyOnWriteArrayList<>();

    public CatalogChangePublisher(NotifyBusPublisher publisher) {
        this.publisher = publisher;
    }

    public void publishChange(String sku, ChangeType changeType) {
        publisher.publish(TOPIC, sku, changeType);
        history.add(0, new PublishedChange(sku, changeType, Instant.now().toString()));
        while (history.size() > MAX_HISTORY) {
            history.remove(history.size() - 1);
        }
    }

    public List<PublishedChange> recentHistory() {
        return Collections.unmodifiableList(history);
    }

    /** 一条已发布记录，仅用于示例的 {@code GET} 接口展示，不是 {@code NotifyBusEvent} 本身。 */
    public static final class PublishedChange {
        private final String sku;
        private final ChangeType changeType;
        private final String publishedAt;

        PublishedChange(String sku, ChangeType changeType, String publishedAt) {
            this.sku = sku;
            this.changeType = changeType;
            this.publishedAt = publishedAt;
        }

        public String getSku() {
            return sku;
        }

        public ChangeType getChangeType() {
            return changeType;
        }

        public String getPublishedAt() {
            return publishedAt;
        }
    }
}
