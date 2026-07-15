package com.tranbinlee.notifybus.lite.core.internal;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SDK 被禁用（{@code notifybus.lite.enabled=false} 或未配置）时使用的空实现。
 * <p>
 * 目的是让业务代码在 SDK 关闭的场景下依然能正常注入
 * {@link com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher}/
 * {@link com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer} 并正常调用，而不是因为容器缺少 bean 而启动失败，也不是因为
 * 调用底层 transport 而抛出异常——每次调用只打印一条 WARN 日志，提醒开发者当前配置下
 * SDK 实际不生效。
 * <p>
 * 不做限流/去重：每次调用都打印，这是一个明确的误用信号，密集打印能促使开发者尽快
 * 修正配置（要么真正开启 SDK，要么移除对发布/订阅接口的调用）。
 */
public final class NoOpNotifyBusTransport implements NotifyBusTransport {

    private static final Logger log = LoggerFactory.getLogger(NoOpNotifyBusTransport.class);

    @Override
    public void publish(NotifyBusEvent event) {
        log.warn("notify-bus-lite is disabled (notifybus.lite.enabled=false), publish() is a no-op, " +
                        "topic={}, resourceKey={}, changeType={}",
                event.getTopic(), event.getResourceKey(), event.getChangeType());
    }

    @Override
    public NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener) {
        log.warn("notify-bus-lite is disabled (notifybus.lite.enabled=false), subscribeTopic() is a no-op " +
                "and will never receive events, topic={}", topic);
        return NO_OP_NOTIFY_BUS_SUBSCRIPTION;
    }

    @Override
    public NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener) {
        log.warn("notify-bus-lite is disabled (notifybus.lite.enabled=false), subscribeResource() is a no-op " +
                "and will never receive events, topic={}, resourceKey={}", topic, resourceKey);
        return NO_OP_NOTIFY_BUS_SUBSCRIPTION;
    }

    @Override
    public void close() {
        // no-op：没有任何资源需要释放
    }

    private static final NotifyBusSubscription NO_OP_NOTIFY_BUS_SUBSCRIPTION = () -> {
        // no-op：没有任何订阅需要取消
    };
}
