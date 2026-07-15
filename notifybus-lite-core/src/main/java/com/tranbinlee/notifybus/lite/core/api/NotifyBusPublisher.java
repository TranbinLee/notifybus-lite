package com.tranbinlee.notifybus.lite.core.api;

import java.time.Clock;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;

/**
 * 发布端门面。只负责把 (topic, resourceKey, changeType) 组装成 {@link NotifyBusEvent}
 * 并委托给底层 {@link NotifyBusTransport}——不关心具体是 ZK、Nacos 还是别的实现。
 */
public final class NotifyBusPublisher {

    private final NotifyBusTransport transport;
    private final Clock clock;

    /**
     * 使用系统 UTC 时钟。生产环境默认走这个构造器。
     *
     * @param transport 底层传输实现
     */
    public NotifyBusPublisher(NotifyBusTransport transport) {
        this(transport, Clock.systemUTC());
    }

    /**
     * 注入自定义 {@link Clock}，主要供测试固定时间戳使用。
     *
     * @param transport 底层传输实现
     * @param clock     时间源，事件的 timestamp 取自 {@code clock.millis()}
     */
    public NotifyBusPublisher(NotifyBusTransport transport, Clock clock) {
        if (transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.transport = transport;
        this.clock = clock;
    }

    /**
     * 发布一次触发信号。内容只有 changeType，不携带任何业务数据——
     * 消费者收到通知后自己去读最新状态。
     *
     * @param topic       topic
     * @param resourceKey 具体资源
     * @param changeType  变更类型
     */
    public void publish(String topic, String resourceKey, ChangeType changeType) {
        transport.publish(NotifyBusEvent.builder()
                .topic(topic)
                .resourceKey(resourceKey)
                .changeType(changeType)
                .timestamp(clock.millis())
                .build());
    }
}
