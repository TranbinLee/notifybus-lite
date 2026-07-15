package com.tranbinlee.notifybus.lite.core.spi;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;

/**
 * "触发广播"能力的扩展点。任何注册中心（ZooKeeper、Nacos、...）只要实现这个接口，
 * 就能接入 {@code notifybus-lite-core} 的 {@link com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher}/{@link com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer}。
 * <p>
 * 实现类只负责"把触发信号发出去/收回来"，不做异常兜底、不做线程隔离——这些通用鲁棒性
 * 逻辑收敛在 core 层，避免每个 transport 各写一套。
 */
public interface NotifyBusTransport {

    /**
     * 发布一次触发信号。
     *
     * @param event 触发事件，不为 null
     */
    void publish(NotifyBusEvent event);

    /**
     * 订阅某个 topic 下所有 resourceKey 的变化。
     *
     * @param topic    topic，不为空
     * @param listener 收到触发信号时的回调
     * @return 用于取消订阅的句柄
     */
    NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener);

    /**
     * 订阅某个具体资源的变化。
     *
     * @param topic       topic，不为空
     * @param resourceKey 资源 key，不为空
     * @param listener    收到触发信号时的回调
     * @return 用于取消订阅的句柄
     */
    NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener);

    /**
     * 关闭该 transport 持有的所有资源（连接、订阅、内部线程等）。
     */
    void close();
}
