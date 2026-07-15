package com.tranbinlee.notifybus.lite.core.api;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.internal.NotifyBusDispatchExecutor;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * 订阅端门面。核心职责是把业务传入的 {@link NotifyBusListener} 包一层再交给底层
 * {@link NotifyBusTransport}：
 * <ul>
 *     <li>提交到独立、命名、有界的调度线程池，绝不让业务 handler 跑在 transport 自己的
 *     事件线程上（比如 ZK 的 event thread）；</li>
 *     <li>try/catch 兜底，一个 handler 抛异常不影响后续事件；</li>
 *     <li>记录统一结构的日志（topic/resourceKey/changeType/timestamp/result/durationMs）。</li>
 * </ul>
 * transport 实现本身只管"触发回调"，这些通用鲁棒性逻辑不需要每个 transport 各写一套。
 */
public final class NotifyBusConsumer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NotifyBusConsumer.class);

    private final NotifyBusTransport transport;
    private final NotifyBusDispatchExecutor notifyBusDispatchExecutor;
    private final int shutdownAwaitSeconds;

    /**
     * 使用默认调度线程池配置（core=2, max=8, queue=1000, 关闭等待 10s）。
     *
     * @param transport 底层传输实现
     */
    public NotifyBusConsumer(NotifyBusTransport transport) {
        this(builder(transport));
    }

    /**
     * @deprecated 参数过多、可读性差，改用 {@link #builder(NotifyBusTransport)}。保留仅为向后兼容。
     */
    @Deprecated
    public NotifyBusConsumer(NotifyBusTransport transport, String dispatchPoolName,
                             int corePoolSize, int maxPoolSize, int queueCapacity) {
        this(builder(transport)
                .dispatchPoolName(dispatchPoolName)
                .corePoolSize(corePoolSize)
                .maxPoolSize(maxPoolSize)
                .queueCapacity(queueCapacity));
    }

    private NotifyBusConsumer(Builder b) {
        if (b.transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        this.transport = b.transport;
        this.shutdownAwaitSeconds = b.shutdownAwaitSeconds;
        this.notifyBusDispatchExecutor = new NotifyBusDispatchExecutor(
                b.dispatchPoolName, b.corePoolSize, b.maxPoolSize, b.queueCapacity);
    }

    /**
     * 创建一个 {@link Builder}，用链式配置替代多参数构造器。
     *
     * @param transport 底层传输实现，不能为 null
     * @return 新的 Builder
     */
    public static Builder builder(NotifyBusTransport transport) {
        return new Builder(transport);
    }

    public NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener) {
        return transport.subscribeTopic(topic, wrap(listener));
    }

    public NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener) {
        return transport.subscribeResource(topic, resourceKey, wrap(listener));
    }

    /**
     * 优雅关闭调度线程池。不关闭底层 {@link NotifyBusTransport}——transport 的生命周期由
     * 创建它的一方（比如 Spring Boot starter）负责。
     */
    @Override
    public void close() {
        notifyBusDispatchExecutor.shutdown(shutdownAwaitSeconds);
    }

    private NotifyBusListener wrap(NotifyBusListener userListener) {
        return event -> notifyBusDispatchExecutor.execute(new DispatchTask(event, userListener));
    }

    /**
     * {@link NotifyBusConsumer} 的链式构造器。所有调度参数都有合理默认值，只有 transport 必填。
     */
    public static final class Builder {
        private final NotifyBusTransport transport;
        private String dispatchPoolName = "notifybus-lite-dispatch";
        private int corePoolSize = 2;
        private int maxPoolSize = 8;
        private int queueCapacity = 1000;
        private int shutdownAwaitSeconds = 10;

        private Builder(NotifyBusTransport transport) {
            this.transport = transport;
        }

        /**
         * @param dispatchPoolName 调度线程池名称前缀（用于线程名，便于排查），默认 {@code notifybus-lite-dispatch}
         * @return this
         */
        public Builder dispatchPoolName(String dispatchPoolName) {
            this.dispatchPoolName = dispatchPoolName;
            return this;
        }

        /**
         * @param corePoolSize 核心线程数，默认 2
         * @return this
         */
        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        /**
         * @param maxPoolSize 最大线程数，默认 8
         * @return this
         */
        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        /**
         * @param queueCapacity 有界队列容量，默认 1000；队列满时按丢弃策略处理，绝不无界堆积
         * @return this
         */
        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        /**
         * @param shutdownAwaitSeconds {@link #close()} 时等待在途任务完成的秒数，默认 10
         * @return this
         */
        public Builder shutdownAwaitSeconds(int shutdownAwaitSeconds) {
            this.shutdownAwaitSeconds = shutdownAwaitSeconds;
            return this;
        }

        /**
         * @return 按当前配置构造的 {@link NotifyBusConsumer}
         */
        public NotifyBusConsumer build() {
            return new NotifyBusConsumer(this);
        }
    }

    private static final class DispatchTask implements Runnable {
        private final NotifyBusEvent event;
        private final NotifyBusListener listener;

        private DispatchTask(NotifyBusEvent event, NotifyBusListener listener) {
            this.event = event;
            this.listener = listener;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            String result = "SUCCESS";
            try {
                listener.onTrigger(event);
            } catch (Exception e) {
                result = "FAILURE";
                log.error("notify-bus-lite trigger handler threw an exception, topic={}, resourceKey={}, " +
                                "changeType={}, timestamp={}",
                        event.getTopic(), event.getResourceKey(), event.getChangeType(), event.getTimestamp(), e);
            } finally {
                long durationMs = System.currentTimeMillis() - start;
                log.info("notify-bus-lite trigger dispatched, topic={}, resourceKey={}, changeType={}, " +
                                "timestamp={}, result={}, durationMs={}",
                        event.getTopic(), event.getResourceKey(), event.getChangeType(), event.getTimestamp(),
                        result, durationMs);
            }
        }

        @Override
        public String toString() {
            return "DispatchTask{topic=" + event.getTopic() + ", resourceKey=" + event.getResourceKey() + "}";
        }
    }
}
