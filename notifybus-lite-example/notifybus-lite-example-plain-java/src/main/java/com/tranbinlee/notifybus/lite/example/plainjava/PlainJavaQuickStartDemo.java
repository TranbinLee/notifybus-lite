package com.tranbinlee.notifybus.lite.example.plainjava;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import com.tranbinlee.notifybus.lite.transport.zk.ZkNotifyBusTransportBuilder;
import org.apache.curator.test.TestingServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示不依赖 Spring Boot、纯手写代码接入 NotifyBus Lite 的最小流程：
 * <ol>
 *     <li>用 {@link ZkNotifyBusTransportBuilder} 手动构造一个 {@link NotifyBusTransport}
 *     （Spring Boot starter 场景下这一步是 {@code NotifyBusAutoConfiguration} 自动做的）；</li>
 *     <li>用它分别 new 出 {@link NotifyBusConsumer}/{@link NotifyBusPublisher}；</li>
 *     <li>topic 级订阅 + resourceKey 级订阅各注册一个，验证订阅粒度；</li>
 *     <li>发布两条事件，验证 topic 级订阅两条都收到、resourceKey 级订阅只收到匹配的那条；</li>
 *     <li>按依赖关系逆序优雅关闭：先取消订阅，再关调度线程池，最后关 transport（ZK 连接）。</li>
 * </ol>
 * 内嵌的 {@link TestingServer} 只是为了让这个 demo 不需要用户先手工起一个真实 ZK 集群就能跑——
 * 生产代码永远应该指向一个真实的 ZooKeeper 集群，不要引入 {@code curator-test}。
 */
public final class PlainJavaQuickStartDemo {

    private static final String TOPIC = "catalog.changed";
    private static final String WATCHED_SKU = "sku-1001";
    private static final String OTHER_SKU = "sku-2002";

    public static void main(String[] args) throws Exception {
        TestingServer embeddedZk = new TestingServer();
        NotifyBusTransport transport = null;
        NotifyBusConsumer consumer = null;
        NotifyBusSubscription topicNotifyBusSubscription = null;
        NotifyBusSubscription resourceNotifyBusSubscription = null;
        try {
            System.out.println("[demo] embedded ZooKeeper started at " + embeddedZk.getConnectString());

            transport = ZkNotifyBusTransportBuilder.builder()
                    .connectString(embeddedZk.getConnectString())
                    .rootPath("/notifybus-lite-example")
                    .build();

            consumer = new NotifyBusConsumer(transport);

            AtomicInteger topicEventCount = new AtomicInteger(0);
            AtomicInteger resourceEventCount = new AtomicInteger(0);
            CountDownLatch topicLatch = new CountDownLatch(2);
            CountDownLatch resourceLatch = new CountDownLatch(1);

            // topic 级订阅：这个 topic 下任何 resourceKey 变化都能收到
            topicNotifyBusSubscription = consumer.subscribeTopic(TOPIC, event -> {
                topicEventCount.incrementAndGet();
                System.out.println("[topic subscriber] received " + describe(event));
                topicLatch.countDown();
            });

            // resourceKey 级订阅：只关心某一个具体资源
            resourceNotifyBusSubscription = consumer.subscribeResource(TOPIC, WATCHED_SKU, event -> {
                resourceEventCount.incrementAndGet();
                System.out.println("[resource subscriber, sku=" + WATCHED_SKU + "] received " + describe(event));
                resourceLatch.countDown();
            });

            NotifyBusPublisher publisher = new NotifyBusPublisher(transport);
            System.out.println("[demo] publishing 2 events...");
            publisher.publish(TOPIC, WATCHED_SKU, ChangeType.UPDATED);
            publisher.publish(TOPIC, OTHER_SKU, ChangeType.CREATED);

            if (!topicLatch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("topic subscriber did not receive both events in time");
            }
            if (!resourceLatch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("resource subscriber did not receive its event in time");
            }

            System.out.println();
            System.out.println("[demo] SUCCESS: topic subscriber received " + topicEventCount.get()
                    + " event(s) (expected 2), resource subscriber received " + resourceEventCount.get()
                    + " event(s) (expected 1, only for sku=" + WATCHED_SKU + ")");
        } finally {
            // 优雅关闭顺序：先取消订阅 -> 再关调度线程池（NotifyBusConsumer.close()）
            // -> 最后关 transport 持有的 ZK 连接（NotifyBusTransport.close()）。
            closeQuietly(topicNotifyBusSubscription);
            closeQuietly(resourceNotifyBusSubscription);
            if (consumer != null) {
                consumer.close();
            }
            if (transport != null) {
                transport.close();
            }
            embeddedZk.close();
            System.out.println("[demo] everything shut down cleanly");
        }
    }

    private static String describe(NotifyBusEvent event) {
        return "topic=" + event.getTopic() + ", resourceKey=" + event.getResourceKey()
                + ", changeType=" + event.getChangeType();
    }

    private static void closeQuietly(NotifyBusSubscription notifyBusSubscription) {
        if (notifyBusSubscription != null) {
            notifyBusSubscription.close();
        }
    }

    private PlainJavaQuickStartDemo() {
    }
}
