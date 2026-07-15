package com.tranbinlee.notifybus.lite.transport.zk;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 用内嵌 ZK（curator-test）验证 {@link ZkNotifyBusTransport} 的真实行为：
 * “先订阅后发布”能收到通知；“先发布后订阅”不会把订阅之前已经发生的触发当成历史补发。
 */
class ZkNotifyBusTransportTest {

    private static TestingServer testingServer;

    private final List<ZkNotifyBusTransport> transports = new ArrayList<ZkNotifyBusTransport>();
    private final List<NotifyBusSubscription> notifyBusSubscriptions = new ArrayList<NotifyBusSubscription>();

    @BeforeAll
    static void startZk() throws Exception {
        testingServer = new TestingServer();
    }

    @AfterAll
    static void stopZk() throws Exception {
        testingServer.close();
    }

    @AfterEach
    void closeAll() {
        for (NotifyBusSubscription notifyBusSubscription : notifyBusSubscriptions) {
            notifyBusSubscription.close();
        }
        notifyBusSubscriptions.clear();
        for (ZkNotifyBusTransport transport : transports) {
            transport.close();
        }
        transports.clear();
    }

    private ZkNotifyBusTransport newTransport(String rootPath) {
        ZkNotifyBusTransport transport = ZkNotifyBusTransportBuilder.builder()
                .connectString(testingServer.getConnectString())
                .rootPath(rootPath)
                .build();
        transports.add(transport);
        return transport;
    }

    private String uniqueRoot() {
        return "/notifybus-test-" + UUID.randomUUID();
    }

    @Test
    void topicSubscriberReceivesEventPublishedAfterSubscribing() {
        ZkNotifyBusTransport transport = newTransport(uniqueRoot());
        Queue<NotifyBusEvent> received = new ConcurrentLinkedQueue<NotifyBusEvent>();
        notifyBusSubscriptions.add(transport.subscribeTopic("orders", received::add));

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-1")
                .changeType(ChangeType.UPDATED)
                .timestamp(System.currentTimeMillis())
                .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).hasSize(1));
        NotifyBusEvent event = received.peek();
        assertThat(event.getTopic()).isEqualTo("orders");
        assertThat(event.getResourceKey()).isEqualTo("order-1");
        assertThat(event.getChangeType()).isEqualTo(ChangeType.UPDATED);
        assertThat(event.getTimestamp()).isGreaterThan(0);
    }

    @Test
    void resourceSubscriberReceivesEventPublishedAfterSubscribing() {
        ZkNotifyBusTransport transport = newTransport(uniqueRoot());
        Queue<NotifyBusEvent> received = new ConcurrentLinkedQueue<NotifyBusEvent>();
        notifyBusSubscriptions.add(transport.subscribeResource("orders", "order-1", received::add));

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-1")
                .changeType(ChangeType.CREATED)
                .timestamp(System.currentTimeMillis())
                .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).hasSize(1));
        assertThat(received.peek().getChangeType()).isEqualTo(ChangeType.CREATED);
    }

    @Test
    void topicSubscriberDoesNotReceiveEventPublishedBeforeSubscribing() {
        ZkNotifyBusTransport transport = newTransport(uniqueRoot());

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-1")
                .changeType(ChangeType.CREATED)
                .timestamp(System.currentTimeMillis())
                .build());

        Queue<NotifyBusEvent> received = new ConcurrentLinkedQueue<NotifyBusEvent>();
        notifyBusSubscriptions.add(transport.subscribeTopic("orders", received::add));

        // 给足够时间让 PathChildrenCache 完成启动；此时不应该收到订阅之前发生的那次触发。
        await().pollDelay(2, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isEmpty());

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-2")
                .changeType(ChangeType.UPDATED)
                .timestamp(System.currentTimeMillis())
                .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).hasSize(1));
        assertThat(received.peek().getResourceKey()).isEqualTo("order-2");
    }

    @Test
    void resourceSubscriberDoesNotReceiveEventPublishedBeforeSubscribing() {
        ZkNotifyBusTransport transport = newTransport(uniqueRoot());

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-1")
                .changeType(ChangeType.CREATED)
                .timestamp(System.currentTimeMillis())
                .build());

        Queue<NotifyBusEvent> received = new ConcurrentLinkedQueue<NotifyBusEvent>();
        notifyBusSubscriptions.add(transport.subscribeResource("orders", "order-1", received::add));

        await().pollDelay(2, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isEmpty());

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-1")
                .changeType(ChangeType.DELETED)
                .timestamp(System.currentTimeMillis())
                .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).hasSize(1));
        assertThat(received.peek().getChangeType()).isEqualTo(ChangeType.DELETED);
    }

    @Test
    void resourceSubscriberIgnoresOtherResourceKeysUnderSameTopic() {
        ZkNotifyBusTransport transport = newTransport(uniqueRoot());
        AtomicInteger callCount = new AtomicInteger();
        notifyBusSubscriptions.add(transport.subscribeResource("orders", "order-1", event -> callCount.incrementAndGet()));

        transport.publish(NotifyBusEvent.builder()
                .topic("orders")
                .resourceKey("order-2")
                .changeType(ChangeType.CREATED)
                .timestamp(System.currentTimeMillis())
                .build());

        await().pollDelay(2, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(callCount.get()).isZero());
    }
}
