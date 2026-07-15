package com.tranbinlee.notifybus.lite.core;

import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class NotifyBusConsumerTest {

    @Test
    void dispatchesToListenerOnADedicatedThreadNotTheCallingThread() throws InterruptedException {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        NotifyBusConsumer consumer = new NotifyBusConsumer(transport);
        String callingThread = Thread.currentThread().getName();
        List<String> handlerThreads = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        consumer.subscribeTopic("product-config", event -> {
            handlerThreads.add(Thread.currentThread().getName());
            latch.countDown();
        });

        new NotifyBusPublisher(transport).publish("product-config", "1", ChangeType.UPDATED);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(handlerThreads).hasSize(1);
        assertThat(handlerThreads.get(0)).isNotEqualTo(callingThread);
        assertThat(handlerThreads.get(0)).startsWith("notifybus-lite-dispatch-");

        consumer.close();
    }

    @Test
    void listenerExceptionDoesNotBlockSubsequentEvents() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        NotifyBusConsumer consumer = new NotifyBusConsumer(transport);
        AtomicInteger successfulCalls = new AtomicInteger();

        consumer.subscribeResource("product-config", "1", event -> {
            if (event.getChangeType() == ChangeType.DELETED) {
                throw new RuntimeException("boom");
            }
            successfulCalls.incrementAndGet();
        });

        NotifyBusPublisher publisher = new NotifyBusPublisher(transport);
        publisher.publish("product-config", "1", ChangeType.DELETED);
        publisher.publish("product-config", "1", ChangeType.UPDATED);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(successfulCalls.get()).isEqualTo(1));

        consumer.close();
    }

    @Test
    void resourceSubscriberIgnoresOtherResourceKeys() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        NotifyBusConsumer consumer = new NotifyBusConsumer(transport);
        AtomicInteger received = new AtomicInteger();

        consumer.subscribeResource("product-config", "1", event -> received.incrementAndGet());

        NotifyBusPublisher publisher = new NotifyBusPublisher(transport);
        publisher.publish("product-config", "2", ChangeType.UPDATED);

        await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(received.get()).isZero());

        consumer.close();
    }

    @Test
    void builderThreadNamePrefixIsHonored() throws InterruptedException {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        NotifyBusConsumer consumer = NotifyBusConsumer.builder(transport)
                .dispatchPoolName("custom-pool")
                .corePoolSize(1)
                .maxPoolSize(1)
                .queueCapacity(16)
                .build();
        List<String> threads = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        consumer.subscribeTopic("product-config", event -> {
            threads.add(Thread.currentThread().getName());
            latch.countDown();
        });
        new NotifyBusPublisher(transport).publish("product-config", "1", ChangeType.UPDATED);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threads.get(0)).startsWith("custom-pool-");

        consumer.close();
    }

    @Test
    void deliversEveryEventUnderConcurrentPublishing() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        NotifyBusConsumer consumer = NotifyBusConsumer.builder(transport)
                .corePoolSize(4)
                .maxPoolSize(4)
                .queueCapacity(10_000)
                .build();
        int publishers = 4;
        int perPublisher = 250;
        AtomicInteger received = new AtomicInteger();

        consumer.subscribeTopic("product-config", event -> received.incrementAndGet());

        NotifyBusPublisher publisher = new NotifyBusPublisher(transport);
        List<Thread> workers = new java.util.ArrayList<>();
        for (int p = 0; p < publishers; p++) {
            Thread t = new Thread(() -> {
                for (int i = 0; i < perPublisher; i++) {
                    publisher.publish("product-config", "1", ChangeType.UPDATED);
                }
            });
            workers.add(t);
            t.start();
        }
        workers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(received.get()).isEqualTo(publishers * perPublisher));

        consumer.close();
    }
}
