package com.tranbinlee.notifybus.lite.test;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryNotifyBusTransportTest {

    @Test
    void topicSubscriberReceivesEventForAnyResourceKey() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        AtomicInteger received = new AtomicInteger();
        transport.subscribeTopic("product-config", event -> received.incrementAndGet());

        transport.publish(event("product-config", "1", ChangeType.CREATED));
        transport.publish(event("product-config", "2", ChangeType.UPDATED));

        assertThat(received.get()).isEqualTo(2);
    }

    @Test
    void resourceSubscriberOnlyReceivesMatchingResourceKey() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        AtomicInteger received = new AtomicInteger();
        transport.subscribeResource("product-config", "1", event -> received.incrementAndGet());

        transport.publish(event("product-config", "1", ChangeType.CREATED));
        transport.publish(event("product-config", "2", ChangeType.CREATED));

        assertThat(received.get()).isEqualTo(1);
    }

    @Test
    void closingSubscriptionStopsFurtherCallbacks() {
        InMemoryNotifyBusTransport transport = new InMemoryNotifyBusTransport();
        AtomicInteger received = new AtomicInteger();
        NotifyBusSubscription notifyBusSubscription = transport.subscribeTopic("product-config", event -> received.incrementAndGet());

        notifyBusSubscription.close();
        transport.publish(event("product-config", "1", ChangeType.CREATED));

        assertThat(received.get()).isZero();
    }

    private static NotifyBusEvent event(String topic, String resourceKey, ChangeType changeType) {
        return NotifyBusEvent.builder()
                .topic(topic)
                .resourceKey(resourceKey)
                .changeType(changeType)
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
