package com.tranbinlee.notifybus.lite.core;

import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class NotifyBusPublisherTest {

    @Test
    void publishBuildsEventWithCurrentTimestampAndDelegatesToTransport() {
        NotifyBusTransport transport = mock(NotifyBusTransport.class);
        NotifyBusPublisher publisher = new NotifyBusPublisher(transport);

        long before = System.currentTimeMillis();
        publisher.publish("product-config", "123", ChangeType.UPDATED);
        long after = System.currentTimeMillis();

        ArgumentCaptor<NotifyBusEvent> captor = ArgumentCaptor.forClass(NotifyBusEvent.class);
        verify(transport).publish(captor.capture());

        NotifyBusEvent event = captor.getValue();
        assertThat(event.getTopic()).isEqualTo("product-config");
        assertThat(event.getResourceKey()).isEqualTo("123");
        assertThat(event.getChangeType()).isEqualTo(ChangeType.UPDATED);
        assertThat(event.getTimestamp()).isBetween(before, after);
    }

    @Test
    void publishUsesInjectedClockForTimestamp() {
        NotifyBusTransport transport = mock(NotifyBusTransport.class);
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(42_000L), ZoneOffset.UTC);
        NotifyBusPublisher publisher = new NotifyBusPublisher(transport, fixed);

        publisher.publish("product-config", "123", ChangeType.UPDATED);

        ArgumentCaptor<NotifyBusEvent> captor = ArgumentCaptor.forClass(NotifyBusEvent.class);
        verify(transport).publish(captor.capture());
        assertThat(captor.getValue().getTimestamp()).isEqualTo(42_000L);
    }
}
