package com.tranbinlee.notifybus.lite.api;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotifyBusEventTest {

    @Test
    void buildsWithAllFields() {
        NotifyBusEvent event = NotifyBusEvent.builder()
                .topic("product-config")
                .resourceKey("123")
                .changeType(ChangeType.UPDATED)
                .timestamp(1000L)
                .build();

        assertThat(event.getTopic()).isEqualTo("product-config");
        assertThat(event.getResourceKey()).isEqualTo("123");
        assertThat(event.getChangeType()).isEqualTo(ChangeType.UPDATED);
        assertThat(event.getTimestamp()).isEqualTo(1000L);
    }

    @Test
    void rejectsBlankTopic() {
        assertThatThrownBy(() -> NotifyBusEvent.builder()
                .topic("")
                .resourceKey("123")
                .changeType(ChangeType.UPDATED)
                .timestamp(1000L)
                .build())
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void rejectsBlankResourceKey() {
        assertThatThrownBy(() -> NotifyBusEvent.builder()
                .topic("product-config")
                .resourceKey(null)
                .changeType(ChangeType.UPDATED)
                .timestamp(1000L)
                .build())
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void rejectsMissingChangeType() {
        assertThatThrownBy(() -> NotifyBusEvent.builder()
                .topic("product-config")
                .resourceKey("123")
                .timestamp(1000L)
                .build())
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void rejectsNonPositiveTimestamp() {
        assertThatThrownBy(() -> NotifyBusEvent.builder()
                .topic("product-config")
                .resourceKey("123")
                .changeType(ChangeType.UPDATED)
                .timestamp(0L)
                .build())
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void equalEventsHaveEqualHashCode() {
        NotifyBusEvent a = NotifyBusEvent.builder()
                .topic("product-config").resourceKey("123")
                .changeType(ChangeType.UPDATED).timestamp(1000L).build();
        NotifyBusEvent b = NotifyBusEvent.builder()
                .topic("product-config").resourceKey("123")
                .changeType(ChangeType.UPDATED).timestamp(1000L).build();

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differsWhenAnyFieldDiffers() {
        NotifyBusEvent base = NotifyBusEvent.builder()
                .topic("product-config").resourceKey("123")
                .changeType(ChangeType.UPDATED).timestamp(1000L).build();

        assertThat(base).isNotEqualTo(NotifyBusEvent.builder()
                .topic("other").resourceKey("123")
                .changeType(ChangeType.UPDATED).timestamp(1000L).build());
        assertThat(base).isNotEqualTo(NotifyBusEvent.builder()
                .topic("product-config").resourceKey("999")
                .changeType(ChangeType.UPDATED).timestamp(1000L).build());
        assertThat(base).isNotEqualTo(NotifyBusEvent.builder()
                .topic("product-config").resourceKey("123")
                .changeType(ChangeType.DELETED).timestamp(1000L).build());
        assertThat(base).isNotEqualTo(NotifyBusEvent.builder()
                .topic("product-config").resourceKey("123")
                .changeType(ChangeType.UPDATED).timestamp(2000L).build());
    }
}
