package com.tranbinlee.notifybus.lite.transport.nacos;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code changeType|timestamp|resourceKey} 编解码的纯逻辑校验，不需要连接真实 Nacos server。
 */
class NacosNotifyBusEventCodecTest {

    @Test
    void encodeThenDecodeRoundTripsAllFields() {
        String content = NacosNotifyBusEventCodec.encode("order-1", ChangeType.UPDATED, 12345L);

        NotifyBusEvent event = NacosNotifyBusEventCodec.decode("orders", content);

        assertThat(event.getTopic()).isEqualTo("orders");
        assertThat(event.getResourceKey()).isEqualTo("order-1");
        assertThat(event.getChangeType()).isEqualTo(ChangeType.UPDATED);
        assertThat(event.getTimestamp()).isEqualTo(12345L);
    }

    @Test
    void resourceKeyContainingDelimiterSurvivesRoundTrip() {
        String content = NacosNotifyBusEventCodec.encode("a|b|c", ChangeType.CREATED, 1L);

        NotifyBusEvent event = NacosNotifyBusEventCodec.decode("orders", content);

        assertThat(event.getResourceKey()).isEqualTo("a|b|c");
    }

    @Test
    void decodeThrowsOnEmptyContent() {
        assertThatThrownBy(() -> NacosNotifyBusEventCodec.decode("orders", ""))
                .isInstanceOf(NotifyBusLiteException.class);
        assertThatThrownBy(() -> NacosNotifyBusEventCodec.decode("orders", null))
                .isInstanceOf(NotifyBusLiteException.class);
    }

    @Test
    void decodeThrowsOnMalformedContent() {
        assertThatThrownBy(() -> NacosNotifyBusEventCodec.decode("orders", "not-enough-parts"))
                .isInstanceOf(NotifyBusLiteException.class);
    }
}
