package com.tranbinlee.notifybus.lite.transport.zk;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;

import java.nio.charset.StandardCharsets;

/**
 * znode 内容编解码：只放最小元数据 {@code changeType|timestamp}，不含任何业务数据。
 * topic/resourceKey 由 znode 路径决定，不放进 payload。
 */
final class ZkNotifyBusEventCodec {

    private static final String DELIMITER = "\\|";

    private ZkNotifyBusEventCodec() {
    }

    static byte[] encode(ChangeType changeType, long timestamp) {
        String raw = changeType.name() + "|" + timestamp;
        return raw.getBytes(StandardCharsets.UTF_8);
    }

    static NotifyBusEvent decode(String topic, String resourceKey, byte[] data) {
        if (data == null) {
            throw new NotifyBusLiteException("zk trigger payload is empty for " + topic + "/" + resourceKey);
        }
        String raw = new String(data, StandardCharsets.UTF_8);
        String[] parts = raw.split(DELIMITER, 2);
        if (parts.length != 2) {
            throw new NotifyBusLiteException("malformed zk trigger payload for " + topic + "/" + resourceKey + ": " + raw);
        }
        return NotifyBusEvent.builder()
                .topic(topic)
                .resourceKey(resourceKey)
                .changeType(ChangeType.valueOf(parts[0]))
                .timestamp(Long.parseLong(parts[1]))
                .build();
    }
}
