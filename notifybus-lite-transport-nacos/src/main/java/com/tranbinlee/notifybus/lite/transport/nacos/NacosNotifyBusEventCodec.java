package com.tranbinlee.notifybus.lite.transport.nacos;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;

/**
 * Nacos config 内容编解码：{@code changeType|timestamp|resourceKey}。
 * <p>
 * 和 {@code ZkNotifyBusEventCodec} 不同，这里始终把 {@code resourceKey} 编进内容里——resource channel
 * 上它其实和调用方已知的上下文重复，但 broadcast channel（见 {@link NacosPaths}）的 dataId/group
 * 本身并不携带具体是哪个 resourceKey 变化了，必须从内容里还原，索性两个 channel 统一走同一份编解码逻辑。
 * {@code split(..., 3)} 保证 resourceKey 里即使包含 {@code |} 也不会被错误切分。
 */
final class NacosNotifyBusEventCodec {

    private static final String DELIMITER = "\\|";

    private NacosNotifyBusEventCodec() {
    }

    static String encode(String resourceKey, ChangeType changeType, long timestamp) {
        return changeType.name() + "|" + timestamp + "|" + resourceKey;
    }

    static NotifyBusEvent decode(String topic, String content) {
        if (content == null || content.isEmpty()) {
            throw new NotifyBusLiteException("nacos trigger payload is empty for topic " + topic);
        }
        String[] parts = content.split(DELIMITER, 3);
        if (parts.length != 3) {
            throw new NotifyBusLiteException("malformed nacos trigger payload for topic " + topic + ": " + content);
        }
        return NotifyBusEvent.builder()
                .topic(topic)
                .resourceKey(parts[2])
                .changeType(ChangeType.valueOf(parts[0]))
                .timestamp(Long.parseLong(parts[1]))
                .build();
    }
}
