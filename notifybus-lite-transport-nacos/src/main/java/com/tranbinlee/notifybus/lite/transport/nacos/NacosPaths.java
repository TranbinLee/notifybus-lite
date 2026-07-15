package com.tranbinlee.notifybus.lite.transport.nacos;

/**
 * dataId/group 编码规则。
 * <p>
 * Nacos Config 只有"确切 dataId + group"这一种订阅粒度，没有 ZK {@code PathChildrenCache} 那种
 * "watch 某个目录下所有子节点"的能力，所以 topic 级订阅（{@code subscribeTopic}）没法直接对应到
 * 一个 resourceKey 的 dataId 上。这里用两条独立的 channel 解决：
 * <ul>
 *     <li>resource channel：{@code group=root.topic, dataId=resourceKey}，一个 resourceKey 一个 dataId，
 *     内容是这次触发的最新状态，last-write-wins；</li>
 *     <li>broadcast channel：{@code group=root.topic.broadcast, dataId=固定常量}，topic 下任意 resourceKey
 *     发生变化都会额外写一次这个 channel（内容里带上 resourceKey，见 {@link NacosNotifyBusEventCodec}），
 *     {@code subscribeTopic} 只订阅这一个 channel。已知局限：同一个 topic 下、几乎同时发生的多个不同
 *     resourceKey 变化，topic 级订阅者可能只观察到最后一次——这是"只广播最新状态"这个模型本身的取舍，
 *     和单资源场景的 last-write-wins 是同一类局限，只是把范围从一个资源扩大到了整个 topic。</li>
 * </ul>
 */
final class NacosPaths {

    private static final String BROADCAST_GROUP_SUFFIX = ".broadcast";
    private static final String BROADCAST_DATA_ID = "topic";

    private NacosPaths() {
    }

    static String resourceGroup(String root, String topic) {
        return sanitize(root) + "." + sanitize(topic);
    }

    static String resourceDataId(String resourceKey) {
        return sanitize(resourceKey);
    }

    static String broadcastGroup(String root, String topic) {
        return sanitize(root) + "." + sanitize(topic) + BROADCAST_GROUP_SUFFIX;
    }

    static String broadcastDataId() {
        return BROADCAST_DATA_ID;
    }

    /**
     * Nacos dataId/group 只允许 {@code [a-zA-Z0-9-_.]}，其余字符（包括 ZK 风格根路径里的 {@code /}）
     * 统一替换成 {@code _}。
     */
    static String sanitize(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "_";
        }
        StringBuilder sanitized = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.') {
                sanitized.append(c);
            } else {
                sanitized.append('_');
            }
        }
        return sanitized.toString();
    }
}
