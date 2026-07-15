package com.tranbinlee.notifybus.lite.core.event;

import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;

/**
 * 一次"资源变了"的触发信号。不携带任何业务数据——消费者收到后自己去读最新状态。
 * <p>
 * 不可变，只能通过 {@link #builder()} 构造。
 */
public final class NotifyBusEvent {

    private final String topic;
    private final String resourceKey;
    private final ChangeType changeType;
    private final long timestamp;

    private NotifyBusEvent(Builder builder) {
        this.topic = builder.topic;
        this.resourceKey = builder.resourceKey;
        this.changeType = builder.changeType;
        this.timestamp = builder.timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getResourceKey() {
        return resourceKey;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NotifyBusEvent that = (NotifyBusEvent) o;
        return timestamp == that.timestamp
                && topic.equals(that.topic)
                && resourceKey.equals(that.resourceKey)
                && changeType == that.changeType;
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + resourceKey.hashCode();
        result = 31 * result + changeType.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "NotifyBusEvent{" +
                "topic='" + topic + '\'' +
                ", resourceKey='" + resourceKey + '\'' +
                ", changeType=" + changeType +
                ", timestamp=" + timestamp +
                '}';
    }

    public static final class Builder {
        private String topic;
        private String resourceKey;
        private ChangeType changeType;
        private long timestamp;

        private Builder() {
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder resourceKey(String resourceKey) {
            this.resourceKey = resourceKey;
            return this;
        }

        public Builder changeType(ChangeType changeType) {
            this.changeType = changeType;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public NotifyBusEvent build() {
            if (topic == null || topic.isEmpty()) {
                throw new ConfigurationException("topic must not be blank");
            }
            if (resourceKey == null || resourceKey.isEmpty()) {
                throw new ConfigurationException("resourceKey must not be blank");
            }
            if (changeType == null) {
                throw new ConfigurationException("changeType must not be null");
            }
            if (timestamp <= 0) {
                throw new ConfigurationException("timestamp must be positive");
            }
            return new NotifyBusEvent(this);
        }
    }
}
