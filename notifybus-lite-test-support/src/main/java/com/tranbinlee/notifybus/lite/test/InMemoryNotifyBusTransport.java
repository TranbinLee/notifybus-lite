package com.tranbinlee.notifybus.lite.test;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 纯内存实现的 {@link NotifyBusTransport}，同步回调，不需要真实的注册中心。
 * 供 {@code notifybus-lite-core} 单测以及业务方自测使用。
 */
public final class InMemoryNotifyBusTransport implements NotifyBusTransport {

    private final ConcurrentHashMap<String, Set<NotifyBusListener>> topicListeners = new ConcurrentHashMap<String, Set<NotifyBusListener>>();
    private final ConcurrentHashMap<String, Set<NotifyBusListener>> resourceListeners = new ConcurrentHashMap<String, Set<NotifyBusListener>>();

    @Override
    public void publish(NotifyBusEvent event) {
        for (NotifyBusListener listener : listenersOf(topicListeners, event.getTopic())) {
            listener.onTrigger(event);
        }
        for (NotifyBusListener listener : listenersOf(resourceListeners, resourceKey(event.getTopic(), event.getResourceKey()))) {
            listener.onTrigger(event);
        }
    }

    @Override
    public NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener) {
        return subscribe(topicListeners, topic, listener);
    }

    @Override
    public NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener) {
        return subscribe(resourceListeners, resourceKey(topic, resourceKey), listener);
    }

    @Override
    public void close() {
        topicListeners.clear();
        resourceListeners.clear();
    }

    private static NotifyBusSubscription subscribe(ConcurrentHashMap<String, Set<NotifyBusListener>> registry, String key, NotifyBusListener listener) {
        Set<NotifyBusListener> listeners = registry.computeIfAbsent(key, k -> new CopyOnWriteArraySet<NotifyBusListener>());
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    private static Set<NotifyBusListener> listenersOf(ConcurrentHashMap<String, Set<NotifyBusListener>> registry, String key) {
        Set<NotifyBusListener> listeners = registry.get(key);
        return listeners == null ? Collections.<NotifyBusListener>emptySet() : listeners;
    }

    private static String resourceKey(String topic, String resourceKey) {
        return topic + "/" + resourceKey;
    }
}
