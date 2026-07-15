package com.tranbinlee.notifybus.lite.transport.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractListener;
import com.alibaba.nacos.api.exception.NacosException;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Nacos Config 的 {@link NotifyBusTransport} 实现。
 * <p>
 * 语义和 {@code ZkNotifyBusTransport} 一致：只广播"最新一次触发"，不是事件日志——{@code publish} 就是
 * {@code publishConfig} 覆盖内容，last-write-wins，没有排队。dataId/group 编码规则见 {@link NacosPaths}，
 * 内容编解码见 {@link NacosNotifyBusEventCodec}。
 */
public final class NacosNotifyBusTransport implements NotifyBusTransport {

    private static final Logger log = LoggerFactory.getLogger(NacosNotifyBusTransport.class);

    private final ConfigService configService;
    private final String rootPath;
    private final boolean ownsConfigService;
    private final List<ManagedListener> managedListeners = new CopyOnWriteArrayList<ManagedListener>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * @param configService 已经创建好的 {@link ConfigService}，生命周期由调用方管理，{@link #close()}
     *                      不会关闭它
     * @param rootPath      根路径/前缀，比如 {@code /notifybus}，会被 {@link NacosPaths#sanitize} 处理成
     *                      合法的 group 片段
     */
    public NacosNotifyBusTransport(ConfigService configService, String rootPath) {
        this(configService, rootPath, false);
    }

    NacosNotifyBusTransport(ConfigService configService, String rootPath, boolean ownsConfigService) {
        this.configService = configService;
        this.rootPath = rootPath;
        this.ownsConfigService = ownsConfigService;
    }

    @Override
    public void publish(NotifyBusEvent event) {
        String topic = event.getTopic();
        String resourceKey = event.getResourceKey();
        String content = NacosNotifyBusEventCodec.encode(resourceKey, event.getChangeType(), event.getTimestamp());
        String resourceGroup = NacosPaths.resourceGroup(rootPath, topic);
        String resourceDataId = NacosPaths.resourceDataId(resourceKey);
        String broadcastGroup = NacosPaths.broadcastGroup(rootPath, topic);
        try {
            configService.publishConfig(resourceDataId, resourceGroup, content);
            configService.publishConfig(NacosPaths.broadcastDataId(), broadcastGroup, content);
        } catch (NacosException e) {
            throw new NotifyBusLiteException("failed to publish trigger to nacos dataId=" + resourceDataId
                    + " group=" + resourceGroup, e);
        }
    }

    @Override
    public NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener) {
        return doSubscribe(topic, NacosPaths.broadcastDataId(), NacosPaths.broadcastGroup(rootPath, topic), listener);
    }

    @Override
    public NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener) {
        return doSubscribe(topic, NacosPaths.resourceDataId(resourceKey), NacosPaths.resourceGroup(rootPath, topic),
                listener);
    }

    private NotifyBusSubscription doSubscribe(String topic, String dataId, String group, NotifyBusListener listener) {
        AbstractListener nacosListener = new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                dispatch(topic, configInfo, listener);
            }
        };
        try {
            configService.addListener(dataId, group, nacosListener);
        } catch (NacosException e) {
            throw new NotifyBusLiteException("failed to add nacos listener for dataId=" + dataId
                    + " group=" + group, e);
        }
        final ManagedListener managedListener = new ManagedListener(dataId, group, nacosListener);
        managedListeners.add(managedListener);
        return () -> removeListenerQuietly(managedListener);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (ManagedListener managedListener : managedListeners) {
            removeListenerQuietly(managedListener);
        }
        managedListeners.clear();
        if (ownsConfigService) {
            try {
                configService.shutDown();
            } catch (NacosException e) {
                log.warn("failed to shut down nacos config service cleanly", e);
            }
        }
    }

    private void dispatch(String topic, String content, NotifyBusListener listener) {
        try {
            NotifyBusEvent event = NacosNotifyBusEventCodec.decode(topic, content);
            listener.onTrigger(event);
        } catch (Exception e) {
            log.error("failed to decode/dispatch nacos trigger event, topic={}", topic, e);
        }
    }

    private void removeListenerQuietly(ManagedListener managedListener) {
        try {
            configService.removeListener(managedListener.dataId, managedListener.group, managedListener.listener);
        } catch (Exception e) {
            log.warn("failed to remove nacos listener cleanly, dataId={}, group={}",
                    managedListener.dataId, managedListener.group, e);
        }
    }

    private static final class ManagedListener {
        private final String dataId;
        private final String group;
        private final AbstractListener listener;

        private ManagedListener(String dataId, String group, AbstractListener listener) {
            this.dataId = dataId;
            this.group = group;
            this.listener = listener;
        }
    }
}
