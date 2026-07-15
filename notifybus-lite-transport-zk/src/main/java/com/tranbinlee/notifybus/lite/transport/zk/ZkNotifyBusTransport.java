package com.tranbinlee.notifybus.lite.transport.zk;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Apache Curator 的 {@link NotifyBusTransport} 实现。
 * <p>
 * znode 布局：{@code {root}/{topic}/{resourceKey}}，内容只有最小元数据（见 {@link ZkNotifyBusEventCodec}）。
 * 用经典的 {@link PathChildrenCache}/{@link NodeCache} recipe（一次性 Watch 触发后自动重新注册），
 * 不使用 ZK 3.6+ 才有的持久化递归 Watch，以保持对 ZK 3.4.x 老集群的兼容。
 */
public final class ZkNotifyBusTransport implements NotifyBusTransport {

    private static final Logger log = LoggerFactory.getLogger(ZkNotifyBusTransport.class);

    private final CuratorFramework client;
    private final String rootPath;
    private final boolean ownsClient;
    private final List<Closeable> managedCaches = new CopyOnWriteArrayList<Closeable>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * @param client   已经 {@code start()} 过的 CuratorFramework，生命周期由调用方管理，
     *                 {@link #close()} 不会关闭它
     * @param rootPath 根路径，比如 {@code /notifybus}
     */
    public ZkNotifyBusTransport(CuratorFramework client, String rootPath) {
        this(client, rootPath, false);
    }

    ZkNotifyBusTransport(CuratorFramework client, String rootPath, boolean ownsClient) {
        this.client = client;
        this.rootPath = ZkPaths.normalizeRoot(rootPath);
        this.ownsClient = ownsClient;
    }

    @Override
    public void publish(NotifyBusEvent event) {
        String path = ZkPaths.resourcePath(rootPath, event.getTopic(), event.getResourceKey());
        byte[] data = ZkNotifyBusEventCodec.encode(event.getChangeType(), event.getTimestamp());
        try {
            client.create().orSetData().creatingParentsIfNeeded().forPath(path, data);
        } catch (Exception e) {
            throw new NotifyBusLiteException("failed to publish trigger to " + path, e);
        }
    }

    @Override
    public NotifyBusSubscription subscribeTopic(String topic, NotifyBusListener listener) {
        String path = ZkPaths.topicPath(rootPath, topic);
        final PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.getListenable().addListener((curatorClient, event) -> {
            PathChildrenCacheEvent.Type type = event.getType();
            if (type == PathChildrenCacheEvent.Type.CHILD_ADDED || type == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                ChildData data = event.getData();
                dispatch(topic, ZkPaths.lastSegment(data.getPath()), data.getData(), listener);
            }
        });
        try {
            // BUILD_INITIAL_CACHE 同步预热现有子节点、不触发事件；订阅之前已经发生的触发不算“补发”，
            // 只有订阅成功之后的真实变化才会回调 listener。
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            throw new NotifyBusLiteException("failed to start topic watch on " + path, e);
        }
        managedCaches.add(cache);
        return () -> closeQuietly(cache);
    }

    @Override
    public NotifyBusSubscription subscribeResource(String topic, String resourceKey, NotifyBusListener listener) {
        String path = ZkPaths.resourcePath(rootPath, topic, resourceKey);
        final NodeCache cache = new NodeCache(client, path);
        try {
            // 先同步加载一次当前数据，此时还没注册 listener，不会把已有数据当成一次“触发”回调出去；
            // 加完 listener 之后，只有后续真正的数据变化才会触发 nodeChanged()。
            cache.start(true);
        } catch (Exception e) {
            throw new NotifyBusLiteException("failed to start resource watch on " + path, e);
        }
        cache.getListenable().addListener(() -> {
            ChildData data = cache.getCurrentData();
            if (data != null) {
                dispatch(topic, resourceKey, data.getData(), listener);
            }
        });
        managedCaches.add(cache);
        return () -> closeQuietly(cache);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Closeable cache : managedCaches) {
            closeQuietly(cache);
        }
        managedCaches.clear();
        if (ownsClient) {
            client.close();
        }
    }

    private void dispatch(String topic, String resourceKey, byte[] data, NotifyBusListener listener) {
        try {
            NotifyBusEvent event = ZkNotifyBusEventCodec.decode(topic, resourceKey, data);
            listener.onTrigger(event);
        } catch (Exception e) {
            log.error("failed to decode/dispatch zk trigger event, topic={}, resourceKey={}", topic, resourceKey, e);
        }
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            log.warn("failed to close zk watch cleanly", e);
        }
    }
}
