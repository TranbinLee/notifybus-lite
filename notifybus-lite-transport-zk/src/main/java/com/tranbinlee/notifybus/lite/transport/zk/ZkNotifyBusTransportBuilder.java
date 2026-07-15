package com.tranbinlee.notifybus.lite.transport.zk;

import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * {@link ZkNotifyBusTransport} 的便捷构造入口：内部创建并启动一个 {@link CuratorFramework}，
 * 该 client 的生命周期归 transport 所有，{@link ZkNotifyBusTransport#close()} 时一并关闭。
 * <p>
 * 如果应用已经自己维护了一个 {@link CuratorFramework}（比如复用给其他用途），
 * 应该直接用 {@link ZkNotifyBusTransport#ZkNotifyBusTransport(CuratorFramework, String)}，
 * 而不是这个 builder——那样 transport 不会去关闭一个不属于它的 client。
 */
public final class ZkNotifyBusTransportBuilder {

    private String connectString;
    private int sessionTimeoutMs = 60_000;
    private int connectionTimeoutMs = 15_000;
    private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    private String rootPath = "/notifybus";

    private ZkNotifyBusTransportBuilder() {
    }

    public static ZkNotifyBusTransportBuilder builder() {
        return new ZkNotifyBusTransportBuilder();
    }

    public ZkNotifyBusTransportBuilder connectString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    public ZkNotifyBusTransportBuilder sessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        return this;
    }

    public ZkNotifyBusTransportBuilder connectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    public ZkNotifyBusTransportBuilder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public ZkNotifyBusTransportBuilder rootPath(String rootPath) {
        this.rootPath = rootPath;
        return this;
    }

    public ZkNotifyBusTransport build() {
        if (connectString == null || connectString.isEmpty()) {
            throw new ConfigurationException("connectString must not be blank");
        }
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();
        return new ZkNotifyBusTransport(client, rootPath, true);
    }
}
