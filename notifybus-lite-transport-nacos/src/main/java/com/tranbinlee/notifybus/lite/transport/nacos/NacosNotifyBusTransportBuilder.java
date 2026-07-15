package com.tranbinlee.notifybus.lite.transport.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;

import java.util.Properties;

/**
 * {@link NacosNotifyBusTransport} 的便捷构造入口：内部创建一个 {@link ConfigService}，
 * 该 client 的生命周期归 transport 所有，{@link NacosNotifyBusTransport#close()} 时一并 {@code shutDown()}。
 * <p>
 * 如果应用已经自己维护了一个 {@link ConfigService}（比如复用给其他用途），应该直接用
 * {@link NacosNotifyBusTransport#NacosNotifyBusTransport(ConfigService, String)}，而不是这个 builder——
 * 那样 transport 不会去关闭一个不属于它的 client。
 */
public final class NacosNotifyBusTransportBuilder {

    private String serverAddr;
    private String nacosNamespace;
    private String username;
    private String password;
    private String rootPath = "/notifybus";

    private NacosNotifyBusTransportBuilder() {
    }

    public static NacosNotifyBusTransportBuilder builder() {
        return new NacosNotifyBusTransportBuilder();
    }

    public NacosNotifyBusTransportBuilder serverAddr(String serverAddr) {
        this.serverAddr = serverAddr;
        return this;
    }

    /** Nacos 自身的租户隔离概念（{@code PropertyKeyConst.NAMESPACE}），和 NotifyBus 的 rootPath 是两回事。 */
    public NacosNotifyBusTransportBuilder nacosNamespace(String nacosNamespace) {
        this.nacosNamespace = nacosNamespace;
        return this;
    }

    public NacosNotifyBusTransportBuilder username(String username) {
        this.username = username;
        return this;
    }

    public NacosNotifyBusTransportBuilder password(String password) {
        this.password = password;
        return this;
    }

    public NacosNotifyBusTransportBuilder rootPath(String rootPath) {
        this.rootPath = rootPath;
        return this;
    }

    public NacosNotifyBusTransport build() {
        if (serverAddr == null || serverAddr.isEmpty()) {
            throw new ConfigurationException("serverAddr must not be blank");
        }
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        if (nacosNamespace != null && !nacosNamespace.isEmpty()) {
            properties.put(PropertyKeyConst.NAMESPACE, nacosNamespace);
        }
        if (username != null && !username.isEmpty()) {
            properties.put(PropertyKeyConst.USERNAME, username);
        }
        if (password != null && !password.isEmpty()) {
            properties.put(PropertyKeyConst.PASSWORD, password);
        }
        ConfigService configService;
        try {
            configService = NacosFactory.createConfigService(properties);
        } catch (NacosException e) {
            throw new NotifyBusLiteException("failed to create nacos ConfigService for serverAddr=" + serverAddr, e);
        }
        return new NacosNotifyBusTransport(configService, rootPath, true);
    }
}
