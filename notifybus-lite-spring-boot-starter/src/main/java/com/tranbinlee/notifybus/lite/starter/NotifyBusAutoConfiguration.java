package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import com.tranbinlee.notifybus.lite.transport.nacos.NacosNotifyBusTransport;
import com.tranbinlee.notifybus.lite.transport.nacos.NacosNotifyBusTransportBuilder;
import com.tranbinlee.notifybus.lite.transport.zk.ZkNotifyBusTransport;
import com.tranbinlee.notifybus.lite.transport.zk.ZkNotifyBusTransportBuilder;
import com.alibaba.nacos.api.config.ConfigService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 核心装配：{@link NotifyBusTransport}/{@link NotifyBusPublisher}/{@link NotifyBusConsumer}。
 * <p>
 * 整个类只在显式配置 {@code notifybus.lite.enabled=true} 时才生效——引入 starter 依赖本身
 * 不会让任何 bean 生效，必须显式开启，避免"加了依赖就默默连上 ZK/Nacos"这种意外行为。
 * <p>
 * 用哪个 transport 由 {@code notifybus.lite.type} 决定（见 {@link NotifyBusProperties.TransportType}）。
 * ZooKeeper 和 Nacos 都支持"应用自己已经维护了一个 client bean（{@link CuratorFramework}/
 * {@link ConfigService}）就直接复用"——复用时 {@code close()} 不会把它关掉；否则用
 * {@code notifybus.lite.zk.*}/{@code notifybus.lite.nacos.*} 配置自建一个专属的 client，生命周期完全
 * 归 starter 管理。配置成 {@code db} 会在装配时直接报错，因为对应的 transport 模块还没有实现。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyBusProperties.class)
@ConditionalOnProperty(prefix = "notifybus.lite", name = "enabled", havingValue = "true")
public class NotifyBusAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public NotifyBusTransport notifyBusLiteTransport(NotifyBusProperties properties,
                                                            ObjectProvider<CuratorFramework> curatorFrameworkProvider,
                                                            ObjectProvider<ConfigService> configServiceProvider) {
        NotifyBusProperties.TransportType type = properties.getType();
        switch (type) {
            case ZOOKEEPER:
                return createZookeeperTransport(properties, curatorFrameworkProvider);
            case NACOS:
                return createNacosTransport(properties, configServiceProvider);
            case DB:
                throw new ConfigurationException("notifybus.lite.type=db is not implemented yet in this version "
                        + "of NotifyBus Lite (notifybus-lite-transport-db does not exist yet).");
            default:
                throw new ConfigurationException("unsupported notifybus.lite.type: " + type);
        }
    }

    private NotifyBusTransport createZookeeperTransport(NotifyBusProperties properties,
                                                        ObjectProvider<CuratorFramework> curatorFrameworkProvider) {
        CuratorFramework existingClient = curatorFrameworkProvider.getIfAvailable();
        if (existingClient != null) {
            return new ZkNotifyBusTransport(existingClient, properties.getNamespace());
        }
        String connectString = properties.getZk().getConnectString();
        if (connectString == null || connectString.isEmpty()) {
            throw new ConfigurationException("notifybus.lite.zk.connect-string must not be empty " +
                    "(or provide your own CuratorFramework bean)");
        }
        return ZkNotifyBusTransportBuilder.builder()
                .connectString(connectString)
                .sessionTimeoutMs((int) properties.getZk().getSessionTimeout().toMillis())
                .connectionTimeoutMs((int) properties.getZk().getConnectionTimeout().toMillis())
                .retryPolicy(new ExponentialBackoffRetry(
                        properties.getZk().getRetry().getBaseSleepMs(),
                        properties.getZk().getRetry().getMaxRetries()))
                .rootPath(properties.getNamespace())
                .build();
    }

    private NotifyBusTransport createNacosTransport(NotifyBusProperties properties,
                                                    ObjectProvider<ConfigService> configServiceProvider) {
        ConfigService existingConfigService = configServiceProvider.getIfAvailable();
        if (existingConfigService != null) {
            return new NacosNotifyBusTransport(existingConfigService, properties.getNamespace());
        }
        String serverAddr = properties.getNacos().getServerAddr();
        if (serverAddr == null || serverAddr.isEmpty()) {
            throw new ConfigurationException("notifybus.lite.nacos.server-addr must not be empty " +
                    "(or provide your own ConfigService bean)");
        }
        return NacosNotifyBusTransportBuilder.builder()
                .serverAddr(serverAddr)
                .nacosNamespace(properties.getNacos().getNamespace())
                .username(properties.getNacos().getUsername())
                .password(properties.getNacos().getPassword())
                .rootPath(properties.getNamespace())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public NotifyBusPublisher notifyBusLitePublisher(NotifyBusTransport transport) {
        return new NotifyBusPublisher(transport);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public NotifyBusConsumer notifyBusLiteConsumer(NotifyBusTransport transport, NotifyBusProperties properties) {
        NotifyBusProperties.Dispatch dispatch = properties.getDispatch();
        return NotifyBusConsumer.builder(transport)
                .dispatchPoolName(dispatch.getThreadNamePrefix())
                .corePoolSize(dispatch.getCorePoolSize())
                .maxPoolSize(dispatch.getMaxPoolSize())
                .queueCapacity(dispatch.getQueueCapacity())
                .build();
    }
}
