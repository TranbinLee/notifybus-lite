package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.internal.NoOpNotifyBusTransport;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@code notifybus.lite.enabled=false}（或未配置，即默认值）时的降级装配。
 * <p>
 * 与 {@link NotifyBusAutoConfiguration} 条件互斥（一个要求 {@code enabled=true}，
 * 一个要求 {@code enabled=false} 或缺省），两者合起来覆盖所有取值，保证不管开关状态如何，
 * {@link NotifyBusTransport}/{@link NotifyBusPublisher}/{@link NotifyBusConsumer} 三个 bean
 * 始终存在——业务代码 {@code @Autowired} 这三个类型不会因为 SDK 被关闭而启动失败。
 * <p>
 * 这里装配的 {@link NotifyBusTransport} 是 {@link NoOpNotifyBusTransport}：调用
 * {@code publish}/{@code subscribeTopic}/{@code subscribeResource} 只会打印一条 WARN
 * 日志、不抛异常、不做任何真正的发布/订阅。{@code @NotifyBusListener} 扫描器
 * （{@link NotifyBusListenerAutoConfiguration}）依然会照常装配并注册监听器，
 * 只是永远收不到事件——这是符合预期的降级行为。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyBusProperties.class)
@ConditionalOnProperty(prefix = "notifybus.lite", name = "enabled", havingValue = "false", matchIfMissing = true)
public class NotifyBusNoOpAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public NotifyBusTransport notifyBusLiteTransport() {
        return new NoOpNotifyBusTransport();
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
