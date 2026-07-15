package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import com.tranbinlee.notifybus.lite.core.internal.NoOpNotifyBusTransport;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * 覆盖 {@link NotifyBusAutoConfiguration}/{@link NotifyBusNoOpAutoConfiguration} 的行为：
 * <ol>
 *     <li>{@code notifybus.lite.enabled} 默认 {@code false}，不显式配置为 {@code true} 时装配的是
 *     {@link NoOpNotifyBusTransport}，而不是完全没有 bean——业务代码 {@code @Autowired}
 *     {@link NotifyBusPublisher}/{@link NotifyBusConsumer} 不会因为 SDK 关闭而启动失败，调用
 *     {@code publish}/{@code subscribeTopic} 也只是打印 WARN、不抛异常；</li>
 *     <li>{@code notifybus.lite.type} 决定启用状态下用哪个 transport 实现，{@code zookeeper}/{@code nacos}
 *     都已实现，{@code db} 目前尚未实现，启动时应该直接失败并报 {@link ConfigurationException}，而不是
 *     静默退化成 zookeeper。</li>
 * </ol>
 * 这里用 {@link ApplicationContextRunner} 而不是现有测试用的 {@code SpringApplicationBuilder}：只需要验证
 * bean 是否存在/启动是否失败，不需要真的连上一个 ZooKeeper/Nacos，所以不依赖内嵌 server——
 * {@code CuratorFramework}/{@code ConfigService} 的构造/{@code start()} 本身不会同步等待连接成功。
 */
class NotifyBusLiteConditionalAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    NotifyBusAutoConfiguration.class, NotifyBusNoOpAutoConfiguration.class));

    @Test
    void configuresNoOpTransportWhenEnabledIsUnset() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(NotifyBusTransport.class);
            assertThat(context.getBean(NotifyBusTransport.class)).isInstanceOf(NoOpNotifyBusTransport.class);
            assertThat(context).hasSingleBean(NotifyBusPublisher.class);
            assertThat(context).hasSingleBean(NotifyBusConsumer.class);
        });
    }

    @Test
    void configuresNoOpTransportWhenEnabledIsExplicitlyFalse() {
        contextRunner.withPropertyValues("notifybus.lite.enabled=false").run(context -> {
            assertThat(context).hasSingleBean(NotifyBusTransport.class);
            assertThat(context.getBean(NotifyBusTransport.class)).isInstanceOf(NoOpNotifyBusTransport.class);
            assertThat(context).hasSingleBean(NotifyBusPublisher.class);
            assertThat(context).hasSingleBean(NotifyBusConsumer.class);
        });
    }

    @Test
    void noOpPublisherAndConsumerNeverThrowWhenSdkIsDisabled() {
        contextRunner.run(context -> {
            NotifyBusPublisher publisher = context.getBean(NotifyBusPublisher.class);
            NotifyBusConsumer consumer = context.getBean(NotifyBusConsumer.class);

            assertThatCode(() -> publisher.publish("product-config", "1", ChangeType.UPDATED))
                    .doesNotThrowAnyException();
            assertThatCode(() -> consumer.subscribeTopic("product-config", event -> { }))
                    .doesNotThrowAnyException();
            assertThatCode(() -> consumer.subscribeResource("product-config", "1", event -> { }))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    void configuresAllBeansWhenEnabledAndTypeDefaultsToZookeeper() {
        contextRunner
                .withPropertyValues(
                        "notifybus.lite.enabled=true",
                        "notifybus.lite.zk.connect-string=127.0.0.1:2181")
                .run(context -> {
                    assertThat(context).hasSingleBean(NotifyBusTransport.class);
                    assertThat(context).hasSingleBean(NotifyBusPublisher.class);
                    assertThat(context).hasSingleBean(NotifyBusConsumer.class);
                });
    }

    @Test
    void configuresAllBeansWhenEnabledAndTypeExplicitlyZookeeper() {
        contextRunner
                .withPropertyValues(
                        "notifybus.lite.enabled=true",
                        "notifybus.lite.type=zookeeper",
                        "notifybus.lite.zk.connect-string=127.0.0.1:2181")
                .run(context -> {
                    assertThat(context).hasSingleBean(NotifyBusTransport.class);
                    assertThat(context).hasSingleBean(NotifyBusPublisher.class);
                    assertThat(context).hasSingleBean(NotifyBusConsumer.class);
                });
    }

    @Test
    void configuresAllBeansWhenEnabledAndTypeIsNacos() {
        contextRunner
                .withPropertyValues(
                        "notifybus.lite.enabled=true",
                        "notifybus.lite.type=nacos",
                        "notifybus.lite.nacos.server-addr=127.0.0.1:8848")
                .run(context -> {
                    assertThat(context).hasSingleBean(NotifyBusTransport.class);
                    assertThat(context).hasSingleBean(NotifyBusPublisher.class);
                    assertThat(context).hasSingleBean(NotifyBusConsumer.class);
                });
    }

    @Test
    void failsStartupWithConfigurationExceptionWhenTypeIsDb() {
        contextRunner
                .withPropertyValues("notifybus.lite.enabled=true", "notifybus.lite.type=db")
                .run(context -> assertThat(context).getFailure()
                        .hasRootCauseInstanceOf(ConfigurationException.class));
    }

    @Test
    void failsStartupWithConfigurationExceptionWhenTypeIsNacosButServerAddrMissing() {
        contextRunner
                .withPropertyValues("notifybus.lite.enabled=true", "notifybus.lite.type=nacos")
                .run(context -> assertThat(context).getFailure()
                        .hasRootCauseInstanceOf(ConfigurationException.class));
    }
}
