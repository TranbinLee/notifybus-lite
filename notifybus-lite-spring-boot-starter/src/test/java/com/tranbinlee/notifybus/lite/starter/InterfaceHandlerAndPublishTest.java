package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusHandler;
import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 覆盖接口式订阅（bean 直接 {@code implements NotifyBusHandler}）：topic 级订阅、
 * resourceKey 级订阅均能收到端到端发布的事件；{@code getTopic()} 为空白时启动直接失败。
 */
class InterfaceHandlerAndPublishTest {

    @Test
    void discoversHandlerBeansAndDeliversPublishedEventsEndToEnd() throws Exception {
        TestingServer testingServer = new TestingServer();
        ConfigurableApplicationContext context = null;
        try {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("notifybus.lite.enabled", "true");
            properties.put("notifybus.lite.zk.connect-string", testingServer.getConnectString());
            properties.put("notifybus.lite.namespace", "/notifybus-handler-test");

            context = new SpringApplicationBuilder(TestApp.class)
                    .web(WebApplicationType.NONE)
                    .properties(properties)
                    .run();

            NotifyBusPublisher publisher = context.getBean(NotifyBusPublisher.class);
            TopicLevelHandler topicLevelHandler = context.getBean(TopicLevelHandler.class);
            ResourceLevelHandler resourceLevelHandler = context.getBean(ResourceLevelHandler.class);

            publisher.publish("orders", "order-1", ChangeType.UPDATED);
            publisher.publish("invoices", "inv-1", ChangeType.CREATED);

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(topicLevelHandler.received).hasSize(1));
            assertThat(topicLevelHandler.received.get(0).getTopic()).isEqualTo("orders");
            assertThat(topicLevelHandler.received.get(0).getResourceKey()).isEqualTo("order-1");

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(resourceLevelHandler.received).hasSize(1));
            assertThat(resourceLevelHandler.received.get(0).getChangeType()).isEqualTo(ChangeType.CREATED);
        } finally {
            if (context != null) {
                context.close();
            }
            testingServer.close();
        }
    }

    @Test
    void failsStartupWithConfigurationExceptionWhenHandlerTopicIsBlank() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        NotifyBusAutoConfiguration.class,
                        NotifyBusNoOpAutoConfiguration.class,
                        NotifyBusListenerAutoConfiguration.class))
                .withBean(BlankTopicHandler.class)
                .run(runningContext -> assertThat(runningContext).getFailure()
                        .isInstanceOf(ConfigurationException.class));
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestApp {

        @Bean
        TopicLevelHandler topicLevelHandler() {
            return new TopicLevelHandler();
        }

        @Bean
        ResourceLevelHandler resourceLevelHandler() {
            return new ResourceLevelHandler();
        }
    }

    /** topic 级订阅：不覆写 {@code getResourceKey()}，收到 "orders" 下所有资源的变化。 */
    static class TopicLevelHandler implements NotifyBusHandler {

        final List<NotifyBusEvent> received = new CopyOnWriteArrayList<NotifyBusEvent>();

        @Override
        public String getTopic() {
            return "orders";
        }

        @Override
        public void onTrigger(NotifyBusEvent event) {
            received.add(event);
        }
    }

    /** resourceKey 级订阅：只关心 "invoices" 下 "inv-1" 这一个具体资源。 */
    static class ResourceLevelHandler implements NotifyBusHandler {

        final List<NotifyBusEvent> received = new CopyOnWriteArrayList<NotifyBusEvent>();

        @Override
        public String getTopic() {
            return "invoices";
        }

        @Override
        public String getResourceKey() {
            return "inv-1";
        }

        @Override
        public void onTrigger(NotifyBusEvent event) {
            received.add(event);
        }
    }

    /** {@code getTopic()} 返回空白，期望启动时 fail-fast。 */
    static class BlankTopicHandler implements NotifyBusHandler {

        @Override
        public String getTopic() {
            return "  ";
        }

        @Override
        public void onTrigger(NotifyBusEvent event) {
            // never invoked
        }
    }
}
