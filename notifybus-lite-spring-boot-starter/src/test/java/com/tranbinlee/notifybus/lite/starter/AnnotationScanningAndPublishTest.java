package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.starter.listener.NotifyBusListener;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
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

/** 覆盖 @NotifyBusListener 注解扫描：topic 级订阅 + resourceKey 级订阅，均能收到端到端发布的事件。 */
class AnnotationScanningAndPublishTest {

    @Test
    void scansAnnotatedMethodsAndDeliversPublishedEventsEndToEnd() throws Exception {
        TestingServer testingServer = new TestingServer();
        ConfigurableApplicationContext context = null;
        try {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("notifybus.lite.enabled", "true");
            properties.put("notifybus.lite.zk.connect-string", testingServer.getConnectString());
            properties.put("notifybus.lite.namespace", "/notifybus-scan-test");

            context = new SpringApplicationBuilder(TestApp.class)
                    .web(WebApplicationType.NONE)
                    .properties(properties)
                    .run();

            NotifyBusPublisher publisher = context.getBean(NotifyBusPublisher.class);
            RecordingListener recordingListener = context.getBean(RecordingListener.class);

            publisher.publish("orders", "order-1", ChangeType.UPDATED);
            publisher.publish("invoices", "inv-1", ChangeType.CREATED);

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(recordingListener.topicEvents).hasSize(1));
            assertThat(recordingListener.topicEvents.get(0).getTopic()).isEqualTo("orders");
            assertThat(recordingListener.topicEvents.get(0).getResourceKey()).isEqualTo("order-1");

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(recordingListener.resourceEvents).hasSize(1));
            assertThat(recordingListener.resourceEvents.get(0).getChangeType()).isEqualTo(ChangeType.CREATED);
        } finally {
            if (context != null) {
                context.close();
            }
            testingServer.close();
        }
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestApp {

        @Bean
        RecordingListener recordingListener() {
            return new RecordingListener();
        }
    }

    static class RecordingListener {

        final List<NotifyBusEvent> topicEvents = new CopyOnWriteArrayList<NotifyBusEvent>();
        final List<NotifyBusEvent> resourceEvents = new CopyOnWriteArrayList<NotifyBusEvent>();

        @NotifyBusListener(topic = "orders")
        public void onAnyOrderChanged(NotifyBusEvent event) {
            topicEvents.add(event);
        }

        @NotifyBusListener(topic = "invoices", resourceKey = "inv-1")
        public void onInvoiceOneChanged(NotifyBusEvent event) {
            resourceEvents.add(event);
        }
    }
}
