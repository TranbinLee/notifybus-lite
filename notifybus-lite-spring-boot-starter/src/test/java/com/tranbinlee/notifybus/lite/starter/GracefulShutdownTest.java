package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.event.ChangeType;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.exception.NotifyBusLiteException;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusPublisher;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusTransport;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 优雅关闭：{@code context.close()} 之后，调度线程池必须完全退出（不留残留线程阻塞 JVM），
 * 且自建的 {@code CuratorFramework} 必须被关闭（表现为 close 之后再 publish 会失败）。
 */
class GracefulShutdownTest {

    @Test
    void contextCloseTerminatesDispatchPoolAndClosesOwnedZkClient() throws Exception {
        TestingServer testingServer = new TestingServer();
        try {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("notifybus.lite.enabled", "true");
            properties.put("notifybus.lite.zk.connect-string", testingServer.getConnectString());
            properties.put("notifybus.lite.namespace", "/notifybus-shutdown-test");
            properties.put("notifybus.lite.dispatch.thread-name-prefix", "notifybus-lite-dispatch-shutdown-test");

            ConfigurableApplicationContext context = new SpringApplicationBuilder(ShutdownApp.class)
                    .web(WebApplicationType.NONE)
                    .properties(properties)
                    .run();

            NotifyBusTransport transport = context.getBean(NotifyBusTransport.class);
            NotifyBusConsumer consumer = context.getBean(NotifyBusConsumer.class);
            NotifyBusPublisher publisher = context.getBean(NotifyBusPublisher.class);

            // 触发一次 dispatch，让调度线程池的核心线程真正起来（ThreadPoolExecutor 默认懒启动）。
            AtomicBoolean delivered = new AtomicBoolean(false);
            consumer.subscribeTopic("orders", event -> delivered.set(true));
            publisher.publish("orders", "order-1", ChangeType.UPDATED);
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(delivered.get()).isTrue());
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(dispatchThreadNames()).isNotEmpty());

            context.close();

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(dispatchThreadNames()).as("dispatch pool threads must terminate on shutdown").isEmpty());

            assertThatThrownBy(() -> transport.publish(NotifyBusEvent.builder()
                    .topic("orders")
                    .resourceKey("order-1")
                    .changeType(ChangeType.UPDATED)
                    .timestamp(System.currentTimeMillis())
                    .build()))
                    .as("the self-managed CuratorFramework must be closed on shutdown")
                    .isInstanceOf(NotifyBusLiteException.class);
        } finally {
            testingServer.close();
        }
    }

    private static Set<String> dispatchThreadNames() {
        Set<String> names = new java.util.HashSet<String>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("notifybus-lite-dispatch-shutdown-test")) {
                names.add(thread.getName());
            }
        }
        return names;
    }

    @Configuration
    @EnableAutoConfiguration
    static class ShutdownApp {
    }
}
