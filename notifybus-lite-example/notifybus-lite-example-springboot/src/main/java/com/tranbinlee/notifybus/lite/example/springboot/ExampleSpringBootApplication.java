package com.tranbinlee.notifybus.lite.example.springboot;

import com.tranbinlee.notifybus.lite.example.springboot.consumer.cache.CacheStateReporter;
import com.tranbinlee.notifybus.lite.example.springboot.consumer.cache.CatalogCacheListener;
import com.tranbinlee.notifybus.lite.example.springboot.consumer.cache.CatalogSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 单一进程同时扮演发布方和消费方：{@code notifybus-lite-spring-boot-starter} 自动装配好
 * {@code NotifyBusTransport}/{@code NotifyBusPublisher}/{@code NotifyBusConsumer}，业务代码只需要
 * 注入 {@code NotifyBusPublisher}（见 {@code publisher} 包下的 {@code CatalogChangePublisher}/
 * {@code CatalogChangeController}）或者用 {@code @NotifyBusListener} 标注方法（见 {@code consumer}
 * 包下的 {@link CatalogCacheListener}）即可，完全不用关心 ZooKeeper 长什么样。
 * <p>
 * 把发布方和消费方合成一个进程纯粹是为了让这个示例"开箱即跑"：启动后直接用 curl 触发一次
 * {@code POST /catalog/{sku}/changes}，同一个进程里的 {@code @NotifyBusListener} 立刻就能收到，
 * {@link CacheStateReporter} 的定时日志里马上能看到缓存被刷新——不需要再额外起第二个进程。
 * 生产环境里发布方和消费方几乎总是不同的服务/进程，这里只是为了演示方便合并在一起。
 */
@SpringBootApplication
@EnableScheduling
public class ExampleSpringBootApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleSpringBootApplication.class, args);
    }

    @Bean
    public CatalogSource catalogSource() {
        return new CatalogSource();
    }

    @Bean
    public CatalogCacheListener catalogCacheListener(CatalogSource catalogSource) {
        return new CatalogCacheListener(catalogSource);
    }

    @Bean
    public CacheStateReporter cacheStateReporter(CatalogCacheListener catalogCacheListener) {
        return new CacheStateReporter(catalogCacheListener);
    }
}
