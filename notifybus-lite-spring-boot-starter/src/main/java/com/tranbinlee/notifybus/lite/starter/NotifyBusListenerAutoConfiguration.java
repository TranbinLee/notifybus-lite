package com.tranbinlee.notifybus.lite.starter;

import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.starter.listener.NotifyBusListenerBeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 订阅发现装配：{@code @NotifyBusListener} 注解方法扫描 + {@code NotifyBusHandler} 接口实现扫描。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBean(NotifyBusConsumer.class)
@AutoConfigureAfter({NotifyBusAutoConfiguration.class, NotifyBusNoOpAutoConfiguration.class})
public class NotifyBusListenerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public static NotifyBusListenerBeanPostProcessor notifyListenerBeanPostProcessor() {
        return new NotifyBusListenerBeanPostProcessor();
    }
}
