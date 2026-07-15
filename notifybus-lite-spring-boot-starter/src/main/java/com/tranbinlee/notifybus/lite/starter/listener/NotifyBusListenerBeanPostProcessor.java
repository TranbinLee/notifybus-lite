package com.tranbinlee.notifybus.lite.starter.listener;

import com.tranbinlee.notifybus.lite.core.api.NotifyBusConsumer;
import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.api.NotifyBusHandler;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusSubscription;
import com.tranbinlee.notifybus.lite.core.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 发现所有 Spring bean 上的订阅意图并注册，支持两种形式：
 * <ul>
 *     <li>注解式：{@code @NotifyBusListener} 标注的方法；</li>
 *     <li>接口式：直接实现 {@link NotifyBusHandler} 的 bean。</li>
 * </ul>
 * <p>
 * 不在 {@link BeanPostProcessor} 里直接持有/注入 {@link NotifyBusConsumer}——那是已知的 Spring 反模式，
 * 会导致其依赖提前初始化并绕过自身的后置处理。真正的 {@code consumer.subscribeXxx(...)} 调用被推迟到
 * {@link #afterSingletonsInstantiated()}，此时所有单例（含代理）都已就绪，再通过持有的
 * {@link ApplicationContext} 取出 {@link NotifyBusConsumer} bean。
 */
public class NotifyBusListenerBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton,
        ApplicationContextAware, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(NotifyBusListenerBeanPostProcessor.class);

    private final List<PendingRegistration> pendingRegistrations = new CopyOnWriteArrayList<PendingRegistration>();
    private final List<NotifyBusHandler> pendingHandlers = new CopyOnWriteArrayList<NotifyBusHandler>();
    private final List<NotifyBusSubscription> activeNotifyBusSubscriptions = new CopyOnWriteArrayList<NotifyBusSubscription>();

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof NotifyBusHandler) {
            pendingHandlers.add((NotifyBusHandler) bean);
        }
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        Map<Method, NotifyBusListener> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                new MethodIntrospector.MetadataLookup<NotifyBusListener>() {
                    @Override
                    public NotifyBusListener inspect(Method method) {
                        return AnnotatedElementUtils.findMergedAnnotation(method, NotifyBusListener.class);
                    }
                });
        if (annotatedMethods.isEmpty()) {
            return bean;
        }
        for (Map.Entry<Method, NotifyBusListener> entry : annotatedMethods.entrySet()) {
            Method declaredMethod = entry.getKey();
            NotifyBusListener annotation = entry.getValue();
            validateSignature(beanName, declaredMethod);
            Method invocableMethod = AopUtils.selectInvocableMethod(declaredMethod, bean.getClass());
            pendingRegistrations.add(new PendingRegistration(bean, invocableMethod, annotation));
        }
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (pendingRegistrations.isEmpty() && pendingHandlers.isEmpty()) {
            return;
        }
        NotifyBusConsumer consumer = applicationContext.getBean(NotifyBusConsumer.class);
        for (PendingRegistration registration : pendingRegistrations) {
            NotifyBusListener annotation = registration.annotation;
            NotifyBusMethodListener listener = new NotifyBusMethodListener(registration.bean, registration.method);
            NotifyBusSubscription notifyBusSubscription = annotation.resourceKey().isEmpty()
                    ? consumer.subscribeTopic(annotation.topic(), listener)
                    : consumer.subscribeResource(annotation.topic(), annotation.resourceKey(), listener);
            activeNotifyBusSubscriptions.add(notifyBusSubscription);
            log.info("registered @NotifyBusListener topic={} resourceKey={}", annotation.topic(),
                    annotation.resourceKey().isEmpty() ? "<whole-topic>" : annotation.resourceKey());
        }
        for (NotifyBusHandler handler : pendingHandlers) {
            String topic = handler.getTopic();
            if (topic == null || topic.trim().isEmpty()) {
                throw new ConfigurationException("NotifyBusHandler " + handler.getClass().getName() +
                        "#getTopic() must not be blank");
            }
            String resourceKey = handler.getResourceKey();
            NotifyBusSubscription notifyBusSubscription = (resourceKey == null || resourceKey.isEmpty())
                    ? consumer.subscribeTopic(topic, handler)
                    : consumer.subscribeResource(topic, resourceKey, handler);
            activeNotifyBusSubscriptions.add(notifyBusSubscription);
            log.info("registered NotifyBusHandler {} topic={} resourceKey={}",
                    handler.getClass().getName(), topic,
                    (resourceKey == null || resourceKey.isEmpty()) ? "<whole-topic>" : resourceKey);
        }
    }

    @Override
    public void destroy() {
        for (NotifyBusSubscription notifyBusSubscription : activeNotifyBusSubscriptions) {
            notifyBusSubscription.close();
        }
        activeNotifyBusSubscriptions.clear();
    }

    private static void validateSignature(String beanName, Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || !NotifyBusEvent.class.isAssignableFrom(parameterTypes[0])) {
            throw new ConfigurationException("@NotifyBusListener method " + beanName + "#" + method.getName() +
                    " must take exactly one " + NotifyBusEvent.class.getSimpleName() + " parameter");
        }
        if (method.getReturnType() != void.class) {
            throw new ConfigurationException("@NotifyBusListener method " + beanName + "#" + method.getName() +
                    " must return void");
        }
    }

    private static final class PendingRegistration {
        private final Object bean;
        private final Method method;
        private final NotifyBusListener annotation;

        private PendingRegistration(Object bean, Method method, NotifyBusListener annotation) {
            this.bean = bean;
            this.method = method;
            this.annotation = annotation;
        }
    }
}
