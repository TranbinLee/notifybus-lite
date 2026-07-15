package com.tranbinlee.notifybus.lite.starter.listener;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;
import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 反射调用 {@code @NotifyBusListener} 标注方法的 {@link NotifyBusListener} 适配器。
 */
public class NotifyBusMethodListener implements NotifyBusListener {

    private final Object bean;
    private final Method method;

    public NotifyBusMethodListener(Object bean, Method method) {
        this.bean = bean;
        this.method = method;
        this.method.setAccessible(true);
    }

    @Override
    public void onTrigger(NotifyBusEvent event) {
        try {
            method.invoke(bean, event);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("failed to invoke @NotifyBusListener method " + method, e);
        }
    }
}
