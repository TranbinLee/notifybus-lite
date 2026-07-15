package com.tranbinlee.notifybus.lite.starter.listener;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在方法上，方法所在 bean 会被自动订阅到指定 topic（或 topic 下某个具体 resourceKey）。
 * 方法签名固定为 {@code void xxx(NotifyBusEvent event)}。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NotifyBusListener {

    /** 订阅的 topic，必填。 */
    String topic();

    /** 订阅的具体资源；留空（默认）表示订阅整个 topic 下所有资源的变化。 */
    String resourceKey() default "";
}
