package com.tranbinlee.notifybus.lite.core.api;

import com.tranbinlee.notifybus.lite.core.spi.NotifyBusListener;

/**
 * 接口实现形式的订阅入口：bean 直接实现本接口即可被自动发现并订阅，无需
 * {@code @NotifyBusListener} 注解。一个实现类只能声明一个订阅目标（一个 topic，
 * 可选一个 resourceKey）；需要订阅多个 topic 或 resourceKey 时，注册多个 bean。
 */
public interface NotifyBusHandler extends NotifyBusListener {

    /**
     * 订阅的 topic，必填，不能为空白。
     *
     * @return topic 名称
     */
    String getTopic();

    /**
     * 订阅的具体资源；默认空字符串表示订阅整个 topic 下所有资源的变化。
     *
     * @return resourceKey，留空表示订阅整个 topic
     */
    default String getResourceKey() {
        return "";
    }
}
