package com.tranbinlee.notifybus.lite.core.spi;

import com.tranbinlee.notifybus.lite.core.event.NotifyBusEvent;

/**
 * 收到触发信号时的回调。实现方只应该做"决定要不要刷新/怎么刷新"这类业务判断，
 * 不应该假设自己跑在哪个线程上——具体在哪个线程执行由 core 层的调度策略决定。
 */
public interface NotifyBusListener {

    /**
     * @param event 触发事件
     */
    void onTrigger(NotifyBusEvent event);
}
