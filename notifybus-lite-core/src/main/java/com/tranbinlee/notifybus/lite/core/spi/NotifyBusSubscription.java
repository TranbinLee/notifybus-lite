package com.tranbinlee.notifybus.lite.core.spi;

import java.io.Closeable;

/**
 * 一次订阅的句柄，{@link #close()} 用于取消订阅。
 */
public interface NotifyBusSubscription extends Closeable {

    @Override
    void close();
}
