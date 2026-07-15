package com.tranbinlee.notifybus.lite.core.internal;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 产出带统一前缀命名的线程，方便 jstack/监控里区分线程池归属。
 */
final class NamedDaemonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger counter = new AtomicInteger(1);

    NamedDaemonThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, prefix + "-" + counter.getAndIncrement());
        thread.setDaemon(true);
        return thread;
    }
}
