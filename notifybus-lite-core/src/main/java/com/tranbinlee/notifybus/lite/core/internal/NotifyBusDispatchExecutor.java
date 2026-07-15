package com.tranbinlee.notifybus.lite.core.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 命名、有界、可配置的调度线程池：把触发回调从 transport 的内部事件线程（比如 ZK 的
 * event thread）挪到独立线程执行，避免业务 handler 拖慢/阻塞 transport 自身的处理循环。
 * <p>
 * 队列打满时不做无界排队，也不阻塞调用方——直接丢弃并记录错误日志。这与本 SDK "只负责
 * 在线时触发、不保证补发" 的定位是一致的：持续过载下丢弃个别触发信号是可接受的降级，
 * 而不是让调用线程被拖死。
 */
public final class NotifyBusDispatchExecutor {

    private static final Logger log = LoggerFactory.getLogger(NotifyBusDispatchExecutor.class);

    private final ThreadPoolExecutor executor;

    public NotifyBusDispatchExecutor(String name, int corePoolSize, int maxPoolSize, int queueCapacity) {
        this.executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(queueCapacity),
                new NamedDaemonThreadFactory(name),
                new LoggingDropPolicy(name));
    }

    public void execute(Runnable task) {
        executor.execute(task);
    }

    public void shutdown(long awaitSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(awaitSeconds, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class LoggingDropPolicy implements RejectedExecutionHandler {
        private final String name;

        private LoggingDropPolicy(String name) {
            this.name = name;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            log.error("notify-bus-lite dispatch pool '{}' is saturated (queue full), dropping trigger dispatch: {}",
                    name, r);
        }
    }
}
