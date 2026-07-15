package com.tranbinlee.notifybus.lite.core;

import com.tranbinlee.notifybus.lite.core.internal.NotifyBusDispatchExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class NotifyBusDispatchExecutorTest {

    /**
     * 单线程、队列容量 1：一个任务占住唯一线程，一个塞进队列，再往里提交的任务会被丢弃策略吞掉，
     * 而不是阻塞调用方或无界堆积。验证"过载丢弃"这一核心降级语义。
     */
    @Test
    void dropsTasksWhenSaturatedInsteadOfBlockingCaller() throws InterruptedException {
        NotifyBusDispatchExecutor executor = new NotifyBusDispatchExecutor("test-drop", 1, 1, 1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstStarted = new CountDownLatch(1);
        AtomicInteger executed = new AtomicInteger();

        // 任务1：占住唯一线程直到 release
        executor.execute(() -> {
            firstStarted.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executed.incrementAndGet();
        });
        assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();

        // 任务2：进入容量为 1 的队列
        executor.execute(executed::incrementAndGet);
        // 任务3、4：队列已满 -> 被丢弃，execute() 不抛异常、不阻塞
        executor.execute(executed::incrementAndGet);
        executor.execute(executed::incrementAndGet);

        release.countDown();
        executor.shutdown(5);

        // 只有占线程的那个 + 队列里的那个真正跑了，被丢的两个不计数
        assertThat(executed.get()).isEqualTo(2);
    }

    @Test
    void shutdownWaitsForInFlightTaskToComplete() {
        NotifyBusDispatchExecutor executor = new NotifyBusDispatchExecutor("test-shutdown", 1, 1, 10);
        AtomicInteger completed = new AtomicInteger();

        executor.execute(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            completed.incrementAndGet();
        });

        executor.shutdown(5); // 等待窗口足够，在途任务应正常跑完
        assertThat(completed.get()).isEqualTo(1);
    }

    @Test
    void shutdownForcesTerminationWhenAwaitWindowElapses() {
        NotifyBusDispatchExecutor executor = new NotifyBusDispatchExecutor("test-force", 1, 1, 10);
        AtomicInteger interrupted = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);

        executor.execute(() -> {
            started.countDown();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                interrupted.incrementAndGet();
            }
        });

        await().atMost(Duration.ofSeconds(2)).until(() -> started.getCount() == 0);
        executor.shutdown(1); // 1s 后仍未结束 -> shutdownNow 中断任务

        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(interrupted.get()).isEqualTo(1));
    }
}
