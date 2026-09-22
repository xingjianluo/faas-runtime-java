package com.github.xingjianluo.faasruntimejava.metrics;

import com.github.xingjianluo.faasruntimejava.model.InstanceMetrics;
import com.github.xingjianluo.faasruntimejava.model.InvocationStatus;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 按 函数+版本 维度汇总调用分类计数与冷启动次数。
 * 计数全部使用原子变量，并发调用下指标可复现。
 */
public final class MetricsCollector {

    private record Key(String function, int version) {
    }

    private static final class Counters {
        final AtomicLong total = new AtomicLong();
        final AtomicLong success = new AtomicLong();
        final AtomicLong replay = new AtomicLong();
        final AtomicLong timeout = new AtomicLong();
        final AtomicLong functionError = new AtomicLong();
        final AtomicLong concurrencyRejected = new AtomicLong();
        final AtomicLong coldStarts = new AtomicLong();
    }

    private final ConcurrentHashMap<Key, Counters> all = new ConcurrentHashMap<>();

    private Counters counters(String function, int version) {
        return all.computeIfAbsent(new Key(function, version), k -> new Counters());
    }

    /**
     * 记录一次调用终局。
     *
     * @param status    结果分类（REPLAY 也计入 total 并单独计数）
     * @param coldStart 是否冷启动（重放沿用首次结果的冷启动标记，不重复计数）
     */
    public void recordOutcome(String function, int version, InvocationStatus status,
                              boolean coldStart) {
        Counters c = counters(function, version);
        c.total.incrementAndGet();
        switch (status) {
            case SUCCESS -> c.success.incrementAndGet();
            case REPLAY -> c.replay.incrementAndGet();
            case TIMEOUT -> c.timeout.incrementAndGet();
            case FUNCTION_ERROR -> c.functionError.incrementAndGet();
            case CONCURRENCY_REJECTED -> c.concurrencyRejected.incrementAndGet();
            default -> { /* INVALID_ARGUMENT / VERSION_NOT_FOUND 不针对具体版本计数 */ }
        }
        if (coldStart && status != InvocationStatus.REPLAY) {
            c.coldStarts.incrementAndGet();
        }
    }

    public InstanceMetrics snapshot(String function, int version, int created, int idle,
                                    int inflight) {
        Counters c = counters(function, version);
        return new InstanceMetrics(function, version, created, idle, inflight,
                c.total.get(), c.success.get(), c.replay.get(),
                c.timeout.get(), c.functionError.get(),
                c.concurrencyRejected.get(), c.coldStarts.get());
    }
}
