package com.github.xingjianluo.faasruntimejava.faas;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实例管理：按 (函数, 版本) 维护暖实例，空闲超时后缩容到零。
 *
 * <p>并发约定：
 * <ul>
 *   <li>获取实例与缩容回收都通过 {@link ConcurrentHashMap#compute} 系列方法
 *       在同一 key 上原子完成，因此“缩容与调用同时发生”时调用要么复用旧实例、
 *       要么触发一次新的冷启动，不会丢失也不会重复执行；</li>
 *   <li>正在执行（inFlight &gt; 0）的实例不会被回收。</li>
 * </ul>
 */
@Component
public class InstanceManager {

    /** 单个暖实例的账本 */
    private static final class Instance {
        final AtomicLong lastUsedMillis;
        final AtomicInteger inFlight = new AtomicInteger(0);

        Instance(long now) {
            this.lastUsedMillis = new AtomicLong(now);
        }
    }

    private final Map<String, Instance> instances = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faas-instance-reaper");
                t.setDaemon(true);
                return t;
            });

    /** 空闲多久后缩容，默认 30s，测试可调小 */
    private volatile long idleTimeoutMillis = 30_000;
    /** 缩容扫描周期，默认 5s，测试可调小 */
    private volatile long reapIntervalMillis = 5_000;

    private final AtomicLong coldStarts = new AtomicLong(0);
    private final AtomicLong warmHits = new AtomicLong(0);

    private ScheduledFuture<?> reapTask;

    public InstanceManager() {
        scheduleReaper();
    }

    private synchronized void scheduleReaper() {
        if (reapTask != null) {
            reapTask.cancel(false);
        }
        reapTask = reaper.scheduleWithFixedDelay(this::reapIdleInstances,
                reapIntervalMillis, reapIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 为一次调用获取（或冷启动）实例，并占用一个并发槽位。
     *
     * @return true 表示本次发生了冷启动，false 表示复用了暖实例
     */
    public boolean acquire(String functionName, int version) {
        String key = key(functionName, version);
        long now = System.currentTimeMillis();
        AtomicBoolean cold = new AtomicBoolean(false);
        instances.compute(key, (k, existing) -> {
            if (existing == null) {
                cold.set(true);
                Instance created = new Instance(now);
                created.inFlight.incrementAndGet();
                return created;
            }
            existing.lastUsedMillis.set(now);
            existing.inFlight.incrementAndGet();
            return existing;
        });
        if (cold.get()) {
            coldStarts.incrementAndGet();
        } else {
            warmHits.incrementAndGet();
        }
        return cold.get();
    }

    /**
     * 调用结束后释放并发槽位，并刷新实例的最后活跃时间。
     */
    public void release(String functionName, int version) {
        String key = key(functionName, version);
        instances.computeIfPresent(key, (k, inst) -> {
            inst.lastUsedMillis.set(System.currentTimeMillis());
            inst.inFlight.decrementAndGet();
            return inst;
        });
    }

    /** 回收空闲超期且没有进行中调用的实例 */
    private void reapIdleInstances() {
        long now = System.currentTimeMillis();
        long idleTimeout = idleTimeoutMillis;
        instances.entrySet().removeIf(entry -> {
            Instance inst = entry.getValue();
            return inst.inFlight.get() == 0 && now - inst.lastUsedMillis.get() >= idleTimeout;
        });
    }

    private static String key(String functionName, int version) {
        return functionName + ":" + version;
    }

    /** 当前活跃实例数（指标用） */
    public int activeInstances() {
        return instances.size();
    }

    public long coldStartCount() {
        return coldStarts.get();
    }

    public long warmHitCount() {
        return warmHits.get();
    }

    public void setIdleTimeoutMillis(long idleTimeoutMillis) {
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    public void setReapIntervalMillis(long reapIntervalMillis) {
        this.reapIntervalMillis = reapIntervalMillis;
        scheduleReaper();
    }

    /** 关闭并停止后台缩容线程 */
    @PreDestroy
    public void shutdown() {
        reaper.shutdownNow();
    }
}
