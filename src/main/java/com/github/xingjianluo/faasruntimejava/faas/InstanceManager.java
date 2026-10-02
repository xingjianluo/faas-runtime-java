package com.github.xingjianluo.faasruntimejava.faas;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实例管理：按 (函数, 版本) 维护暖实例池，支持多实例、实例内并发上限、
 * 空闲缩容与故障实例淘汰。
 *
 * <p>每个 {@link Instance} 持有：
 * <ul>
 *   <li>仅属于自己的 {@link FunctionHandler}（由 {@link FunctionHandlerFactory}
 *       在冷启动时创建）——处理器内部字段即实例级状态，实例之间互不可见；</li>
 *   <li>独立的实例内并发信号量；</li>
 *   <li>稳定的实例 ID，用于结果与日志中识别处理者。</li>
 * </ul>
 *
 * <p>容量与淘汰约定：
 * <ul>
 *   <li>获取顺序：先尝试复用有空槽的健康暖实例；都忙且健康实例数小于
 *       {@code maxWarmInstances} 时冷启动一个新实例；否则立即拒绝
 *       {@link InvocationStatus#INSTANCE_CAPACITY_EXCEEDED}，不排队；</li>
 *   <li>超时或执行出错的实例在该次调用释放时立即从健康集合淘汰，
 *       之后的调用只会落到其他健康实例或触发新冷启动；</li>
 *   <li>被淘汰时若仍有在飞调用，实例进入 draining 集合，调用全部结束后自然回收；
 *       draining 集合只可能由“正在执行的调用”撑出，因此反复出问题也不会
 *       让实例总数无限上涨；</li>
 *   <li>获取与淘汰、缩容都在同一实例池的锁内完成：调用要么拿到某一个实例、
 *       要么被明确拒绝，同一个调用不可能被两个实例同时执行。</li>
 * </ul>
 */
@Component
public class InstanceManager {

    /** 一个正在运行的函数实例 */
    static final class Instance {
        private final String id;
        private final FunctionHandler handler;
        private final Semaphore slots;
        private final int capacitySlots;
        private volatile long lastUsedMillis;

        Instance(FunctionHandler handler, int maxConcurrency, long now) {
            this.id = UUID.randomUUID().toString().substring(0, 8);
            this.handler = handler;
            this.capacitySlots = maxConcurrency;
            // 遗留容量的 perInstance 为 MAX_VALUE，信号量实际不起约束作用
            this.slots = new Semaphore(maxConcurrency);
            this.lastUsedMillis = now;
        }

        boolean idleAt(long now, long idleTimeoutMillis) {
            return slots.availablePermits() == capacitySlots
                    && now - lastUsedMillis >= idleTimeoutMillis;
        }
    }

    /** 单次获取实例的结果（获取与判定在池锁内一次性完成） */
    static final class AcquireResult {
        enum Kind { ACQUIRED, REJECTED, CREATE_FAILED }

        final Kind kind;
        final Lease lease;
        final Throwable cause;

        private AcquireResult(Kind kind, Lease lease, Throwable cause) {
            this.kind = kind;
            this.lease = lease;
            this.cause = cause;
        }

        static AcquireResult acquired(String poolKey, Instance instance, boolean coldStart) {
            return new AcquireResult(Kind.ACQUIRED, new Lease(poolKey, instance, coldStart), null);
        }

        static AcquireResult rejected() {
            return new AcquireResult(Kind.REJECTED, null, null);
        }

        static AcquireResult createFailed(Throwable cause) {
            return new AcquireResult(Kind.CREATE_FAILED, null, cause);
        }
    }

    /** 调用方持有到调用结束的租约，释放时告知实例是否仍然健康 */
    static final class Lease {
        final String poolKey;
        final Instance instance;
        final boolean coldStart;
        /** 释放是否已完成，防止重复释放 */
        private volatile boolean released;

        Lease(String poolKey, Instance instance, boolean coldStart) {
            this.poolKey = poolKey;
            this.instance = instance;
            this.coldStart = coldStart;
        }

        public String instanceId() {
            return instance.id;
        }

        public boolean coldStart() {
            return coldStart;
        }

        FunctionHandler handler() {
            return instance.handler;
        }
    }

    /** 单个 (函数, 版本) 的暖实例池 */
    private static final class Pool {
        final Capacity capacity;
        /** 健康可复用实例集合；实例池所有结构性变更都在 lock 内完成 */
        final List<Instance> healthy = new ArrayList<>();

        Pool(Capacity capacity) {
            this.capacity = capacity;
        }
    }

    private final ConcurrentHashMap<String, Pool> pools = new ConcurrentHashMap<>();
    /** 已从健康集合淘汰、但仍有在飞调用的实例；调用排空后移除，仅用于计数与诊断 */
    private final ConcurrentHashMap<String, Instance> draining = new ConcurrentHashMap<>();
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
    private final AtomicLong evictions = new AtomicLong(0);
    private final AtomicLong capacityRejections = new AtomicLong(0);

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

    private static String key(String functionName, int version) {
        return functionName + ":" + version;
    }

    private Pool poolOf(FunctionDefinition definition) {
        return pools.computeIfAbsent(key(definition.functionName(), definition.version()),
                k -> new Pool(definition.capacity()));
    }

    /**
     * 为一次调用获取实例，判定过程在池锁内原子完成：
     * 复用暖实例 / 冷启动 / 容量到顶拒绝，三者互斥且无中间态。
     */
    AcquireResult acquire(FunctionDefinition definition) {
        String poolKey = key(definition.functionName(), definition.version());
        Pool pool = poolOf(definition);
        long now = System.currentTimeMillis();
        synchronized (pool) {
            // 1. 优先复用有空槽的健康实例（同版本多实例间负载分摊）
            for (Instance inst : pool.healthy) {
                if (inst.slots.tryAcquire()) {
                    inst.lastUsedMillis = now;
                    warmHits.incrementAndGet();
                    return AcquireResult.acquired(poolKey, inst, false);
                }
            }
            // 2. 健康实例都忙且未到上限：冷启动一个相互独立的新实例
            if (pool.healthy.size() < pool.capacity.maxWarmInstances()) {
                FunctionHandler handler;
                try {
                    handler = definition.newInstanceHandler();
                } catch (Throwable t) {
                    // 冷启动创建失败：不向池中加入任何实例，归类为 HANDLER_ERROR
                    return AcquireResult.createFailed(t);
                }
                Instance created = new Instance(handler,
                        pool.capacity.maxConcurrencyPerInstance(), now);
                created.slots.acquireUninterruptibly();
                pool.healthy.add(created);
                coldStarts.incrementAndGet();
                return AcquireResult.acquired(poolKey, created, true);
            }
            // 3. 到顶：立即拒绝，不排队、不产生副作用
            capacityRejections.incrementAndGet();
            return AcquireResult.rejected();
        }
    }

    /**
     * 调用结束释放租约。
     *
     * @param healthy true 表示本次调用正常完成，实例可继续复用；
     *                false 表示超时或执行出错，实例必须立即淘汰，不再接后续调用
     */
    void release(Lease lease, boolean healthy) {
        if (lease.released) {
            return;
        }
        lease.released = true;
        Instance inst = lease.instance;
        Pool pool = pools.get(lease.poolKey);
        boolean evictedNow = false;
        boolean stillInFlight;
        if (pool != null) {
            synchronized (pool) {
                inst.lastUsedMillis = System.currentTimeMillis();
                inst.slots.release();
                stillInFlight = inst.slots.availablePermits() < inst.capacitySlots;
                if (!healthy) {
                    // 故障实例：立即从健康集合摘除，后续调用永远不会再选中它
                    if (pool.healthy.remove(inst)) {
                        evictions.incrementAndGet();
                        evictedNow = true;
                    }
                }
            }
        } else {
            inst.slots.release();
            stillInFlight = inst.slots.availablePermits() < inst.capacitySlots;
        }
        if (evictedNow) {
            if (stillInFlight) {
                // 还有别的在飞调用：纳入 draining 计数，全部结束后移除。
                // draining 只可能由“正在执行的调用”撑出，反复淘汰也不会让实例数无限增长
                draining.put(inst.id, inst);
            }
        } else if (draining.containsKey(inst.id) && !stillInFlight) {
            // 在已淘汰实例上执行的最后一个调用也结束了，实例自然回收
            draining.remove(inst.id);
        }
    }

    /** 回收空闲超期且没有进行中调用的健康实例（缩容到零） */
    private void reapIdleInstances() {
        long now = System.currentTimeMillis();
        long idleTimeout = idleTimeoutMillis;
        for (Map.Entry<String, Pool> entry : pools.entrySet()) {
            Pool pool = entry.getValue();
            synchronized (pool) {
                pool.healthy.removeIf(inst -> inst.idleAt(now, idleTimeout));
            }
        }
    }

    /** 当前健康暖实例总数（指标用） */
    public int activeInstances() {
        int total = 0;
        for (Pool pool : pools.values()) {
            synchronized (pool) {
                total += pool.healthy.size();
            }
        }
        return total;
    }

    /** 当前已淘汰但仍有在飞调用的实例数 */
    public int drainingInstances() {
        return draining.size();
    }

    public long coldStartCount() {
        return coldStarts.get();
    }

    public long warmHitCount() {
        return warmHits.get();
    }

    /** 累计被淘汰的故障实例数 */
    public long evictedCount() {
        return evictions.get();
    }

    /** 累计因版本容量到顶被拒绝的次数 */
    public long capacityRejectionCount() {
        return capacityRejections.get();
    }

    /** 每个 (函数, 版本) 池的实例明细，指标与本地验证用 */
    public Map<String, Integer> poolSizes() {
        Map<String, Integer> sizes = new LinkedHashMap<>();
        for (Map.Entry<String, Pool> entry : pools.entrySet()) {
            synchronized (entry.getValue()) {
                sizes.put(entry.getKey(), entry.getValue().healthy.size());
            }
        }
        return sizes;
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
