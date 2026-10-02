package com.github.xingjianluo.faasruntimejava.faas;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实例管理：按 (函数, 版本) 维护暖实例池，支持多实例并存、实例级私有状态、
 * 故障淘汰与空闲缩容。
 *
 * <p>每个版本持有一个 {@link VersionPool}，池内所有挑选、冷启动、槽位计数、
 * 淘汰动作都在池对象的同一把锁上串行完成，因此：
 * <ul>
 *   <li>同一个调用只会被分配给唯一一个实例，不可能两个实例同时跑同一个调用；</li>
 *   <li>故障实例在调用结束的释放动作中原子摘除，摘除之后的新调用要么落到别的
 *       健康暖实例、要么冷启动新实例，绝不会再被分配到已淘汰实例；</li>
 *   <li>实例数永远不超过该版本配置的 maxWarmInstances，反复故障也不会无限增长；</li>
 *   <li>池空且无实例可复用时才冷启动，冷启动与并发槽位占用在同一临界区内，
 *       不丢调用、不重复计数；到顶则返回拒绝结果，绝不排队。</li>
 * </ul>
 */
@Component
public class InstanceManager {

    private static final Logger log = LoggerFactory.getLogger(InstanceManager.class);

    /** 一次成功的实例占用：携带实例 ID、该实例的私有状态与冷启动标记 */
    public record Lease(String instanceId, Object state, boolean coldStart) {
    }

    /** 获取实例的结果：要么拿到租约，要么被容量拒绝（携带拒绝原因） */
    public static final class AcquireResult {
        private final Lease lease;
        private final String rejectReason;

        private AcquireResult(Lease lease, String rejectReason) {
            this.lease = lease;
            this.rejectReason = rejectReason;
        }

        public static AcquireResult granted(Lease lease) {
            return new AcquireResult(lease, null);
        }

        public static AcquireResult denied(String reason) {
            return new AcquireResult(null, reason);
        }

        public boolean granted() {
            return lease != null;
        }

        public Lease lease() {
            if (lease == null) {
                throw new IllegalStateException("acquire was denied: " + rejectReason);
            }
            return lease;
        }

        public String rejectReason() {
            return rejectReason;
        }
    }

    /** 单个暖实例的账本：state 为实例私有，其他实例不可见 */
    private static final class Instance {
        final String id;
        final Object state;
        int inFlight;
        long lastUsedMillis;
        boolean healthy = true;

        Instance(String id, Object state, long now) {
            this.id = id;
            this.state = state;
            this.lastUsedMillis = now;
        }
    }

    /** 单个 (函数, 版本) 的暖实例池；所有变更都在 this 锁上完成 */
    private static final class VersionPool {
        private final Set<Instance> instances = new LinkedHashSet<>();

        private synchronized AcquireResult acquire(int maxWarmInstances, int maxConcurrencyPerInstance,
                                                   String functionName, int version,
                                                   FunctionHandler handler, InstanceManager owner) {
            long now = System.currentTimeMillis();
            // 1. 优先复用健康且有空闲槽位的暖实例（按创建顺序取第一个）
            Instance chosen = null;
            boolean cold = false;
            for (Instance inst : instances) {
                if (inst.healthy && inst.inFlight < maxConcurrencyPerInstance) {
                    chosen = inst;
                    break;
                }
            }
            // 2. 没有可复用实例时，未到上限则冷启动一个新实例；到顶则立即拒绝
            if (chosen == null) {
                if (instances.size() >= maxWarmInstances) {
                    return AcquireResult.denied("all " + maxWarmInstances
                            + " warm instance(s) of '" + functionName + "' v" + version
                            + " are at their concurrency limit (" + maxConcurrencyPerInstance
                            + " per instance)");
                }
                Object state = handler instanceof StatefulFunctionHandler sfh
                        ? sfh.initInstanceState() : null;
                chosen = new Instance(owner.newInstanceId(functionName, version), state, now);
                instances.add(chosen);
                cold = true;
                log.info("[实例池] {}.v{} 冷启动新实例 instanceId={}（池内实例数={}/{}）",
                        functionName, version, chosen.id, instances.size(), maxWarmInstances);
            }
            chosen.inFlight++;
            chosen.lastUsedMillis = now;
            Lease lease = new Lease(chosen.id, chosen.state, cold);
            if (cold) {
                owner.coldStarts.incrementAndGet();
            } else {
                owner.warmHits.incrementAndGet();
                log.info("[实例池] {}.v{} 复用暖实例 instanceId={}（在飞调用={}/{}）",
                        functionName, version, chosen.id, chosen.inFlight, maxConcurrencyPerInstance);
            }
            return AcquireResult.granted(lease);
        }

        /** 释放槽位；faulty=true 时该实例被原子淘汰，之后永不复用 */
        private synchronized void release(String instanceId, boolean faulty,
                                          String functionName, int version, InstanceManager owner) {
            Instance inst = null;
            for (Instance candidate : instances) {
                if (candidate.id.equals(instanceId)) {
                    inst = candidate;
                    break;
                }
            }
            if (inst == null) {
                return;
            }
            inst.inFlight--;
            inst.lastUsedMillis = System.currentTimeMillis();
            if (faulty) {
                inst.healthy = false;
                instances.remove(inst);
                owner.evictions.incrementAndGet();
                log.warn("[实例池] {}.v{} 淘汰故障实例 instanceId={}（剩余健康实例={}）",
                        functionName, version, instanceId, instances.size());
            }
        }

        /** 回收空闲超期且无在飞调用的实例，返回被回收数量 */
        private synchronized int reap(long now, long idleTimeout, String functionName, int version) {
            int removed = 0;
            var it = instances.iterator();
            while (it.hasNext()) {
                Instance inst = it.next();
                if (inst.inFlight == 0 && now - inst.lastUsedMillis >= idleTimeout) {
                    it.remove();
                    removed++;
                    log.info("[实例池] {}.v{} 空闲缩容移除实例 instanceId={}",
                            functionName, version, inst.id);
                }
            }
            return removed;
        }

        private synchronized int size() {
            return instances.size();
        }

        private synchronized List<String> instanceIds() {
            List<String> ids = new ArrayList<>(instances.size());
            for (Instance inst : instances) {
                ids.add(inst.id);
            }
            return ids;
        }
    }

    private final Map<String, VersionPool> pools = new ConcurrentHashMap<>();
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
    private final AtomicLong instanceSequence = new AtomicLong(0);

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
     * 为一次调用获取实例：优先复用健康暖实例，否则在容量内冷启动，到顶立即拒绝。
     *
     * @param configuredCapacity           发布时配置的版本容量；null 表示老函数默认行为
     *                                     （单实例 + 全局函数级并发上限）
     * @param defaultConcurrencyPerInstance 老函数默认的单实例并发上限
     */
    public AcquireResult acquire(String functionName, int version,
                                 CapacityConfig configuredCapacity,
                                 int defaultConcurrencyPerInstance,
                                 FunctionHandler handler) {
        int maxWarm = configuredCapacity != null ? configuredCapacity.maxWarmInstances() : 1;
        int perInstance = configuredCapacity != null
                ? configuredCapacity.maxConcurrencyPerInstance() : defaultConcurrencyPerInstance;
        VersionPool pool = pools.computeIfAbsent(key(functionName, version), k -> new VersionPool());
        return pool.acquire(maxWarm, perInstance, functionName, version, handler, this);
    }

    /**
     * 调用结束后释放槽位；实例超时或执行出错时 faulty=true，实例被立即淘汰且不再复用。
     */
    public void release(String functionName, int version, String instanceId, boolean faulty) {
        VersionPool pool = pools.get(key(functionName, version));
        if (pool != null) {
            pool.release(instanceId, faulty, functionName, version, this);
        }
    }

    /** 回收各版本池中空闲超期且没有进行中调用的实例 */
    private void reapIdleInstances() {
        long now = System.currentTimeMillis();
        long idleTimeout = idleTimeoutMillis;
        for (Map.Entry<String, VersionPool> entry : pools.entrySet()) {
            String[] parts = splitKey(entry.getKey());
            entry.getValue().reap(now, idleTimeout, parts[0], Integer.parseInt(parts[1]));
        }
    }

    String newInstanceId(String functionName, int version) {
        return functionName + ":" + version + "#inst-" + instanceSequence.incrementAndGet();
    }

    private static String key(String functionName, int version) {
        return functionName + ":" + version;
    }

    private static String[] splitKey(String key) {
        int idx = key.lastIndexOf(':');
        return new String[]{key.substring(0, idx), key.substring(idx + 1)};
    }

    /** 当前活跃实例总数（指标用） */
    public int activeInstances() {
        int total = 0;
        for (VersionPool pool : pools.values()) {
            total += pool.size();
        }
        return total;
    }

    /** 各版本池当前的实例 ID 快照（调试/指标用） */
    public Map<String, List<String>> activeInstanceDetails() {
        Map<String, List<String>> details = new LinkedHashMap<>();
        for (Map.Entry<String, VersionPool> entry : pools.entrySet()) {
            List<String> ids = entry.getValue().instanceIds();
            if (!ids.isEmpty()) {
                details.put(entry.getKey(), ids);
            }
        }
        return details;
    }

    public long coldStartCount() {
        return coldStarts.get();
    }

    public long warmHitCount() {
        return warmHits.get();
    }

    public long evictionCount() {
        return evictions.get();
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
