package com.github.xingjianluo.faasruntimejava.instance;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单个函数版本独占的实例池。
 *
 * <p>并发模型（保证“缩容与并发调用同时发生时不丢失、不重复执行”）：
 * <ul>
 *   <li>{@code maxInstances == 函数并发上限}：每获取一个租约就占用一个实例槽位，
 *       槽位满则 {@link #tryAcquire()} 返回 empty，由上层映射为 CONCURRENCY_REJECTED；</li>
 *   <li>获取（取空闲实例 / 冷启动新建）与回收（缩容摘除空闲实例）在同一把池锁内完成并互斥，
 *       缩容只会摘除确定空闲的实例，in-flight 实例绝不会被回收；</li>
 *   <li>租约 {@link Lease#release()} 把实例放回空闲队列，实例不会被重复分发；</li>
 *   <li>每次冷启动通过 supplier 构造全新的处理器，实例间内存状态完全隔离；
 *       超时实例 {@link Lease#discard()} 后即使旧线程仍在运行也不会污染新实例。</li>
 * </ul>
 */
public final class InstancePool {

    private static final Logger log = LoggerFactory.getLogger(InstancePool.class);

    private final String qualifier;
    private final Supplier<? extends FunctionHandler> handlerSupplier;
    private final int maxInstances;
    private final Object lock = new Object();

    private final Deque<FunctionInstance> idle = new ArrayDeque<>();
    private int created;
    private int inflightCount;

    public InstancePool(String qualifier, Supplier<? extends FunctionHandler> handlerSupplier,
                        int maxInstances) {
        if (maxInstances < 1) {
            throw new IllegalArgumentException("maxInstances must be >= 1");
        }
        this.qualifier = qualifier;
        this.handlerSupplier = handlerSupplier;
        this.maxInstances = maxInstances;
    }

    /** 一次实例租约：调用结束必须 {@link #release()}，超时等不可复用场景须 {@link #discard()}。 */
    public final class Lease {
        private FunctionInstance instance;
        private final boolean coldStart;
        private volatile boolean closed;

        Lease(FunctionInstance instance, boolean coldStart) {
            this.instance = instance;
            this.coldStart = coldStart;
        }

        public FunctionInstance instance() {
            return instance;
        }

        public boolean coldStart() {
            return coldStart;
        }

        /** 正常归还：实例进入空闲队列供后续调用复用。 */
        public void release() {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                inflightCount--;
                instance.touch();
                idle.addLast(instance);
            }
            log.debug("[{}] release instance={} inflight={}", qualifier, instance.id(), inflightCount);
        }

        /**
         * 销毁租约：调用超时后工作线程可能仍在使用该处理器，实例不能回到空闲队列，
         * 必须摘除槽位，后续冷启动会经 supplier 得到全新处理器。
         */
        public void discard() {
            String id;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                inflightCount--;
                created--;
                id = instance.id();
                instance = null;
            }
            log.warn("[{}] discard instance={} after unrecoverable call remain={}",
                    qualifier, id, created);
        }
    }

    /**
     * 非阻塞获取一个实例。
     *
     * @return 空闲复用或冷启动的租约；槽位已满时返回 empty（超出并发上限）
     */
    public Optional<Lease> tryAcquire() {
        FunctionInstance inst;
        boolean cold;
        synchronized (lock) {
            FunctionInstance existing = idle.pollFirst();
            if (existing != null) {
                inst = existing;
                cold = false;
            } else if (created < maxInstances) {
                // 冷启动：全新处理器实例，状态与历史实例隔离。
                inst = new FunctionInstance(qualifier, handlerSupplier.get(), Instant.now());
                created++;
                cold = true;
                log.info("[{}] COLD-START instance={} created={}/{}",
                        qualifier, inst.id(), created, maxInstances);
            } else {
                log.warn("[{}] CONCURRENCY-REJECTED inflight={} limit={}",
                        qualifier, inflightCount, maxInstances);
                return Optional.empty();
            }
            inflightCount++;
        }
        return Optional.of(new Lease(inst, cold));
    }

    /**
     * 回收空闲时间超过 ttl 的实例（缩容，可缩到 0）。只摘除空闲队列中的实例，
     * 与 {@link #tryAcquire()} 互斥，因此不会回收正在执行调用的实例。
     *
     * @return 被回收的实例数
     */
    public int evictIdle(Duration idleTtl, long nowMs) {
        int evicted = 0;
        synchronized (lock) {
            long ttlMs = idleTtl.toMillis();
            var it = idle.iterator();
            while (it.hasNext()) {
                FunctionInstance candidate = it.next();
                long idleMs = nowMs - candidate.lastUsedAt().toEpochMilli();
                if (idleMs >= ttlMs) {
                    it.remove();
                    created--;
                    evicted++;
                    log.info("[{}] SCALE-IN evict instance={} idleMs={} remain={}",
                            qualifier, candidate.id(), idleMs, created);
                }
            }
        }
        return evicted;
    }

    public int createdCount() {
        synchronized (lock) {
            return created;
        }
    }

    public int idleCount() {
        synchronized (lock) {
            return idle.size();
        }
    }

    public int inflight() {
        synchronized (lock) {
            return inflightCount;
        }
    }

    public int maxInstances() {
        return maxInstances;
    }
}
