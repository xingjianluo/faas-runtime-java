package com.github.xingjianluo.faasruntimejava.scaling;

import com.github.xingjianluo.faasruntimejava.registry.FunctionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 周期性回收空闲实例（缩容到 0）。
 *
 * <p>安全性：回收动作与实例获取共用池锁且只摘除“确定空闲”的实例，
 * 因此与并发调用竞争时，正在执行调用的实例绝不会被回收，调用不丢失、不重复。
 * 也可通过 {@link #reapOnce()} 在测试中手动触发，保证状态可复现。
 */
public final class IdleReaper {

    private static final Logger log = LoggerFactory.getLogger(IdleReaper.class);

    private final FunctionRegistry registry;

    public IdleReaper(FunctionRegistry registry) {
        this.registry = registry;
    }

    /** 执行一轮回收，返回被回收的实例总数。 */
    @Scheduled(fixedDelayString = "${faas.reaper-interval-ms:5000}")
    public int reapOnce() {
        long now = System.currentTimeMillis();
        int total = 0;
        for (FunctionRegistry.VersionEntry entry : registry.allEntries()) {
            int before = entry.pool().createdCount();
            int evicted = entry.pool().evictIdle(
                    entry.definition().config().idleTtl(), now);
            total += evicted;
            if (evicted > 0) {
                log.info("REAP {} evicted={} instances {}->{}",
                        entry.definition().qualifiedName(), evicted, before,
                        entry.pool().createdCount());
            }
        }
        return total;
    }
}
