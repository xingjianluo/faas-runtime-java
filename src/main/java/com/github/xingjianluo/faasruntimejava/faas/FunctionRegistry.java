package com.github.xingjianluo.faasruntimejava.faas;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 函数注册表：管理函数的多版本。
 *
 * <p>一致性约定：
 * <ul>
 *   <li>历史版本不可覆盖：同一 (函数, 版本) 只允许发布一次，重复发布返回
 *       {@link PublishStatus#DUPLICATE_VERSION}；</li>
 *   <li>并发发布互斥：同一函数的发布串行化，抢不到发布锁的调用立即以
 *       {@link PublishStatus#CONCURRENT_MODIFICATION} 拒绝，可安全重试；</li>
 *   <li>无中间态：版本号与处理器通过单次原子写入同时对外可见，
 *       不存在“版本可见但处理器缺失”的状态。</li>
 * </ul>
 */
@Component
public class FunctionRegistry {

    /** 一次发布的登记内容：处理器与该版本的容量配置（容量配置为 null 表示沿用老行为） */
    public record Registration(FunctionHandler handler, CapacityConfig capacity) {
    }

    /** 每个函数的版本表：版本号 -> 登记内容，条目一经写入不可变 */
    private final ConcurrentMap<String, ConcurrentMap<Integer, Registration>> functions =
            new ConcurrentHashMap<>();
    /** 每个函数的当前生效版本（已发布的最大版本号） */
    private final ConcurrentMap<String, AtomicInteger> currentVersions = new ConcurrentHashMap<>();
    /** 每个函数的发布锁，用于串行化发布并识别并发竞争 */
    private final ConcurrentMap<String, ReentrantLock> publishLocks = new ConcurrentHashMap<>();

    /**
     * 发布函数的一个新版本（不配置容量，沿用老行为）。
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler) {
        return publish(functionName, version, handler, null);
    }

    /**
     * 发布函数的一个新版本，并显式配置该版本的暖实例容量。
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler,
                                 CapacityConfig capacity) {
        if (functionName == null || functionName.isBlank()) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "functionName must not be blank");
        }
        if (version <= 0) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "version must be a positive integer");
        }
        if (handler == null) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "handler must not be null");
        }
        if (capacity != null && (capacity.maxWarmInstances() <= 0
                || capacity.maxConcurrencyPerInstance() <= 0)) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "capacity limits must be positive integers: " + capacity);
        }

        ReentrantLock lock = publishLocks.computeIfAbsent(functionName, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            // 并发发布竞争：明确拒绝，调用方可重试
            return PublishResult.rejected(PublishStatus.CONCURRENT_MODIFICATION, functionName, version,
                    "concurrent publish in progress for function '" + functionName + "', retry later");
        }
        try {
            ConcurrentMap<Integer, Registration> versions =
                    functions.computeIfAbsent(functionName, k -> new ConcurrentHashMap<>());
            // 原子写入：版本号、处理器、容量同时可见，且已存在时不会覆盖历史版本
            Registration previous = versions.putIfAbsent(
                    version, new Registration(handler, capacity));
            if (previous != null) {
                return PublishResult.rejected(PublishStatus.DUPLICATE_VERSION, functionName, version,
                        "version " + version + " of function '" + functionName + "' already exists");
            }
            currentVersions.computeIfAbsent(functionName, k -> new AtomicInteger(0))
                    .accumulateAndGet(version, Math::max);
            return PublishResult.published(functionName, version);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查询指定版本的登记内容（处理器 + 容量配置）；version <= 0 表示当前生效版本。
     */
    public Optional<Registration> lookupRegistration(String functionName, int version) {
        return resolveVersion(functionName, version)
                .flatMap(v -> Optional.ofNullable(functions.get(functionName))
                        .map(versions -> versions.get(v)));
    }

    /**
     * 查询指定版本的处理器；version <= 0 表示当前生效版本（已发布的最大版本号）。
     */
    public Optional<FunctionHandler> lookup(String functionName, int version) {
        return lookupRegistration(functionName, version).map(Registration::handler);
    }

    /**
     * 解析实际版本号：version <= 0 时返回当前生效版本；显式版本需已存在。
     */
    public Optional<Integer> resolveVersion(String functionName, int version) {
        if (functionName == null) {
            return Optional.empty();
        }
        if (version <= 0) {
            AtomicInteger current = currentVersions.get(functionName);
            return current == null || current.get() <= 0 ? Optional.empty() : Optional.of(current.get());
        }
        ConcurrentMap<Integer, Registration> versions = functions.get(functionName);
        return versions != null && versions.containsKey(version) ? Optional.of(version) : Optional.empty();
    }

    public boolean containsFunction(String functionName) {
        return functionName != null && functions.containsKey(functionName);
    }
}
