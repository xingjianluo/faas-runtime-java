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
 *   <li>无中间态：版本号与不可变的 {@link FunctionDefinition}（处理器 + 容量配置）
 *       通过单次原子写入同时对外可见；</li>
 *   <li>定义不可变：新版本发布不触碰老版本条目，老版本已在运行的实例继续按
 *       发布时的处理器与容量提供服务。</li>
 * </ul>
 */
@Component
public class FunctionRegistry {

    /** 每个函数的版本表：版本号 -> 不可变版本定义，条目一经写入不可变 */
    private final ConcurrentMap<String, ConcurrentMap<Integer, FunctionDefinition>> functions =
            new ConcurrentHashMap<>();
    /** 每个函数的当前生效版本（已发布的最大版本号） */
    private final ConcurrentMap<String, AtomicInteger> currentVersions = new ConcurrentHashMap<>();
    /** 每个函数的发布锁，用于串行化发布并识别并发竞争 */
    private final ConcurrentMap<String, ReentrantLock> publishLocks = new ConcurrentHashMap<>();

    /**
     * 老发布方式：共享处理器 + 遗留容量（单实例、函数级并发上限），
     * 未显式配置容量的老函数行为与旧版完全一致。
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler) {
        if (handler == null) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "handler must not be null");
        }
        PublishResult invalid = validateCommon(functionName, version);
        if (invalid != null) {
            return invalid;
        }
        FunctionDefinition definition = new FunctionDefinition(functionName, version,
                handler, null, Capacity.legacy(), true);
        return doPublish(functionName, version, definition);
    }

    /**
     * 发布无状态函数的新版本：所有实例共享同一处理器，按版本显式配置容量。
     *
     * @param maxWarmInstances            该版本最多保留的暖实例数，null 取默认值
     * @param maxConcurrencyPerInstance   单实例允许的并发调用数，null 取默认值
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler,
                                 Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
        if (handler == null) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "handler must not be null");
        }
        PublishResult invalid = validateCommon(functionName, version);
        if (invalid != null) {
            return invalid;
        }
        PublishResult invalidCapacity = validateCapacity(functionName, version,
                maxWarmInstances, maxConcurrencyPerInstance);
        if (invalidCapacity != null) {
            return invalidCapacity;
        }
        FunctionDefinition definition = new FunctionDefinition(functionName, version,
                handler, null,
                Capacity.configured(maxWarmInstances, maxConcurrencyPerInstance), false);
        return doPublish(functionName, version, definition);
    }

    /**
     * 发布有状态函数的新版本：每个实例通过工厂独立创建处理器，
     * 实例级状态天然隔离；按版本显式配置容量。
     *
     * @param maxWarmInstances            该版本最多保留的暖实例数，null 取默认值
     * @param maxConcurrencyPerInstance   单实例允许的并发调用数，null 取默认值
     */
    public PublishResult publish(String functionName, int version, FunctionHandlerFactory factory,
                                 Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
        if (factory == null) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "handler factory must not be null");
        }
        PublishResult invalid = validateCommon(functionName, version);
        if (invalid != null) {
            return invalid;
        }
        PublishResult invalidCapacity = validateCapacity(functionName, version,
                maxWarmInstances, maxConcurrencyPerInstance);
        if (invalidCapacity != null) {
            return invalidCapacity;
        }
        FunctionDefinition definition = new FunctionDefinition(functionName, version,
                null, factory,
                Capacity.configured(maxWarmInstances, maxConcurrencyPerInstance), false);
        return doPublish(functionName, version, definition);
    }

    private static PublishResult validateCommon(String functionName, int version) {
        if (functionName == null || functionName.isBlank()) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "functionName must not be blank");
        }
        if (version <= 0) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "version must be a positive integer");
        }
        return null;
    }

    private static PublishResult validateCapacity(String functionName, int version,
                                                  Integer maxWarmInstances,
                                                  Integer maxConcurrencyPerInstance) {
        if (maxWarmInstances != null && maxWarmInstances <= 0) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "maxWarmInstances must be a positive integer");
        }
        if (maxConcurrencyPerInstance != null && maxConcurrencyPerInstance <= 0) {
            return PublishResult.rejected(PublishStatus.INVALID_ARGUMENT, functionName, version,
                    "maxConcurrencyPerInstance must be a positive integer");
        }
        return null;
    }

    private PublishResult doPublish(String functionName, int version, FunctionDefinition definition) {
        ReentrantLock lock = publishLocks.computeIfAbsent(functionName, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            // 并发发布竞争：明确拒绝，调用方可重试
            return PublishResult.rejected(PublishStatus.CONCURRENT_MODIFICATION, functionName, version,
                    "concurrent publish in progress for function '" + functionName + "', retry later");
        }
        try {
            ConcurrentMap<Integer, FunctionDefinition> versions =
                    functions.computeIfAbsent(functionName, k -> new ConcurrentHashMap<>());
            // 原子写入：版本号与定义同时可见，且已存在时不会覆盖历史版本
            FunctionDefinition previous = versions.putIfAbsent(version, definition);
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
     * 解析指定版本的定义；version <= 0 表示当前生效版本（已发布的最大版本号）。
     */
    public Optional<FunctionDefinition> resolve(String functionName, int version) {
        return resolveVersion(functionName, version)
                .flatMap(v -> Optional.ofNullable(functions.get(functionName))
                        .map(versions -> versions.get(v)));
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
        ConcurrentMap<Integer, FunctionDefinition> versions = functions.get(functionName);
        return versions != null && versions.containsKey(version) ? Optional.of(version) : Optional.empty();
    }

    public boolean containsFunction(String functionName) {
        return functionName != null && functions.containsKey(functionName);
    }
}
