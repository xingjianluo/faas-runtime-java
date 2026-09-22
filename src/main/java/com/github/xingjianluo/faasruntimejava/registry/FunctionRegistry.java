package com.github.xingjianluo.faasruntimejava.registry;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;
import com.github.xingjianluo.faasruntimejava.handler.FunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.instance.InstancePool;
import com.github.xingjianluo.faasruntimejava.model.FunctionConfig;
import com.github.xingjianluo.faasruntimejava.model.FunctionDefinition;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 函数版本注册中心。
 *
 * <p>一致性保证：
 * <ul>
 *   <li>每个函数一把发布锁，并发发布被串行化：版本号重复 →
 *       {@link FaasException.VersionAlreadyExistsException}；
 *       非单调递增 → {@link FaasException.InvalidVersionException}；失败原因可区分。</li>
 *   <li>发布时先通过工厂做一次“可实例化校验”，成功后版本才入表可见，
 *       不存在“版本已可见却不可用”的中间状态；解析失败不留任何痕迹。</li>
 *   <li>历史版本定义与配置不可变（{@link FunctionDefinition} 是不可变 record）；
 *       每次冷启动都由工厂按发布时固定的 handlerType 创建全新处理器，行为与发布时一致，
 *       且实例之间内存状态隔离。</li>
 * </ul>
 */
public final class FunctionRegistry {

    private static final Logger log = LoggerFactory.getLogger(FunctionRegistry.class);

    /** 单个版本的完整条目：不可变定义 + 该版本独占的实例池。 */
    public record VersionEntry(FunctionDefinition definition, InstancePool pool) {
    }

    /** 调用解析结果。 */
    public record ResolvedVersion(FunctionDefinition definition, InstancePool pool) {
    }

    private static final class FunctionState {
        final Object publishLock = new Object();
        final TreeMap<Integer, VersionEntry> versions = new TreeMap<>();
        volatile Integer currentVersion;
    }

    private final FunctionHandlerFactory factory;
    private final ConcurrentHashMap<String, FunctionState> functions = new ConcurrentHashMap<>();

    public FunctionRegistry(FunctionHandlerFactory factory) {
        this.factory = factory;
    }

    /**
     * 原子发布一个新版本。
     */
    public FunctionDefinition publish(String functionName, int version, String handlerType,
                                      Duration timeout, Integer concurrencyLimit,
                                      Duration idleTtl) {
        if (functionName == null || functionName.isBlank()) {
            throw new FaasException.InvalidInvocationException("function name must not be blank");
        }
        if (version < 1) {
            throw new FaasException.InvalidVersionException(
                    "version must be a positive integer, got " + version);
        }
        FunctionConfig config = new FunctionConfig(
                timeout == null ? FunctionConfig.DEFAULT_TIMEOUT : timeout,
                concurrencyLimit == null ? FunctionConfig.DEFAULT_CONCURRENCY : concurrencyLimit,
                idleTtl == null ? FunctionConfig.DEFAULT_IDLE_TTL : idleTtl);

        FunctionState state = functions.computeIfAbsent(functionName, k -> new FunctionState());
        synchronized (state.publishLock) {
            Integer max = state.versions.isEmpty() ? null : state.versions.lastKey();
            int expected = (max == null) ? 1 : max + 1;

            if (state.versions.containsKey(version)) {
                log.warn("PUBLISH-REJECTED reason=VERSION_ALREADY_EXISTS {}#v{}",
                        functionName, version);
                throw new FaasException.VersionAlreadyExistsException(functionName, version);
            }
            if (version != expected) {
                log.warn("PUBLISH-REJECTED reason=NON_MONOTONIC_VERSION {}#v{} expected=v{}",
                        functionName, version, expected);
                throw new FaasException.InvalidVersionException(
                        "version must be monotonically increasing: " + functionName
                                + " got v" + version + ", expected v" + expected);
            }

            // 关键：先校验处理器可解析/可实例化，成功后版本才入表可见。
            try {
                FunctionHandler probe = factory.create(handlerType);
                log.debug("PUBLISH probe ok {}#v{} handler={}", functionName, version,
                        probe.getClass().getSimpleName());
            } catch (FaasException.InvalidHandlerException e) {
                log.warn("PUBLISH-REJECTED reason=INVALID_HANDLER {}#v{} type={}",
                        functionName, version, handlerType);
                throw e;
            }

            FunctionDefinition definition = new FunctionDefinition(
                    functionName, version, handlerType, config, Instant.now());
            InstancePool pool = new InstancePool(definition.qualifiedName(),
                    () -> factory.create(handlerType), config.concurrencyLimit());
            state.versions.put(version, new VersionEntry(definition, pool));
            state.currentVersion = version;
            log.info("PUBLISHED {} handlerType={} timeoutMs={} concurrency={} idleTtlMs={} current=v{}",
                    definition.qualifiedName(), handlerType, config.timeout().toMillis(),
                    config.concurrencyLimit(), config.idleTtl().toMillis(), version);
            return definition;
        }
    }

    /**
     * 解析调用目标。version 为 null 时使用当前生效版本。
     */
    public ResolvedVersion resolve(String functionName, Integer version) {
        if (functionName == null || functionName.isBlank()) {
            throw new FaasException.InvalidInvocationException("function name must not be blank");
        }
        FunctionState state = functions.get(functionName);
        if (state == null) {
            throw new FaasException.FunctionNotFoundException(functionName);
        }
        int target;
        if (version == null) {
            Integer cur = state.currentVersion;
            if (cur == null) {
                throw new FaasException.FunctionNotFoundException(functionName);
            }
            target = cur;
        } else {
            target = version;
        }
        VersionEntry entry = state.versions.get(target);
        if (entry == null) {
            throw new FaasException.VersionNotFoundException(functionName, target);
        }
        return new ResolvedVersion(entry.definition(), entry.pool());
    }

    public Optional<FunctionDefinition> currentDefinition(String functionName) {
        FunctionState state = functions.get(functionName);
        if (state == null || state.currentVersion == null) {
            return Optional.empty();
        }
        return Optional.of(state.versions.get(state.currentVersion).definition());
    }

    public List<FunctionDefinition> versions(String functionName) {
        FunctionState state = functions.get(functionName);
        if (state == null) {
            return List.of();
        }
        synchronized (state.publishLock) {
            List<FunctionDefinition> out = new ArrayList<>();
            state.versions.values().forEach(e -> out.add(e.definition()));
            return List.copyOf(out);
        }
    }

    /** 供缩容器遍历：快照所有函数所有版本条目。 */
    public List<VersionEntry> allEntries() {
        List<VersionEntry> out = new ArrayList<>();
        functions.forEach((name, state) -> {
            synchronized (state.publishLock) {
                out.addAll(state.versions.values());
            }
        });
        return List.copyOf(out);
    }
}
