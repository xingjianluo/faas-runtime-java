package com.github.xingjianluo.faasruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.handler.DefaultFunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.model.FunctionDefinition;
import com.github.xingjianluo.faasruntimejava.runtime.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.runtime.RuntimeProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景一：并发发布。
 * 断言：同一版本并发发布只有一个成功，其余得到可区分的 VERSION_ALREADY_EXISTS；
 * 版本严格单调；历史版本发布后立即可调用（不存在“可见不可用”中间态）；
 * 未知 handlerType 发布被拒绝且不留痕。
 */
class ConcurrentPublishTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPublishTest.class);

    private FaasRuntime newRuntime() {
        return new FaasRuntime(new DefaultFunctionHandlerFactory(), new RuntimeProperties());
    }

    @Test
    void concurrentPublishOfSameVersion_singleSuccess_othersRejectedDistinguishably()
            throws Exception {
        FaasRuntime runtime = newRuntime();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<String>> futures = new ArrayList<>();
            // 所有线程同时发布 fn-a 的 v1
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    try {
                        FunctionDefinition def = runtime.publish("fn-a", 1, "echo",
                                Duration.ofSeconds(2), 4, Duration.ofSeconds(30));
                        log.info("publisher#{} -> PUBLISHED {} thread={}", idx,
                                def.qualifiedName(), Thread.currentThread().getName());
                        return "PUBLISHED:" + def.qualifiedName();
                    } catch (FaasException.VersionAlreadyExistsException e) {
                        log.info("publisher#{} -> REJECTED reason=VERSION_ALREADY_EXISTS fn-a#v1 "
                                + "thread={} basis={}", idx, Thread.currentThread().getName(),
                                e.getMessage());
                        return "REJECTED:VERSION_ALREADY_EXISTS";
                    }
                }));
            }

            int published = 0;
            int rejected = 0;
            for (Future<String> f : futures) {
                String outcome = f.get(10, TimeUnit.SECONDS);
                if (outcome.startsWith("PUBLISHED")) {
                    published++;
                } else if (outcome.equals("REJECTED:VERSION_ALREADY_EXISTS")) {
                    rejected++;
                }
            }
            log.info("concurrent publish v1 summary: published={} rejected={} (basis: per-function "
                    + "publish lock serializes, first wins, others see existing key)",
                    published, rejected);
            assertEquals(1, published, "同一版本并发发布必须恰好一次成功");
            assertEquals(threads - 1, rejected, "其余必须以 VERSION_ALREADY_EXISTS 被拒绝");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void versionsMustBeMonotonicAndHistoryRemainsCallable() throws Exception {
        FaasRuntime runtime = newRuntime();
        runtime.publish("fn-b", 1, "echo", null, null, null);

        // 越序版本必须被明确拒绝，原因可区分
        FaasException.InvalidVersionException ex = assertThrows(
                FaasException.InvalidVersionException.class,
                () -> runtime.publish("fn-b", 3, "echo", null, null, null));
        log.info("out-of-order publish rejected: reason=NON_MONOTONIC_VERSION basis={}",
                ex.getMessage());
        assertTrue(ex.getMessage().contains("expected v2"));

        // 正确顺序发布 v2
        runtime.publish("fn-b", 2, "echo", null, null, null);
        assertEquals(2, runtime.registry().versions("fn-b").size(), "v1/v2 必须共存");

        // 历史版本 v1 仍可显式调用，行为与发布时一致（echo）
        var r1 = runtime.invoke(new com.github.xingjianluo.faasruntimejava.model.InvocationRequest(
                "fn-b", 1, "hello-v1", null));
        var r2 = runtime.invoke(new com.github.xingjianluo.faasruntimejava.model.InvocationRequest(
                "fn-b", 2, "hello-v2", null));
        var rCur = runtime.invoke(new com.github.xingjianluo.faasruntimejava.model.InvocationRequest(
                "fn-b", null, "current", null));
        log.info("history call: fn-b#v1 -> status={} body={} instance={} basis=explicit-version",
                r1.status(), r1.body(), r1.instanceId());
        log.info("history call: fn-b#v2 -> status={} body={} current-pointer -> status={} body={}",
                r2.status(), r2.body(), rCur.status(), rCur.body());
        assertEquals("hello-v1", r1.body());
        assertEquals("hello-v2", r2.body());
        assertEquals("current", rCur.body());
        assertEquals(2, rCur.version(), "未指定版本时必须命中当前生效版本 v2");

        // 已发布历史版本不可覆盖
        assertThrows(FaasException.VersionAlreadyExistsException.class,
                () -> runtime.publish("fn-b", 1, "failing", null, null, null));
    }

    @Test
    void invalidHandlerRejectedAtomicallyAndLeavesNoTrace() {
        FaasRuntime runtime = newRuntime();
        FaasException.InvalidHandlerException ex = assertThrows(
                FaasException.InvalidHandlerException.class,
                () -> runtime.publish("fn-c", 1, "not-exist-type", null, null, null));
        log.info("bad handler publish rejected: reason=INVALID_HANDLER basis={}", ex.getMessage());

        // 发布失败后函数不应存在；此时再用合法处理器发布 v1 必须成功（证明没有残留中间态）
        assertThrows(FaasException.FunctionNotFoundException.class,
                () -> runtime.registry().resolve("fn-c", null));
        FunctionDefinition def = runtime.publish("fn-c", 1, "echo", null, null, null);
        assertEquals(1, def.version());
        log.info("after failed publish, fn-c#v1 published cleanly: {} basis=failed-publish-left-no-state",
                def.qualifiedName());
    }

    @Test
    void concurrentPublishOfDifferentVersionsProducesContiguousChain() throws Exception {
        FaasRuntime runtime = newRuntime();
        int n = 20;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            // 每个发布者先抢占“下一个版本号”：版本号必须由注册中心决定，因此这里改为
            // 多线程循环竞争发布连续版本（每个线程反复尝试 v(k) 直到自己成功一次）。
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int t = 0; t < n; t++) {
                tasks.add(() -> {
                    for (int v = 1; v <= n; v++) {
                        try {
                            runtime.publish("fn-d", v, "echo", null, null, null);
                            log.info("thread={} published fn-d#v{}",
                                    Thread.currentThread().getName(), v);
                            return null;
                        } catch (FaasException.VersionAlreadyExistsException
                                 | FaasException.InvalidVersionException ignore) {
                            // 被其他线程抢先，尝试下一个版本
                        }
                    }
                    return null;
                });
            }
            for (Future<Void> f : pool.invokeAll(tasks)) {
                f.get(10, TimeUnit.SECONDS);
            }
            List<FunctionDefinition> versions = runtime.registry().versions("fn-d");
            log.info("concurrent chained publish final versions={} basis=exactly v1..v{} no gaps",
                    versions.stream().map(FunctionDefinition::version).toList(), n);
            assertEquals(n, versions.size());
            for (int v = 1; v <= n; v++) {
                assertEquals(v, versions.get(v - 1).version());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
