package com.github.xingjianluo.faasruntimejava.tests;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionInvoker;
import com.github.xingjianluo.faasruntimejava.faas.FunctionRegistry;
import com.github.xingjianluo.faasruntimejava.faas.IdempotencyStore;
import com.github.xingjianluo.faasruntimejava.faas.InstanceManager;
import com.github.xingjianluo.faasruntimejava.faas.InvocationResult;
import com.github.xingjianluo.faasruntimejava.faas.InvocationStatus;
import com.github.xingjianluo.faasruntimejava.faas.PublishResult;
import com.github.xingjianluo.faasruntimejava.faas.PublishStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖四类核心场景：并发发布、调用超时、幂等重放、缩容竞争。
 * 每个断言都打印调用/版本标识与判定依据，保证结果可复现、可解释。
 */
class FaasRuntimeTests {

    private static final Logger log = LoggerFactory.getLogger(FaasRuntimeTests.class);

    private InstanceManager instanceManager;
    private FaasRuntime runtime;

    @BeforeEach
    void setUp() {
        FunctionRegistry registry = new FunctionRegistry();
        instanceManager = new InstanceManager();
        IdempotencyStore idempotencyStore = new IdempotencyStore();
        FunctionInvoker invoker = new FunctionInvoker(registry, instanceManager, idempotencyStore);
        runtime = new FaasRuntime(registry, invoker, instanceManager);
    }

    @AfterEach
    void tearDown() {
        instanceManager.shutdown();
    }

    // ------------------------------------------------------------------
    // 场景一：并发发布
    // ------------------------------------------------------------------
    @Test
    void concurrentPublish_sameVersion_exactlyOneSucceedsAndVersionUsable() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<PublishResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int tag = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                PublishResult r = runtime.publish("fn-concurrent", 1, input -> "v1-" + tag);
                log.info("[并发发布] publisher={} version=1 status={} 判定依据={}",
                        tag, r.status(), r.message());
                return r;
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();

        Map<PublishStatus, Integer> counts = new ConcurrentHashMap<>();
        for (Future<PublishResult> f : futures) {
            counts.merge(f.get(5, TimeUnit.SECONDS).status(), 1, Integer::sum);
        }
        pool.shutdown();
        log.info("[并发发布] 结果分布={}", counts);

        // 判定依据：16 个并发发布同一版本，恰好 1 个成功，
        // 其余只能是 DUPLICATE_VERSION 或 CONCURRENT_MODIFICATION，不允许其他状态
        assertEquals(1, counts.getOrDefault(PublishStatus.PUBLISHED, 0),
                "恰好一个发布成功，实际分布=" + counts);
        int rejected = counts.getOrDefault(PublishStatus.DUPLICATE_VERSION, 0)
                + counts.getOrDefault(PublishStatus.CONCURRENT_MODIFICATION, 0);
        assertEquals(threads - 1, rejected,
                "其余发布必须以可区分原因被拒绝，实际分布=" + counts);

        // 重复发布（串行）必须稳定地报 DUPLICATE_VERSION
        PublishResult dup = runtime.publish("fn-concurrent", 1, input -> "overwrite");
        log.info("[并发发布] 串行重复发布 version=1 status={} 判定依据={}", dup.status(), dup.message());
        assertEquals(PublishStatus.DUPLICATE_VERSION, dup.status());

        // 版本一旦可见即可用：调用已发布版本必须成功且返回发布时的行为（历史版本不可覆盖）
        InvocationResult inv = runtime.invoke("fn-concurrent", 1, "x", null);
        log.info("[并发发布] 调用 invocationId={} version={} status={} value={}",
                inv.invocationId(), inv.version(), inv.status(), inv.value());
        assertEquals(InvocationStatus.SUCCESS, inv.status());
        assertTrue(inv.value().toString().startsWith("v1-"),
                "历史版本行为必须与发布时一致，实际=" + inv.value());

        // 不存在的版本必须可区分地拒绝
        InvocationResult missing = runtime.invoke("fn-concurrent", 99, "x", null);
        log.info("[并发发布] 调用不存在版本 version=99 status={} 判定依据={}",
                missing.status(), missing.errorMessage());
        assertEquals(InvocationStatus.VERSION_NOT_FOUND, missing.status());
    }

    // ------------------------------------------------------------------
    // 场景二：调用超时与结果分类
    // ------------------------------------------------------------------
    @Test
    void invocation_timeoutAndFailureClassificationsAreDistinct() throws Exception {
        runtime.setInvokeTimeoutMillis(300);
        runtime.setMaxConcurrencyPerFunction(1);

        assertTrue(runtime.publish("fn-slow", 1, input -> {
            Thread.sleep(5_000);
            return input;
        }).published());
        assertTrue(runtime.publish("fn-ok", 1, input -> "echo:" + input).published());
        assertTrue(runtime.publish("fn-bad", 1, input -> {
            throw new IllegalStateException("boom");
        }).published());

        // 超时：单次执行超过 300ms 必须归类为 TIMEOUT
        InvocationResult timeout = runtime.invoke("fn-slow", 1, "t", null);
        log.info("[调用超时] invocationId={} status={} 判定依据={}",
                timeout.invocationId(), timeout.status(), timeout.errorMessage());
        assertEquals(InvocationStatus.TIMEOUT, timeout.status());

        // 超时调用释放并发槽位后，后续调用不受影响（失败不污染其他调用）
        InvocationResult ok = runtime.invoke("fn-ok", 1, "a", null);
        log.info("[调用超时] 正常调用 invocationId={} status={} value={}",
                ok.invocationId(), ok.status(), ok.value());
        assertEquals(InvocationStatus.SUCCESS, ok.status());
        assertEquals("echo:a", ok.value());

        // 执行体异常：HANDLER_ERROR，且错误信息保留原始异常类型
        InvocationResult handlerError = runtime.invoke("fn-bad", 1, null, null);
        log.info("[调用超时] 异常调用 invocationId={} status={} 判定依据={}",
                handlerError.invocationId(), handlerError.status(), handlerError.errorMessage());
        assertEquals(InvocationStatus.HANDLER_ERROR, handlerError.status());
        assertTrue(handlerError.errorMessage().contains("IllegalStateException"));

        // 并发超限：fn-slow 被占住（占满唯一槽位）期间，第二个调用必须被拒绝
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        Future<InvocationResult> blocking = pool.submit(() -> {
            started.countDown();
            return runtime.invoke("fn-slow", 1, "hold", null);
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread.sleep(100); // 确保第一个调用已占用槽位
        InvocationResult rejected = runtime.invoke("fn-slow", 1, "overflow", null);
        log.info("[调用超时] 超限调用 invocationId={} status={} 判定依据={}",
                rejected.invocationId(), rejected.status(), rejected.errorMessage());
        assertEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, rejected.status());
        InvocationResult first = blocking.get(10, TimeUnit.SECONDS);
        log.info("[调用超时] 占用槽位的调用 invocationId={} status={}", first.invocationId(), first.status());
        assertEquals(InvocationStatus.TIMEOUT, first.status());
        pool.shutdown();

        // 参数非法与函数不存在：可区分且互不掩盖
        InvocationResult invalid = runtime.invoke("  ", 1, null, null);
        log.info("[调用超时] 非法参数 status={} 判定依据={}", invalid.status(), invalid.errorMessage());
        assertEquals(InvocationStatus.INVALID_ARGUMENT, invalid.status());

        InvocationResult notFound = runtime.invoke("fn-ghost", 1, null, null);
        log.info("[调用超时] 函数不存在 status={} 判定依据={}", notFound.status(), notFound.errorMessage());
        assertEquals(InvocationStatus.FUNCTION_NOT_FOUND, notFound.status());

        // 五类失败状态互不相同，未被统一错误掩盖
        List<InvocationStatus> distinct = List.of(timeout.status(), handlerError.status(),
                rejected.status(), invalid.status(), notFound.status());
        assertEquals(5, distinct.stream().distinct().count(),
                "五类失败必须可区分，实际=" + distinct);
    }

    // ------------------------------------------------------------------
    // 场景三：幂等重放
    // ------------------------------------------------------------------
    @Test
    void idempotentReplay_sameKeyExecutesSideEffectOnce() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-pay", 1, input -> {
            sideEffects.incrementAndGet();
            return "receipt-" + sideEffects.get();
        }).published());

        // 串行重放：同键第二次调用直接回放，不重复执行
        InvocationResult first = runtime.invoke("fn-pay", 1, "order-1", "key-1");
        InvocationResult replay = runtime.invoke("fn-pay", 1, "order-1", "key-1");
        log.info("[幂等重放] 首次 invocationId={} status={} value={}",
                first.invocationId(), first.status(), first.value());
        log.info("[幂等重放] 重放 invocationId={} status={} value={} 判定依据=同幂等键 key-1",
                replay.invocationId(), replay.status(), replay.value());
        assertEquals(InvocationStatus.SUCCESS, first.status());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY, replay.status());
        assertEquals(first.value(), replay.value(), "重放结果必须与首次一致");
        assertEquals(1, sideEffects.get(), "副作用只能执行一次");
        assertNotEquals(first.invocationId(), replay.invocationId(), "重放是独立调用记录");

        // 并发重放：N 个同键并发调用，副作用仍只执行一次
        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return runtime.invoke("fn-pay", 1, "order-2", "key-2");
            }));
        }
        go.countDown();
        int replays = 0;
        Object expectedValue = null;
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            log.info("[幂等重放] 并发调用 invocationId={} status={} value={}",
                    r.invocationId(), r.status(), r.value());
            assertTrue(r.success(), "同键并发调用不允许失败，实际=" + r.status());
            if (r.status() == InvocationStatus.IDEMPOTENT_REPLAY) {
                replays++;
            }
            if (expectedValue == null) {
                expectedValue = r.value();
            }
            assertEquals(expectedValue, r.value(), "所有同键调用结果必须一致");
        }
        pool.shutdown();
        log.info("[幂等重放] 并发 {} 个调用中回放 {} 个，副作用总次数={}", threads, replays, sideEffects.get());
        assertEquals(2, sideEffects.get(),
                "key-1 与 key-2 各执行一次，副作用总数必须为 2，实际=" + sideEffects.get());
    }

    // ------------------------------------------------------------------
    // 场景四：缩容竞争
    // ------------------------------------------------------------------
    @Test
    void scaleDown_racingWithInvocations_neverLosesOrDuplicatesCalls() throws Exception {
        // 空闲 150ms 即缩容，每 50ms 扫描一次，制造缩容与调用的竞争窗口
        runtime.setIdleTimeoutMillis(150);
        runtime.setReapIntervalMillis(50);

        AtomicInteger executions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-scale", 1, input -> {
            executions.incrementAndGet();
            return "run-" + executions.get();
        }).published());

        // 冷启动：首次调用必须标记 coldStart=true
        InvocationResult cold = runtime.invoke("fn-scale", 1, "first", null);
        log.info("[缩容竞争] 首次调用 invocationId={} status={} coldStart={} 判定依据=无暖实例",
                cold.invocationId(), cold.status(), cold.coldStart());
        assertEquals(InvocationStatus.SUCCESS, cold.status());
        assertTrue(cold.coldStart(), "首次调用必须是冷启动");

        // 实例复用：紧接着的调用必须复用暖实例
        InvocationResult warm = runtime.invoke("fn-scale", 1, "second", null);
        log.info("[缩容竞争] 热调用 invocationId={} status={} coldStart={} 判定依据=实例未空闲超期",
                warm.invocationId(), warm.status(), warm.coldStart());
        assertFalse(warm.coldStart(), "间隔小于空闲超时，必须复用暖实例");

        // 等待缩容：实例数应降为 0
        long deadline = System.currentTimeMillis() + 5_000;
        while (runtime.activeInstances() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        log.info("[缩容竞争] 缩容后指标={}", runtime.metrics());
        assertEquals(0, runtime.activeInstances(), "空闲超期后实例必须缩容到 0");

        // 缩容与并发调用竞争：在缩容持续发生的同时发起多轮调用，
        // 判定依据：每次调用都成功且执行次数与调用次数严格相等（不丢失、不重复）
        int rounds = 40;
        int executionsBefore = executions.get();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            int tag = i;
            futures.add(pool.submit(() -> {
                Thread.sleep((tag % 5) * 60L); // 错落到达，跨越多次缩容
                InvocationResult r = runtime.invoke("fn-scale", 1, "race-" + tag, null);
                log.info("[缩容竞争] 竞争调用 tag={} invocationId={} status={} coldStart={}",
                        tag, r.invocationId(), r.status(), r.coldStart());
                return r;
            }));
        }
        int success = 0;
        int coldStarts = 0;
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            if (r.status() == InvocationStatus.SUCCESS) {
                success++;
            }
            if (r.coldStart()) {
                coldStarts++;
            }
        }
        pool.shutdown();
        int executed = executions.get() - executionsBefore;
        log.info("[缩容竞争] 调用数={} 成功数={} 实际执行数={} 期间冷启动数={} 判定依据=执行数必须等于调用数",
                rounds, success, executed, coldStarts);
        assertEquals(rounds, success, "缩容竞争下不允许调用丢失");
        assertEquals(rounds, executed, "缩容竞争下不允许重复执行或漏执行");
        assertTrue(coldStarts > 0, "竞争期间应发生至少一次冷启动（缩容确实在发生）");
    }
}
