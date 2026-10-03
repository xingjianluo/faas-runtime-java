package com.github.xingjianluo.faasruntimejava.tests;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionInvoker;
import com.github.xingjianluo.faasruntimejava.faas.FunctionRegistry;
import com.github.xingjianluo.faasruntimejava.faas.IdempotencyStore;
import com.github.xingjianluo.faasruntimejava.faas.InstanceManager;
import com.github.xingjianluo.faasruntimejava.faas.InvocationResult;
import com.github.xingjianluo.faasruntimejava.faas.InvocationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等键与失败结果的交叉分类：
 *
 * <p>业务一（首次执行失败后同键回放）：首次超时/异常 -> 同键后续调用必须是
 * 可识别的失败回放 {@link InvocationStatus#IDEMPOTENT_REPLAY_FAILED}，
 * 与首次成败一致（不算成功），且函数体只真正执行一次。
 *
 * <p>业务二（同键并发时执行那次失败）：真正执行的那一次失败时，所有等待结果的
 * 并发调用必须与执行那次分类一致——恰好 1 个原始失败状态，其余全是失败回放，
 * 不允许任何人拿到“成功”。
 *
 * <p>边界对照：成功回放仍是 {@link InvocationStatus#IDEMPOTENT_REPLAY} 且算成功；
 * 失败回放与容量/并发被拒（从未执行）是可区分的状态。
 */
class IdempotentFailureReplayTests {

    private static final Logger log = LoggerFactory.getLogger(IdempotentFailureReplayTests.class);

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
    // 业务一：首次执行失败（异常/超时）后，同键回放必须仍是可识别的失败
    // ------------------------------------------------------------------
    @Test
    void firstExecutionFails_handlerError_replayStaysRecognizableFailure() {
        AtomicInteger executions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-idem-err", 1, input -> {
            executions.incrementAndGet();
            throw new IllegalStateException("payment gateway down");
        }).published());

        // 首次：真正执行并失败
        InvocationResult first = runtime.invoke("fn-idem-err", 1, "order-1", "pay-key-1");
        log.info("[失败回放-异常] 首次 invocationId={} status={} success={} 判定依据={}",
                first.invocationId(), first.status(), first.success(), first.errorMessage());
        assertEquals(InvocationStatus.HANDLER_ERROR, first.status());
        assertFalse(first.success());
        assertTrue(first.errorMessage().contains("IllegalStateException"));

        // 同键回放：必须是失败回放，不能变成成功
        InvocationResult replay = runtime.invoke("fn-idem-err", 1, "order-1", "pay-key-1");
        log.info("[失败回放-异常] 回放 invocationId={} status={} success={} 判定依据=同键命中首次失败结果",
                replay.invocationId(), replay.status(), replay.success());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILED, replay.status(),
                "首次失败后的同键回放必须是失败回放，不能被算成成功");
        assertFalse(replay.success(), "失败回放不能算成功");
        assertNotEquals(InvocationStatus.IDEMPOTENT_REPLAY, replay.status(),
                "失败回放不能与成功回放混用同一状态");
        // 失败结果仍可识别：保留首次的错误信息，可据此追溯失败原因
        assertEquals(first.errorMessage(), replay.errorMessage(),
                "失败回放必须保留首次的错误信息以便识别");
        assertNull(replay.value());
        // 与“被拒”严格区分：失败回放说明真正执行过，被拒说明没执行
        assertNotEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, replay.status());
        assertNotEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, replay.status());
        assertEquals(1, executions.get(), "函数体只能真正执行一次，实际=" + executions.get());

        // 多次回放结论稳定
        InvocationResult replay2 = runtime.invoke("fn-idem-err", 1, "order-1", "pay-key-1");
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILED, replay2.status());
        assertFalse(replay2.success());
        assertEquals(1, executions.get());
    }

    @Test
    void firstExecutionFails_timeout_replayStaysRecognizableFailure() {
        runtime.setInvokeTimeoutMillis(200);
        AtomicInteger executions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-idem-slow", 1, input -> {
            executions.incrementAndGet();
            Thread.sleep(60_000);
            return input;
        }).published());

        InvocationResult first = runtime.invoke("fn-idem-slow", 1, "job-1", "job-key-1");
        log.info("[失败回放-超时] 首次 invocationId={} status={} success={} 判定依据={}",
                first.invocationId(), first.status(), first.success(), first.errorMessage());
        assertEquals(InvocationStatus.TIMEOUT, first.status());
        assertFalse(first.success());

        InvocationResult replay = runtime.invoke("fn-idem-slow", 1, "job-1", "job-key-1");
        log.info("[失败回放-超时] 回放 invocationId={} status={} success={} 判定依据=同键命中首次超时结果",
                replay.invocationId(), replay.status(), replay.success());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILED, replay.status(),
                "首次超时后的同键回放必须是失败回放");
        assertFalse(replay.success());
        assertEquals(first.errorMessage(), replay.errorMessage());
        assertEquals(1, executions.get(), "超时后回放不能重复执行函数体");
    }

    // ------------------------------------------------------------------
    // 业务二：同键并发进入，执行那次失败 -> 等待者不能各自拿到“成功”
    // ------------------------------------------------------------------
    @Test
    void concurrentSameKey_executingCallFails_allWaitersGetConsistentFailure() throws Exception {
        AtomicInteger executions = new AtomicInteger(0);
        CountDownLatch entered = new CountDownLatch(1);
        assertTrue(runtime.publish("fn-idem-race", 1, input -> {
            executions.incrementAndGet();
            entered.countDown();
            try {
                Thread.sleep(300); // 留出窗口让同键并发调用都进入等待
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("boom during concurrent execution");
        }).published());

        int callers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return runtime.invoke("fn-idem-race", 1, "order-X", "race-key");
            }));
        }
        go.countDown();
        assertTrue(entered.await(5, TimeUnit.SECONDS), "执行那次必须已进入函数体");

        int originalFailures = 0;
        int failureReplays = 0;
        String firstError = null;
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            log.info("[并发失败一致] invocationId={} status={} success={} 判定依据={}",
                    r.invocationId(), r.status(), r.success(), r.errorMessage());
            assertFalse(r.success(), "执行失败时，任何同键调用都不能拿到成功，实际=" + r.status());
            assertNotEquals(InvocationStatus.IDEMPOTENT_REPLAY, r.status(),
                    "失败的那一次不能被包装成成功回放");
            if (r.status() == InvocationStatus.HANDLER_ERROR) {
                originalFailures++;
            } else if (r.status() == InvocationStatus.IDEMPOTENT_REPLAY_FAILED) {
                failureReplays++;
            } else {
                throw new AssertionError("同键并发只允许出现原始失败或失败回放，实际=" + r.status());
            }
            if (firstError == null) {
                firstError = r.errorMessage();
            }
            assertEquals(firstError, r.errorMessage(), "所有同键调用的失败信息必须一致");
        }
        pool.shutdown();
        log.info("[并发失败一致] 原始失败={} 失败回放={} 执行次数={}",
                originalFailures, failureReplays, executions.get());
        assertEquals(1, originalFailures, "恰好一个调用是真正执行并拿到原始失败");
        assertEquals(callers - 1, failureReplays, "其余等待者必须全部拿到失败回放");
        assertEquals(1, executions.get(), "函数体只真正执行一次，实际=" + executions.get());

        // 并发窗口过后，同键串行调用仍是失败回放（结论稳定）
        InvocationResult after = runtime.invoke("fn-idem-race", 1, "order-X", "race-key");
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILED, after.status());
        assertFalse(after.success());
        assertEquals(1, executions.get());
    }

    // ------------------------------------------------------------------
    // 边界对照：成功回放不受影响；失败回放/成功回放/被拒三者可区分
    // ------------------------------------------------------------------
    @Test
    void successReplayUnaffected_andThreeOutcomesAreDistinguishable() {
        AtomicInteger executions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-idem-mix", 1, input -> {
            executions.incrementAndGet();
            String s = String.valueOf(input);
            if (s.startsWith("bad")) {
                throw new IllegalStateException("bad input");
            }
            return "ok-" + s;
        }).published());

        // 成功键：回放仍是 IDEMPOTENT_REPLAY 且算成功
        InvocationResult okFirst = runtime.invoke("fn-idem-mix", 1, "good-1", "good-key");
        InvocationResult okReplay = runtime.invoke("fn-idem-mix", 1, "good-1", "good-key");
        log.info("[回放分类对照] 成功键回放 status={} success={}", okReplay.status(), okReplay.success());
        assertEquals(InvocationStatus.SUCCESS, okFirst.status());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY, okReplay.status());
        assertTrue(okReplay.success());
        assertEquals(okFirst.value(), okReplay.value());

        // 失败键：回放是 IDEMPOTENT_REPLAY_FAILED 且不算成功
        InvocationResult badFirst = runtime.invoke("fn-idem-mix", 1, "bad-1", "bad-key");
        InvocationResult badReplay = runtime.invoke("fn-idem-mix", 1, "bad-1", "bad-key");
        log.info("[回放分类对照] 失败键回放 status={} success={}", badReplay.status(), badReplay.success());
        assertEquals(InvocationStatus.HANDLER_ERROR, badFirst.status());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILED, badReplay.status());
        assertFalse(badReplay.success());

        // 三种“非首次执行”结果两两可区分：成功回放 / 失败回放 / 被拒
        assertNotEquals(okReplay.status(), badReplay.status());
        assertNotEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, badReplay.status());
        assertNotEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, badReplay.status());
        assertEquals(2, executions.get(), "两个键各执行一次，回放不重复执行");
    }
}
