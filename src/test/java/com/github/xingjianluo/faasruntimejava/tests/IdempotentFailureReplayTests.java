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
 * 幂等键与失败结果交叉：首次执行失败（异常/超时）后，同键的后续调用与并发等待者
 * 必须在成败判定上与首次严格一致——失败可识别、绝不算成功、函数体只执行一次。
 * 每个断言打印判定路径，保证结果可复现、可解释。
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
    // 场景一：首次执行失败（异常/超时）后，同键串行回放必须保持失败语义
    // ------------------------------------------------------------------
    @Test
    void firstExecutionFails_serialReplayStaysRecognizableFailure() {
        AtomicInteger boomExecutions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-boom", 1, input -> {
            boomExecutions.incrementAndGet();
            throw new IllegalStateException("intentional boom");
        }).published());

        // 首次：执行体异常 -> HANDLER_ERROR，不是回放、不算成功
        InvocationResult first = runtime.invoke("fn-boom", 1, "order-1", "key-boom");
        log.info("[失败回放] 首次异常 invocationId={} status={} success={} replayed={} 判定路径=执行体抛异常",
                first.invocationId(), first.status(), first.success(), first.replayed());
        assertEquals(InvocationStatus.HANDLER_ERROR, first.status());
        assertFalse(first.success());
        assertFalse(first.replayed(), "首次执行不是回放");
        assertTrue(first.errorMessage().contains("IllegalStateException"));

        // 同键回放：必须是可识别的失败回放，成败与首次一致，错误信息保留
        InvocationResult replay = runtime.invoke("fn-boom", 1, "order-1", "key-boom");
        log.info("[失败回放] 同键回放 invocationId={} status={} success={} replayed={} 判定路径=命中失败幂等键",
                replay.invocationId(), replay.status(), replay.success(), replay.replayed());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILURE, replay.status(),
                "首次失败的回放不能伪装成成功回放");
        assertFalse(replay.success(), "失败回放绝不能算成功");
        assertTrue(replay.replayed(), "失败回放必须可识别为回放");
        assertEquals(first.errorMessage(), replay.errorMessage(), "回放必须保留首次失败原因");
        assertNull(replay.value());
        assertNotEquals(first.invocationId(), replay.invocationId(), "回放是独立调用记录");
        assertEquals(1, boomExecutions.get(), "函数体只真正执行一次");

        // 多次回放结论稳定；换一个键则重新执行（失败键被保留而不是驱逐）
        InvocationResult replay2 = runtime.invoke("fn-boom", 1, "order-1", "key-boom");
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILURE, replay2.status());
        assertFalse(replay2.success());
        InvocationResult otherKey = runtime.invoke("fn-boom", 1, "order-2", "key-boom-2");
        assertEquals(InvocationStatus.HANDLER_ERROR, otherKey.status());
        assertEquals(2, boomExecutions.get(), "不同键各自执行一次");

        // 超时首次失败同样规则：显式容量函数 + 低超时
        runtime.setInvokeTimeoutMillis(200);
        AtomicInteger hangExecutions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-hang", 1, input -> {
            hangExecutions.incrementAndGet();
            Thread.sleep(60_000);
            return input;
        }, 1, 1).published());

        InvocationResult timeoutFirst = runtime.invoke("fn-hang", 1, "job-1", "key-hang");
        log.info("[失败回放] 首次超时 invocationId={} status={} success={} 判定路径=单次执行超时",
                timeoutFirst.invocationId(), timeoutFirst.status(), timeoutFirst.success());
        assertEquals(InvocationStatus.TIMEOUT, timeoutFirst.status());
        assertFalse(timeoutFirst.success());

        InvocationResult timeoutReplay = runtime.invoke("fn-hang", 1, "job-1", "key-hang");
        log.info("[失败回放] 超时同键回放 invocationId={} status={} success={} replayed={} 判定路径=命中失败幂等键",
                timeoutReplay.invocationId(), timeoutReplay.status(),
                timeoutReplay.success(), timeoutReplay.replayed());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILURE, timeoutReplay.status());
        assertFalse(timeoutReplay.success(), "超时失败的回放不能算成功");
        assertTrue(timeoutReplay.replayed());
        assertEquals(timeoutFirst.errorMessage(), timeoutReplay.errorMessage());
        assertEquals(1, hangExecutions.get(), "超时函数体也只执行一次");
    }

    // ------------------------------------------------------------------
    // 场景二：同键并发进入，执行那次失败时，所有等待者拿到的分类与之一致
    // ------------------------------------------------------------------
    @Test
    void concurrentSameKey_executionFails_allCallersSeeConsistentFailure() throws Exception {
        int callers = 8;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-flaky-pay", 1, input -> {
            executions.incrementAndGet();
            started.countDown();
            release.await();
            throw new IllegalStateException("payment gateway down");
        }).published());

        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return runtime.invoke("fn-flaky-pay", 1, "order-9", "key-pay-9");
            }));
        }
        go.countDown();
        // 等执行者进入函数体后，留足时间让其余调用全部登记为等待者，再放行失败
        assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);
        release.countDown();

        int firstFailures = 0;
        int failureReplays = 0;
        String expectedError = null;
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            log.info("[并发失败] invocationId={} status={} success={} replayed={} 判定路径=同键并发执行失败",
                    r.invocationId(), r.status(), r.success(), r.replayed());
            assertFalse(r.success(), "执行失败时同键任何调用都不能算成功，实际=" + r.status());
            assertTrue(r.status() == InvocationStatus.HANDLER_ERROR
                            || r.status() == InvocationStatus.IDEMPOTENT_REPLAY_FAILURE,
                    "同键并发只允许出现“执行失败”或“失败回放”，实际=" + r.status());
            if (r.status() == InvocationStatus.HANDLER_ERROR) {
                firstFailures++;
                assertFalse(r.replayed());
            } else {
                failureReplays++;
                assertTrue(r.replayed(), "等待者必须可识别为回放");
            }
            if (expectedError == null) {
                expectedError = r.errorMessage();
            }
            assertEquals(expectedError, r.errorMessage(), "同键所有调用的失败原因必须一致");
            assertNull(r.value());
        }
        pool.shutdown();
        log.info("[并发失败] 总调用={} 执行失败={} 失败回放={} 函数体执行次数={}",
                callers, firstFailures, failureReplays, executions.get());
        assertEquals(1, firstFailures, "恰好一个调用真正执行并失败");
        assertEquals(callers - 1, failureReplays, "其余等待者全部是失败回放");
        assertEquals(1, executions.get(), "函数体只真正执行一次");

        // 并发失败之后，串行同键调用仍回放同一个失败（结论持续稳定）
        InvocationResult later = runtime.invoke("fn-flaky-pay", 1, "order-9", "key-pay-9");
        log.info("[并发失败] 事后同键 invocationId={} status={} success={} 判定路径=命中失败幂等键",
                later.invocationId(), later.status(), later.success());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY_FAILURE, later.status());
        assertFalse(later.success());
        assertEquals(expectedError, later.errorMessage());
        assertEquals(1, executions.get(), "回放不重复执行");
    }
}
