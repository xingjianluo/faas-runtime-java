package com.github.xingjianluo.faasruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.faasruntimejava.handler.DefaultFunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.InstanceMetrics;
import com.github.xingjianluo.faasruntimejava.model.InvocationRequest;
import com.github.xingjianluo.faasruntimejava.model.InvocationStatus;
import com.github.xingjianluo.faasruntimejava.runtime.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.runtime.RuntimeProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景二：调用超时、执行异常、超并发、非法参数的结果分类必须可区分，
 * 冷启动与实例复用必须在结果/指标中可区分。
 */
class InvocationTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(InvocationTimeoutTest.class);

    private FaasRuntime newRuntime() {
        return new FaasRuntime(new DefaultFunctionHandlerFactory(), new RuntimeProperties());
    }

    private InvocationRequest req(String name, Integer v, String payload, String key) {
        return new InvocationRequest(name, v, payload, key);
    }

    @Test
    void timeoutIsDistinctFromFunctionError() {
        FaasRuntime runtime = newRuntime();
        // 300ms 超时的 sleep 函数
        runtime.publish("slow", 1, "sleep", Duration.ofMillis(300), 2, Duration.ofSeconds(30));
        // 每次抛异常的函数
        runtime.publish("buggy", 1, "failing", Duration.ofSeconds(2), 2, Duration.ofSeconds(30));

        CallResult timeout = runtime.invoke(req("slow", 1, "1000", null));
        log.info("[{}] slow#v1 -> status={} errorCode={} instance={} basis=deadline-300ms-vs-sleep-1000ms",
                timeout.invocationId(), timeout.status(), timeout.errorCode(), timeout.instanceId());
        assertEquals(InvocationStatus.TIMEOUT, timeout.status());
        assertEquals("TIMEOUT", timeout.errorCode());

        // 超时后给工作线程一点时间真正结束，随后下一次调用必须仍可用（实例被丢弃后冷启动）
        CallResult after = runtime.invoke(req("slow", 1, "10", null));
        log.info("[{}] slow#v1 after-timeout -> status={} coldStart={} instance={} "
                        + "basis=timed-out-instance-discarded-fresh-coldstart",
                after.invocationId(), after.status(), after.coldStart(), after.instanceId());
        assertEquals(InvocationStatus.SUCCESS, after.status());
        assertTrue(after.coldStart(), "超时实例被丢弃，再次调用必须冷启动全新实例");

        CallResult error = runtime.invoke(req("buggy", 1, "x", null));
        log.info("[{}] buggy#v1 -> status={} errorCode={} basis=handler-threw-IllegalState",
                error.invocationId(), error.status(), error.errorCode());
        assertEquals(InvocationStatus.FUNCTION_ERROR, error.status());
        assertEquals("FUNCTION_ERROR", error.errorCode());
        assertTrue(error.errorMessage().contains("boom"));
    }

    @Test
    void concurrencyRejectionIsDistinctAndHasNoSideEffects() throws Exception {
        FaasRuntime runtime = newRuntime();
        // 并发上限 2：前两个调用占住实例，其余应被明确拒绝
        runtime.publish("gate", 1, "sleep", Duration.ofSeconds(5), 2, Duration.ofSeconds(30));

        int total = 6;
        // 所有调用者同一起跑线，且都执行 800ms 睡眠，保证峰值时刻恰好 2 个在飞
        CyclicBarrier barrier = new CyclicBarrier(total);
        ExecutorService pool = Executors.newFixedThreadPool(total);
        try {
            List<Future<CallResult>> futures = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    CallResult r = runtime.invoke(req("gate", 1, "800", null));
                    log.info("[{}] gate caller#{} -> status={} errorCode={} instance={} basis=limit=2",
                            r.invocationId(), idx, r.status(), r.errorCode(), r.instanceId());
                    return r;
                }));
            }

            int success = 0, rejected = 0;
            for (Future<CallResult> f : futures) {
                CallResult r = f.get(10, TimeUnit.SECONDS);
                if (r.status() == InvocationStatus.SUCCESS) {
                    success++;
                } else if (r.status() == InvocationStatus.CONCURRENCY_REJECTED) {
                    rejected++;
                    assertEquals("CONCURRENCY_LIMIT_EXCEEDED", r.errorCode());
                }
            }
            log.info("concurrency summary: success={} rejected={} basis=exactly-2-slots",
                    success, rejected);
            assertEquals(2, success);
            assertEquals(total - 2, rejected);

            // 被拒绝调用不占用实例：槽位释放后新调用应成功
            Thread.sleep(900);
            CallResult later = runtime.invoke(req("gate", 1, "5", null));
            assertEquals(InvocationStatus.SUCCESS, later.status());

            InstanceMetrics m = runtime.metrics("gate", 1);
            log.info("gate#v1 metrics: total={} success={} rejected={} inflight={} "
                            + "basis=rejections-counted-separately",
                    m.totalCalls(), m.successCalls(), m.concurrencyRejectedCalls(), m.inflight());
            assertEquals(total - 2, m.concurrencyRejectedCalls());
            assertEquals(0, m.inflight(), "所有租约必须已归还");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void invalidArgumentAndMissingVersionAreDistinct() {
        FaasRuntime runtime = newRuntime();
        runtime.publish("ok", 1, "echo", null, null, null);

        CallResult blank = runtime.invoke(req("  ", null, "p", null));
        CallResult noFunc = runtime.invoke(req("ghost", null, "p", null));
        CallResult noVersion = runtime.invoke(req("ok", 9, "p", null));

        log.info("[{}] blank-name -> {} [{}] basis=name-blank",
                blank.invocationId(), blank.status(), blank.errorCode());
        log.info("[{}] missing-fn -> {} [{}] basis=function-never-published",
                noFunc.invocationId(), noFunc.status(), noFunc.errorCode());
        log.info("[{}] missing-version -> {} [{}] basis=v9-does-not-exist",
                noVersion.invocationId(), noVersion.status(), noVersion.errorCode());

        assertEquals(InvocationStatus.INVALID_ARGUMENT, blank.status());
        assertEquals(InvocationStatus.VERSION_NOT_FOUND, noFunc.status());
        assertEquals("FUNCTION_NOT_FOUND", noFunc.errorCode());
        assertEquals(InvocationStatus.VERSION_NOT_FOUND, noVersion.status());
        assertEquals("VERSION_NOT_FOUND", noVersion.errorCode());
        assertFalse(noFunc.succeeded() && noVersion.succeeded());
    }

    @Test
    void coldStartThenReuseIsObservable() {
        FaasRuntime runtime = newRuntime();
        runtime.publish("warm", 1, "echo", null, 2, Duration.ofMinutes(10));

        CallResult first = runtime.invoke(req("warm", 1, "a", null));
        CallResult second = runtime.invoke(req("warm", 1, "b", null));
        log.info("[{}] first  -> status={} coldStart={} instance={} basis=first-ever-call",
                first.invocationId(), first.status(), first.coldStart(), first.instanceId());
        log.info("[{}] second -> status={} coldStart={} instance={} basis=idle-instance-reused",
                second.invocationId(), second.status(), second.coldStart(), second.instanceId());

        assertTrue(first.coldStart());
        assertFalse(second.coldStart());
        assertEquals(first.instanceId(), second.instanceId(), "空闲实例应被复用");

        InstanceMetrics m = runtime.metrics("warm", 1);
        assertEquals(1, m.coldStarts(), "两次调用只应冷启动一次");
        assertEquals(1, m.createdInstances(), "复用后实例数保持 1");
        log.info("warm#v1 metrics: created={} coldStarts={} total={} basis=single-warm-instance",
                m.createdInstances(), m.coldStarts(), m.totalCalls());
    }
}
