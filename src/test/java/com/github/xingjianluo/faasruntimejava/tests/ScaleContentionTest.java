package com.github.xingjianluo.faasruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.xingjianluo.faasruntimejava.handler.DefaultFunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.handler.builtin.GlobalSideEffectHandler;
import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.InstanceMetrics;
import com.github.xingjianluo.faasruntimejava.model.InvocationRequest;
import com.github.xingjianluo.faasruntimejava.model.InvocationStatus;
import com.github.xingjianluo.faasruntimejava.runtime.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.runtime.RuntimeProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景四：缩容与并发调用竞争。
 * 空闲实例必须可被缩容（到 0），下一次调用表现为冷启动；
 * 缩容与调用并发时：调用不丢失、不重复执行（每个幂等键恰好一次副作用）。
 */
class ScaleContentionTest {

    private static final Logger log = LoggerFactory.getLogger(ScaleContentionTest.class);

    private FaasRuntime newRuntime() {
        return new FaasRuntime(new DefaultFunctionHandlerFactory(), new RuntimeProperties());
    }

    private InvocationRequest req(String name, Integer v, String payload, String key) {
        return new InvocationRequest(name, v, payload, key);
    }

    @Test
    void idleInstancesScaleToZeroAndNextCallColdStarts() throws Exception {
        FaasRuntime runtime = newRuntime();
        // idleTtl=1ms，任何空闲实例都会被下一轮回收
        runtime.publish("scale", 1, "echo", Duration.ofSeconds(2), 4, Duration.ofMillis(1));

        CallResult first = runtime.invoke(req("scale", 1, "a", null));
        assertTrue(first.coldStart());
        InstanceMetrics before = runtime.metrics("scale", 1);
        log.info("before reap: created={} idle={} inflight={} basis=call-finished-instance-idle",
                before.createdInstances(), before.idleInstances(), before.inflight());
        assertEquals(1, before.createdInstances());
        assertEquals(1, before.idleInstances());

        Thread.sleep(5);
        int evicted = runtime.reapIdle();
        InstanceMetrics afterReap = runtime.metrics("scale", 1);
        log.info("reap evicted={} after-reap created={} idle={} basis=idle-ttl-expired-scale-to-zero",
                evicted, afterReap.createdInstances(), afterReap.idleInstances());
        assertEquals(1, evicted);
        assertEquals(0, afterReap.createdInstances());

        CallResult again = runtime.invoke(req("scale", 1, "b", null));
        log.info("[{}] call after scale-in -> status={} coldStart={} instance={} "
                        + "basis=pool-empty-new-cold-start",
                again.invocationId(), again.status(), again.coldStart(), again.instanceId());
        assertEquals(InvocationStatus.SUCCESS, again.status());
        assertTrue(again.coldStart(), "缩容到 0 后下一次调用必须是冷启动");
        assertEquals(2, runtime.metrics("scale", 1).coldStarts());
    }

    @Test
    void scaleDownRacingWithCallsLosesNoCallAndExecutesEachKeyOnce() throws Exception {
        FaasRuntime runtime = newRuntime();
        int concurrency = 8;
        // idleTtl=0：缩容器每轮都会回收全部空闲实例，制造持续缩容压力
        GlobalSideEffectHandler.reset();
        runtime.publish("contended", 1, "globalSideEffect", Duration.ofSeconds(5), concurrency,
                Duration.ZERO);

        int callCount = 40;
        CountDownLatch done = new CountDownLatch(callCount);
        ExecutorService callers = Executors.newFixedThreadPool(concurrency);
        ExecutorService reapers = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "aggressive-reaper");
            t.setDaemon(true);
            return t;
        });
        try {
            // 缩容器疯狂回收空闲实例
            Future<?> reaperTask = reapers.submit(() -> {
                while (done.getCount() > 0) {
                    runtime.reapIdle();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });

            // 每个调用使用唯一幂等键，收集副作用返回值
            List<Future<CallResult>> futures = new ArrayList<>();
            for (int i = 0; i < callCount; i++) {
                final int idx = i;
                futures.add(callers.submit(() -> {
                    CallResult r = runtime.invoke(
                            req("contended", 1, "p", "key-" + idx));
                    done.countDown();
                    return r;
                }));
            }

            int success = 0, replay = 0, rejected = 0;
            Set<String> sideEffectBodies = new HashSet<>();
            Set<String> servedInstances = new HashSet<>();
            for (int i = 0; i < callCount; i++) {
                CallResult r = futures.get(i).get(15, TimeUnit.SECONDS);
                if (r.status() == InvocationStatus.SUCCESS) {
                    success++;
                    assertTrue(sideEffectBodies.add(r.body()),
                            "副作用结果不得重复: " + r.body());
                    servedInstances.add(r.instanceId());
                } else if (r.status() == InvocationStatus.REPLAY) {
                    replay++;
                } else if (r.status() == InvocationStatus.CONCURRENCY_REJECTED) {
                    rejected++;
                } else {
                    throw new AssertionError("unexpected status " + r.status());
                }
            }
            reaperTask.get(5, TimeUnit.SECONDS);

            log.info("scale-contention summary: total={} success={} replay={} rejected={} "
                            + "distinctSideEffects={} distinctInstances={} basis=no-loss-no-dup",
                    callCount, success, replay, rejected, sideEffectBodies.size(),
                    servedInstances.size());

            // 唯一键场景不应有重放；允许超并发拒绝（调用方可重试），但最终不允许丢失
            assertEquals(0, replay, "每个键唯一，不应出现重放");
            assertEquals(0, rejected, "8 并发 / 40 调用，配合 CachedThreadPool 调度不应出现拒绝");
            assertEquals(callCount, success, "缩容竞争下所有调用都必须被服务，不丢失");
            assertEquals(callCount, sideEffectBodies.size(), "每次调用副作用必须恰好执行一次，不重复");

            InstanceMetrics m = runtime.metrics("contended", 1);
            log.info("contended#v1 metrics: total={} success={} coldStarts={} created={} idle={} "
                            + "basis=repeated-cold-starts-under-aggressive-scale-in",
                    m.totalCalls(), m.successCalls(), m.coldStarts(),
                    m.createdInstances(), m.idleInstances());
            assertEquals(callCount, m.totalCalls());
            assertEquals(callCount, m.successCalls());
            assertTrue(m.coldStarts() >= 1, "持续缩容下应观察到多次冷启动");
            assertEquals(0, m.inflight(), "所有租约必须正确归还");
        } finally {
            callers.shutdownNow();
            reapers.shutdownNow();
        }
    }

    @Test
    void inFlightInstanceIsNeverEvicted() throws Exception {
        FaasRuntime runtime = newRuntime();
        // sleep 500ms，idleTtl=0：执行期间疯狂缩容也不能动在飞实例
        runtime.publish("inflight", 1, "sleep", Duration.ofSeconds(5), 2, Duration.ZERO);

        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            Future<CallResult> running = pool.submit(
                    () -> runtime.invoke(req("inflight", 1, "400", null)));
            Thread.sleep(100);
            for (int i = 0; i < 20; i++) {
                int evicted = runtime.reapIdle();
                InstanceMetrics m = runtime.metrics("inflight", 1);
                log.info("reap#{} evicted={} inflight={} created={} basis=in-flight-instance-protected",
                        i, evicted, m.inflight(), m.createdInstances());
                assertEquals(1, m.inflight(), "在飞调用期间 inflight 必须为 1");
                assertEquals(1, m.createdInstances(), "在飞实例绝不能被缩容摘除");
                Thread.sleep(10);
            }
            CallResult r = running.get(5, TimeUnit.SECONDS);
            assertEquals(InvocationStatus.SUCCESS, r.status());
            log.info("[{}] in-flight call completed status={} instance={} "
                    + "basis=never-evicted-while-running", r.invocationId(), r.status(),
                    r.instanceId());
        } finally {
            pool.shutdownNow();
        }
    }
}
