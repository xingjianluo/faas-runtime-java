package com.github.xingjianluo.faasruntimejava.tests;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionHandlerFactory;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 暖实例增强测试：实例级状态隔离、故障实例淘汰、容量与幂等交叉分类、
 * 淘汰替换不丢不重、新版本不影响老版本实例。
 */
class WarmInstanceTests {

    private static final Logger log = LoggerFactory.getLogger(WarmInstanceTests.class);

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

    /** 可重复使用的并发门：调用进入处理器时登记，测试确认占位数后再放行 */
    static final class Gate {
        final AtomicInteger enteredCount = new AtomicInteger(0);
        volatile CountDownLatch pass = new CountDownLatch(1);

        void signalEntered() {
            enteredCount.incrementAndGet();
        }

        void reset() {
            enteredCount.set(0);
            pass = new CountDownLatch(1);
        }

        void awaitEntered(int n) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000;
            while (enteredCount.get() < n && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(n, enteredCount.get(), n + " 个调用都应进入处理器");
        }

        void awaitBoth() throws InterruptedException {
            awaitEntered(2);
        }

        void open() {
            pass.countDown();
        }
    }

    /**
     * 实例级状态工厂：每个实例独立持有计数器（状态隔离）；
     * 输入为 "GATE" 时在共享 Gate 上阻塞，用于制造“两实例同时忙”。
     */
    private static FunctionHandlerFactory statefulGateFactory(AtomicInteger created, Gate gate) {
        return () -> {
            created.incrementAndGet();
            AtomicInteger counter = new AtomicInteger(0);
            return input -> {
                int n = counter.incrementAndGet();
                if ("GATE".equals(input)) {
                    gate.signalEntered();
                    assertTrue(gate.pass.await(10, TimeUnit.SECONDS), "门必须被测试放行");
                }
                return n;
            };
        };
    }

    /** 发起两个并发 GATE 调用，返回两个调用结果（必然落在两个不同实例） */
    private List<InvocationResult> invokeGatePair(Gate gate) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int k = 0; k < 2; k++) {
            futures.add(pool.submit(() -> runtime.invoke("fn-state", 1, "GATE", null)));
        }
        gate.awaitBoth();
        Thread.sleep(50);
        gate.open();
        List<InvocationResult> results = new ArrayList<>();
        for (Future<InvocationResult> f : futures) {
            results.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();
        return results;
    }

    // ------------------------------------------------------------------
    // 场景一：多实例实例级状态隔离 + 冷/暖标记可识别处理实例
    // ------------------------------------------------------------------
    @Test
    void multipleInstances_holdIndependentState_andResultsIdentifyInstance() throws Exception {
        // 容量：最多 2 个实例，每实例同时只能跑 1 个调用
        AtomicInteger created = new AtomicInteger(0);
        Gate gate = new Gate();
        assertTrue(runtime.publish("fn-state", 1,
                statefulGateFactory(created, gate), 2, 1).published());

        // 第一轮：两个并发调用冷启动 A、B（各占一个实例）
        List<InvocationResult> first = invokeGatePair(gate);
        InvocationResult r1 = first.get(0);
        InvocationResult r2 = first.get(1);
        log.info("[实例隔离] 冷启动轮：实例={} coldStart={} 计数={}；实例={} coldStart={} 计数={}",
                r1.instanceId(), r1.coldStart(), r1.value(),
                r2.instanceId(), r2.coldStart(), r2.value());
        assertEquals(InvocationStatus.SUCCESS, r1.status());
        assertEquals(InvocationStatus.SUCCESS, r2.status());
        assertTrue(r1.coldStart() && r2.coldStart(), "首轮两个调用都必须是冷启动");
        assertNotNull(r1.instanceId());
        assertNotEquals(r1.instanceId(), r2.instanceId(), "两个调用必须由不同实例处理");
        assertEquals(2, created.get(), "工厂必须为两个实例各创建一次处理器");

        // 后续 3 轮：每轮两个调用并发占位，实例并发=1，必然 A、B 各接一个且都是暖复用。
        // 每实例计数连续累加（状态延续），彼此独立从 1 起（状态不串）。
        Map<String, List<Integer>> valuesByInstance = new HashMap<>();
        valuesByInstance.computeIfAbsent(r1.instanceId(), id -> new ArrayList<>()).add((Integer) r1.value());
        valuesByInstance.computeIfAbsent(r2.instanceId(), id -> new ArrayList<>()).add((Integer) r2.value());

        for (int round = 0; round < 3; round++) {
            gate.reset();
            List<InvocationResult> pair = invokeGatePair(gate);
            String ids = pair.get(0).instanceId() + "," + pair.get(1).instanceId();
            log.info("[实例隔离] 暖复用轮#{} 实例=[{}] 计数=[{},{}] 判定路径=复用暖实例",
                    round, ids, pair.get(0).value(), pair.get(1).value());
            for (InvocationResult r : pair) {
                assertEquals(InvocationStatus.SUCCESS, r.status());
                assertFalse(r.coldStart(), "暖实例存在时不允许再冷启动，实例=" + r.instanceId());
                assertTrue(r.instanceId().equals(r1.instanceId()) || r.instanceId().equals(r2.instanceId()),
                        "只能复用已有的两个实例，实际=" + r.instanceId());
                valuesByInstance.get(r.instanceId()).add((Integer) r.value());
            }
            assertNotEquals(pair.get(0).instanceId(), pair.get(1).instanceId(),
                    "每轮两个并发调用必须分布到不同实例");
        }

        log.info("[实例隔离] 各实例计数序列={} 工厂累计创建实例数={}", valuesByInstance, created.get());
        // 状态隔离核心断言：每个实例首次计数都是 1，之后连续递增到 4
        assertEquals(List.of(1, 2, 3, 4), valuesByInstance.get(r1.instanceId()),
                "实例 A 计数必须独立连续，实际=" + valuesByInstance.get(r1.instanceId()));
        assertEquals(List.of(1, 2, 3, 4), valuesByInstance.get(r2.instanceId()),
                "实例 B 计数必须独立连续，实际=" + valuesByInstance.get(r2.instanceId()));
        assertEquals(2, created.get(), "复用期间不得再创建实例");
        assertEquals(2, runtime.activeInstances(), "两个健康实例都应保留");
    }

    // ------------------------------------------------------------------
    // 场景二：故障实例（执行异常/超时）立即淘汰，永不复用，替换不超上限
    // ------------------------------------------------------------------
    @Test
    void faultyInstances_areEvictedAndNeverReused_replacementStaysWithinCap() {
        // 单实例容量（maxWarm=1, perInstance=1）：故障后只能冷启动替换
        AtomicInteger created = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-flaky", 1,
                (FunctionHandlerFactory) () -> {
                    created.incrementAndGet();
                    int id = created.get();
                    return input -> {
                        throw new IllegalStateException("boom from instance " + id);
                    };
                }, 1, 1).published());

        // 连续 3 次执行异常：每次都 HANDLER_ERROR，每次的实例都应不同（旧实例不复用）
        String previousInstance = null;
        for (int i = 0; i < 3; i++) {
            InvocationResult r = runtime.invoke("fn-flaky", 1, "x", null);
            log.info("[故障淘汰] 第{}次异常调用 实例={} status={} 判定路径=执行体异常，实例淘汰 依据={}",
                    i, r.instanceId(), r.status(), r.errorMessage());
            assertEquals(InvocationStatus.HANDLER_ERROR, r.status());
            assertNotNull(r.instanceId(), "故障调用也必须真实落到某个实例上");
            if (previousInstance != null) {
                assertNotEquals(previousInstance, r.instanceId(),
                        "出问题的实例不得再次被选中，实际复用了 " + r.instanceId());
            }
            previousInstance = r.instanceId();
            assertTrue(runtime.activeInstances() <= 1,
                    "反复出问题也不允许健康实例数超过上限，实际=" + runtime.activeInstances());
        }
        assertEquals(3, instanceManager.evictedCount(), "三个故障实例都应被淘汰");
        assertEquals(0, runtime.activeInstances(), "淘汰后健康池应为空（下一次调用冷启动替换）");
        assertEquals(0, instanceManager.drainingInstances(), "无在飞调用时不应残留 draining 实例");

        // 替换调用：必须冷启动一个全新健康实例
        InvocationResult replacement = runtime.invoke("fn-flaky", 1, "x", null);
        // fn-flaky 仍会失败，但它是一个新实例（冷启动），失败后同样被淘汰
        assertEquals(InvocationStatus.HANDLER_ERROR, replacement.status());
        assertTrue(replacement.coldStart(), "故障实例被淘汰后，后续调用必须重新冷启动");
        assertNotEquals(previousInstance, replacement.instanceId());
        log.info("[故障淘汰] 替换调用 新实例={} coldStart=true 判定路径=池内无健康实例，冷启动替换",
                replacement.instanceId());
    }

    @Test
    void timedOutInstance_isEvicted_andNextCallColdStarts() {
        runtime.setInvokeTimeoutMillis(200);
        // 共享标志：首实例（坏）一直睡；翻为 true 后，只有“新创建的实例”会立即返回
        java.util.concurrent.atomic.AtomicBoolean healthyNow =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        AtomicInteger created = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-timeout", 1,
                (FunctionHandlerFactory) () -> {
                    int id = created.incrementAndGet();
                    return input -> {
                        if (id == 1 || !healthyNow.get()) {
                            Thread.sleep(2_000); // 坏实例：睡死触发超时
                        }
                        return "ok-" + id;
                    };
                }, 1, 1).published());

        InvocationResult timeout = runtime.invoke("fn-timeout", 1, "t", null);
        log.info("[故障淘汰] 超时调用 实例={} status={} 判定路径=超时中断，实例淘汰",
                timeout.instanceId(), timeout.status());
        assertEquals(InvocationStatus.TIMEOUT, timeout.status());
        assertNotNull(timeout.instanceId());
        assertEquals(0, runtime.activeInstances(), "超时实例必须立即退出健康池");
        assertEquals(1, instanceManager.evictedCount());

        // 放行后续新实例：下一次调用冷启动一个健康实例并成功
        healthyNow.set(true);
        InvocationResult fresh = runtime.invoke("fn-timeout", 1, "a", null);
        log.info("[故障淘汰] 替换调用 新实例={} coldStart={} status={} 判定路径=池空冷启动健康实例",
                fresh.instanceId(), fresh.coldStart(), fresh.status());
        assertEquals(InvocationStatus.SUCCESS, fresh.status());
        assertTrue(fresh.coldStart(), "超时实例淘汰后必须冷启动替换");
        assertNotEquals(timeout.instanceId(), fresh.instanceId(), "替换实例必须是新实例");
        assertEquals("ok-2", fresh.value());

        // 再一次调用复用该健康实例（暖复用），证明后续调用稳定落到健康实例
        InvocationResult warm = runtime.invoke("fn-timeout", 1, "b", null);
        assertEquals(InvocationStatus.SUCCESS, warm.status());
        assertFalse(warm.coldStart());
        assertEquals(fresh.instanceId(), warm.instanceId());
        log.info("[故障淘汰] 暖复用 实例={} 判定路径=健康实例继续接客", warm.instanceId());
        assertEquals(1, runtime.activeInstances());
        assertEquals(1, instanceManager.evictedCount(), "健康实例不应再被淘汰");
    }

    // ------------------------------------------------------------------
    // 场景三：容量到顶立即拒绝（不排队）；幂等 × 容量并发的结果分类
    // ------------------------------------------------------------------
    @Test
    void capacityFull_rejectsImmediately_noQueue() throws Exception {
        // 1 个实例、每实例 1 并发
        Gate gate = new Gate();
        assertTrue(runtime.publish("fn-cap", 1, statefulGateFactory(new AtomicInteger(), gate), 1, 1)
                .published());

        ExecutorService pool = Executors.newFixedThreadPool(1);
        Future<InvocationResult> holder = pool.submit(() -> runtime.invoke("fn-cap", 1, "GATE", null));
        gate.awaitEntered(1);
        Thread.sleep(100);

        long before = System.nanoTime();
        InvocationResult rejected = runtime.invoke("fn-cap", 1, "overflow", null);
        long elapsedMs = (System.nanoTime() - before) / 1_000_000;
        log.info("[容量幂等] 容量到顶拒绝 status={} 耗时={}ms 依据={}",
                rejected.status(), elapsedMs, rejected.errorMessage());
        assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, rejected.status());
        assertFalse(rejected.success(), "被拒结果不能算成功");
        assertFalse(rejected.replayed(), "直接拒绝不是回放");
        assertTrue(elapsedMs < 500, "到顶必须立即拒绝，不得排队等待，实际耗时=" + elapsedMs + "ms");

        gate.open();
        InvocationResult held = holder.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(InvocationStatus.SUCCESS, held.status());
    }

    @Test
    void idempotentKey_concurrentAtCapacity_executesOnce_replaysAreClassifiedCorrectly() throws Exception {
        // 1 实例 / 1 并发。同一个幂等键并发进来：第一个冷启动执行，
        // 其余在幂等层等待（不参与容量竞争），完成后拿到同一份成功回放。
        Gate gate = new Gate();
        AtomicInteger sideEffects = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-ideo-cap", 1, () -> {
            AtomicInteger counter = new AtomicInteger(0);
            return input -> {
                sideEffects.incrementAndGet();
                counter.incrementAndGet();
                if ("GATE".equals(input)) {
                    gate.signalEntered();
                    assertTrue(gate.pass.await(10, TimeUnit.SECONDS));
                }
                return "done";
            };
        }, 1, 1).published());

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> runtime.invoke("fn-ideo-cap", 1, "GATE", "K-SAME")));
        }
        // 8 个同键并发：只有第一个进入处理器；等待者阻塞在幂等 future 上
        Thread.sleep(200);
        log.info("[容量幂等] 同键 {} 并发，执行期间指标={}", threads, runtime.metrics());
        gate.open();

        int replays = 0;
        int successes = 0;
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            assertTrue(r.success(), "同键并发全部应得到成功结果，实际=" + r.status());
            if (r.status() == InvocationStatus.SUCCESS) {
                successes++;
            }
            if (r.status() == InvocationStatus.IDEMPOTENT_REPLAY) {
                replays++;
                assertTrue(r.replayed(), "回放结果必须可识别为回放");
                assertEquals(InvocationStatus.SUCCESS, r.originStatus(),
                        "成功回放的原始状态必须是 SUCCESS");
            }
        }
        pool.shutdown();
        log.info("[容量幂等] 同键并发：真正成功={} 成功回放={} 副作用次数={}", successes, replays, sideEffects.get());
        assertEquals(1, successes, "同一幂等键只允许一个真正执行");
        assertEquals(threads - 1, replays, "其余必须是成功回放");
        assertEquals(1, sideEffects.get(), "副作用只能发生一次");
        assertEquals(0, instanceManager.capacityRejectionCount(),
                "同键等待者不得被容量拒绝");
    }

    @Test
    void idempotentKey_whenLeaderRejectedForCapacity_rejectionIsNotReplayedAsSuccess() throws Exception {
        // 1 实例 / 1 并发，先用“别的幂等键”把容量占满
        Gate gate = new Gate();
        assertTrue(runtime.publish("fn-ideo-rej", 1, statefulGateFactory(new AtomicInteger(), gate), 1, 1)
                .published());

        ExecutorService holder = Executors.newSingleThreadExecutor();
        Future<InvocationResult> holding = holder.submit(
                () -> runtime.invoke("fn-ideo-rej", 1, "GATE", "HOLDER"));
        gate.awaitEntered(1);
        Thread.sleep(100);

        // 容量已满：用一个新幂等键发起调用，领导者在容量判定处被拒。
        InvocationResult rejected = runtime.invoke("fn-ideo-rej", 1, "x", "K-NEW");
        log.info("[容量幂等] 领导者被拒 status={} success={} replayed={} 依据=容量到顶的执行前拒绝",
                rejected.status(), rejected.success(), rejected.replayed());
        assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, rejected.status());
        assertFalse(rejected.success(), "容量拒绝绝不能算成功");

        // 同一键再来一个并发等待者：拒绝结果要么被它回放（驱逐前命中），
        // 要么它在键驱逐后成为新领导者、再次吃一次容量拒绝（驱逐后到达）。
        // 两种路径都必须呈现拒绝状态且不算成功；这里记录分类即可，不强行固定时序。
        ExecutorService waiterPool = Executors.newSingleThreadExecutor();
        Future<InvocationResult> waiterFuture = waiterPool.submit(
                () -> runtime.invoke("fn-ideo-rej", 1, "x", "K-NEW"));
        InvocationResult waiterView = waiterFuture.get(5, TimeUnit.SECONDS);
        waiterPool.shutdown();
        log.info("[容量幂等] 同键等待者 status={} success={} replayed={} originStatus={} 判定路径={}",
                waiterView.status(), waiterView.success(), waiterView.replayed(), waiterView.originStatus(),
                waiterView.replayed() ? "驱逐前命中拒绝回放" : "驱逐后成为新领导者再次被拒");
        assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, waiterView.status(),
                "无论回放还是新拒绝，都必须呈现拒绝状态，不能伪装成成功回放");
        assertFalse(waiterView.success());
        if (waiterView.replayed()) {
            assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, waiterView.originStatus());
        }

        // 容量释放后，同键再次调用必须能真正执行成功（拒绝没有永久占用幂等键）
        gate.open();
        holding.get(5, TimeUnit.SECONDS);
        holder.shutdown();
        InvocationResult retry = runtime.invoke("fn-ideo-rej", 1, "go", "K-NEW");
        log.info("[容量幂等] 释放后同键重试 status={} value={} 判定路径=拒绝已驱逐键，重新真正执行",
                retry.status(), retry.value());
        assertEquals(InvocationStatus.SUCCESS, retry.status());
        assertFalse(retry.replayed());
    }

    @Test
    void distinctKeys_atCapacity_areRejected_notConfusedWithReplay() throws Exception {
        // 多个不同幂等键打满容量：被拒的必须是拒绝状态，不能被当成任何成功结果的回放
        Gate gate = new Gate();
        assertTrue(runtime.publish("fn-distinct", 1, statefulGateFactory(new AtomicInteger(), gate), 1, 1)
                .published());

        ExecutorService pool = Executors.newFixedThreadPool(1);
        Future<InvocationResult> holding = pool.submit(
                () -> runtime.invoke("fn-distinct", 1, "GATE", "K-HOLD"));
        gate.awaitEntered(1);
        Thread.sleep(100);

        for (int i = 0; i < 5; i++) {
            String key = "K-DIFFERENT-" + i;
            InvocationResult r = runtime.invoke("fn-distinct", 1, "x", key);
            log.info("[容量幂等] 不同键 #{} key={} status={} success={} replayed={}",
                    i, key, r.status(), r.success(), r.replayed());
            assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, r.status(),
                    "不同键在容量到顶时必须被明确拒绝");
            assertFalse(r.success());
            assertFalse(r.replayed(), "不同键不可能命中他人的回放");
        }
        gate.open();
        holding.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(5, instanceManager.capacityRejectionCount());
    }

    @Test
    void legacyFunction_withoutCapacityConfig_keepsOldConcurrencyBehavior() throws Exception {
        // 老发布方式（无容量配置）：行为不变——函数级并发信号量，超限是 CONCURRENCY_LIMIT_EXCEEDED
        runtime.setMaxConcurrencyPerFunction(1);
        Gate gate = new Gate();
        // 老的三参发布，处理器共享；用 GATE 输入阻塞
        assertTrue(runtime.publish("fn-legacy", 1, input -> {
            if ("GATE".equals(input)) {
                gate.signalEntered();
                assertTrue(gate.pass.await(10, TimeUnit.SECONDS));
            }
            return "legacy";
        }).published());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<InvocationResult> holding = pool.submit(
                () -> runtime.invoke("fn-legacy", 1, "GATE", null));
        gate.awaitEntered(1);
        Thread.sleep(100);

        InvocationResult rejected = runtime.invoke("fn-legacy", 1, "overflow", null);
        log.info("[容量幂等] 遗留函数超限 status={} 判定路径=函数级并发到顶", rejected.status());
        assertEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, rejected.status());
        assertFalse(rejected.success());

        // 遗留函数拒绝与幂等键同样不永久占坑
        InvocationResult rejectedKey = runtime.invoke("fn-legacy", 1, "overflow", "K-LEGACY");
        assertEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, rejectedKey.status());
        assertFalse(rejectedKey.success());

        gate.open();
        holding.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        InvocationResult retry = runtime.invoke("fn-legacy", 1, "go", "K-LEGACY");
        assertEquals(InvocationStatus.SUCCESS, retry.status(), "释放后同键必须能真正执行");
        log.info("[容量幂等] 遗留函数释放后同键重试 status={} value={}", retry.status(), retry.value());
    }

    // ------------------------------------------------------------------
    // 场景四：故障淘汰与替换并发进行，调用不丢、不重、不被两个实例同时执行
    // ------------------------------------------------------------------
    @Test
    void evictionChurn_neverLosesOrDuplicatesInvocations() throws Exception {
        // 2 实例 / 每实例 1 并发。处理器按调用序号交替失败，
        // 失败实例立即淘汰、下一次调用冷启动替换；大量并发调用下验证账实相符。
        int totalCalls = 200;
        int maxWarm = 2;
        AtomicInteger attempts = new AtomicInteger(0);    // 真正进入处理器的次数
        AtomicInteger succeeded = new AtomicInteger(0);   // 处理器正常返回次数
        AtomicInteger instances = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-churn", 1,
                (FunctionHandlerFactory) () -> {
                    int instanceId = instances.incrementAndGet();
                    return input -> {
                        int seq = attempts.incrementAndGet();
                        if (seq % 3 == 0) {
                            throw new IllegalStateException("forced failure seq=" + seq
                                    + " instance=" + instanceId);
                        }
                        succeeded.incrementAndGet();
                        return seq;
                    };
                }, maxWarm, 1).published());

        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < totalCalls; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return runtime.invoke("fn-churn", 1, "x", null);
            }));
        }
        go.countDown();

        int success = 0;
        int capacityRejected = 0;
        int handlerErrors = 0;
        int cold = 0;
        java.util.Set<String> instanceIds = new java.util.HashSet<>();
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(30, TimeUnit.SECONDS);
            switch (r.status()) {
                case SUCCESS -> {
                    success++;
                    instanceIds.add(r.instanceId());
                }
                case INSTANCE_CAPACITY_EXCEEDED -> capacityRejected++;
                case HANDLER_ERROR -> {
                    handlerErrors++;
                    instanceIds.add(r.instanceId());
                }
                default -> throw new AssertionError("不允许出现其他状态：" + r.status());
            }
            if (r.coldStart()) {
                cold++;
            }
        }
        pool.shutdown();

        log.info("[淘汰替换] 总调用={} 成功={} 执行异常={} 容量拒绝={} 冷启动={} 涉及实例数={} 指标={}",
                totalCalls, success, handlerErrors, capacityRejected, cold,
                instanceIds.size(), runtime.metrics());

        // 1) 不丢不重：每个调用要么真正进入处理器（成功或异常），要么执行前被容量明确拒绝，
        //    二者之和必须严格等于调用总数
        assertEquals(totalCalls, attempts.get() + capacityRejected,
                "调用不允许丢失或重复：进入处理器次数 + 容量拒绝次数必须等于调用总数");
        // 2) 进入处理器的调用中：成功返回 + 抛异常 = 进入次数，每个调用有且仅有一个执行归宿
        assertEquals(attempts.get(), succeeded.get() + handlerErrors,
                "每次进入处理器的调用要么成功要么异常，不允许重复执行");
        assertEquals(attempts.get(), success + handlerErrors,
                "结果分类侧的成功数与异常数之和也必须等于进入处理器次数");
        // 3) 容量到顶会拒绝（2 实例 × 1 并发，12 线程），拒绝是明确状态而非丢失
        assertTrue(capacityRejected > 0, "高并发下应出现容量拒绝");
        // 4) 淘汰确实在发生：冷启动数 > 2（故障实例不断被替换），但健康实例数始终不超上限
        assertTrue(cold > maxWarm, "淘汰替换应触发多次冷启动，实际冷启动=" + cold);
        assertTrue(runtime.activeInstances() <= maxWarm,
                "健康实例数不得超过上限，实际=" + runtime.activeInstances());
        // 5) draining 归零（所有在飞调用最终结束）
        assertEquals(0, instanceManager.drainingInstances(), "替换结束后不应残留 draining 实例");
        assertTrue(instanceManager.evictedCount() > 0, "应发生故障淘汰");
        log.info("[淘汰替换] 判定通过：淘汰实例数={} 健康实例数={}",
                instanceManager.evictedCount(), runtime.activeInstances());
    }

    // ------------------------------------------------------------------
    // 场景五：发布新版本不影响老版本已在运行的实例与行为
    // ------------------------------------------------------------------
    @Test
    void publishingNewVersion_doesNotDisturbOldVersionInstances() throws Exception {
        AtomicInteger createdV1 = new AtomicInteger(0);
        assertTrue(runtime.publish("fn-ver", 1,
                (FunctionHandlerFactory) () -> {
                    createdV1.incrementAndGet();
                    AtomicInteger c = new AtomicInteger(0);
                    return input -> "v1-" + c.incrementAndGet();
                }, 1, 1).published());

        // v1 跑起来，拿到暖实例
        InvocationResult v1a = runtime.invoke("fn-ver", 1, "x", null);
        InvocationResult v1b = runtime.invoke("fn-ver", 1, "x", null);
        assertEquals(InvocationStatus.SUCCESS, v1a.status());
        assertEquals(v1a.instanceId(), v1b.instanceId(), "v1 暖实例复用");
        assertEquals("v1-1", v1a.value());
        assertEquals("v1-2", v1b.value(), "v1 实例保留自身状态");
        log.info("[版本隔离] v1 实例={} 计数 {},{}", v1a.instanceId(), v1a.value(), v1b.value());

        // 发布 v2：不同处理器、不同容量（2 实例）
        assertTrue(runtime.publish("fn-ver", 2,
                (FunctionHandlerFactory) () -> {
                    AtomicInteger c = new AtomicInteger(0);
                    return input -> "v2-" + c.incrementAndGet();
                }, 2, 1).published());

        // 不指定版本 => 当前生效版本 v2；指定版本 1 => 老版本
        InvocationResult v2 = runtime.invoke("fn-ver", 0, "x", null);
        InvocationResult v1After = runtime.invoke("fn-ver", 1, "x", null);
        log.info("[版本隔离] 发布v2后：v2调用 value={} 实例={}；v1调用 value={} 实例={}",
                v2.value(), v2.instanceId(), v1After.value(), v1After.instanceId());
        assertEquals("v2-1", v2.value());
        assertNotEquals(v1a.instanceId(), v2.instanceId(), "v2 必须使用独立的新实例池");
        assertEquals("v1-3", v1After.value(),
                "老版本继续按发布时行为服务，且沿用旧实例的累积状态");
        assertEquals(v1a.instanceId(), v1After.instanceId(),
                "发布新版本不得淘汰/替换老版本已在运行的实例");

        // 池维度指标：两个版本各有自己的实例
        Map<String, Integer> pools = instanceManager.poolSizes();
        log.info("[版本隔离] 各版本实例池={}", pools);
        assertEquals(1, pools.get("fn-ver:1"), "v1 池仍保留 1 个实例");
        assertTrue(pools.get("fn-ver:2") >= 1, "v2 池应已有实例");

        // 重复发布 v1 仍被拒（历史版本不可覆盖）
        assertEquals(com.github.xingjianluo.faasruntimejava.faas.PublishStatus.DUPLICATE_VERSION,
                runtime.publish("fn-ver", 1, input -> "overwrite").status());
    }
}
