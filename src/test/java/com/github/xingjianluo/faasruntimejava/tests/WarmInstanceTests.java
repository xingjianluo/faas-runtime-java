package com.github.xingjianluo.faasruntimejava.tests;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionInvoker;
import com.github.xingjianluo.faasruntimejava.faas.FunctionRegistry;
import com.github.xingjianluo.faasruntimejava.faas.IdempotencyStore;
import com.github.xingjianluo.faasruntimejava.faas.InstanceContext;
import com.github.xingjianluo.faasruntimejava.faas.InstanceManager;
import com.github.xingjianluo.faasruntimejava.faas.InvocationResult;
import com.github.xingjianluo.faasruntimejava.faas.InvocationStatus;
import com.github.xingjianluo.faasruntimejava.faas.StatefulFunctionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 暖实例增强场景：实例级状态隔离、故障实例淘汰、容量×幂等交叉分类、
 * 淘汰替换过程中不丢不重。日志打印实例 ID 与判定路径。
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

    /** 把输入追加进实例私有状态，返回实例 ID 与当前状态快照 */
    private static StatefulFunctionHandler statefulHandler() {
        return new StatefulFunctionHandler() {
            @Override
            public Object initInstanceState() {
                return new ArrayList<String>();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object handle(Object input, InstanceContext context) {
                List<String> state = (List<String>) context.state();
                state.add(String.valueOf(input));
                Map<String, Object> snapshot = new HashMap<>();
                snapshot.put("instanceId", context.instanceId());
                snapshot.put("state", new ArrayList<>(state));
                return snapshot;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> value(InvocationResult r) {
        return (Map<String, Object>) r.value();
    }

    // ------------------------------------------------------------------
    // 场景一：实例级状态隔离 + 新版本不影响老版本实例
    // ------------------------------------------------------------------
    @Test
    @SuppressWarnings("unchecked")
    void instanceStatesAreIsolated_andNewVersionKeepsOldInstances() throws Exception {
        // 屏障钉在执行体内部：确保首轮 3 个调用真正在处理中重叠，
        // 从而强制占用 3 个不同实例（避免调度抖动导致快调用先释放槽位被复用）
        var barrier = new java.util.concurrent.CyclicBarrier(3);
        var barrier2 = new java.util.concurrent.CyclicBarrier(3);
        StatefulFunctionHandler handler = new StatefulFunctionHandler() {
            @Override
            public Object initInstanceState() {
                return new ArrayList<String>();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object handle(Object input, InstanceContext ctx) throws Exception {
                List<String> state = (List<String>) ctx.state();
                state.add(String.valueOf(input));
                if (Set.of("a0", "a1", "a2").contains(String.valueOf(input))) {
                    barrier.await(10, TimeUnit.SECONDS);
                }
                if (Set.of("b0", "b1", "b2").contains(String.valueOf(input))) {
                    barrier2.await(10, TimeUnit.SECONDS);
                }
                Map<String, Object> snapshot = new HashMap<>();
                snapshot.put("instanceId", ctx.instanceId());
                snapshot.put("state", new ArrayList<>(state));
                return snapshot;
            }
        };
        // 3 个暖实例、单实例并发 1：同一函数同一版本可同时持有 3 个独立实例
        assertTrue(runtime.publish("fn-state", 1, handler, 3, 1).published());

        int n = 3;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> round1 = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String input = "a" + i;
            round1.add(pool.submit(() -> {
                go.await();
                return runtime.invoke("fn-state", 1, input, null);
            }));
        }
        go.countDown();

        Set<String> firstRoundInstances = new HashSet<>();
        Set<String> allStateItems = new HashSet<>();
        for (Future<InvocationResult> f : round1) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            assertEquals(InvocationStatus.SUCCESS, r.status());
            assertTrue(r.coldStart(), "首轮并发调用应分别冷启动独立实例");
            Map<String, Object> v = value(r);
            String instanceId = (String) v.get("instanceId");
            List<String> state = (List<String>) v.get("state");
            log.info("[状态隔离] 冷启动 invocationId={} 实例={} 私有状态={} 判定路径=新实例空状态",
                    r.invocationId(), instanceId, state);
            assertTrue(firstRoundInstances.add(instanceId), "三个实例必须互不相同");
            assertEquals(1, state.size(), "新实例不能看到别的实例留下的状态");
            allStateItems.addAll(state);
        }
        assertEquals(Set.of("a0", "a1", "a2"), allStateItems);
        assertEquals(3, instanceManager.activeInstances());

        // 第二轮 3 个并发：必须复用原 3 个实例（暖），且每个实例只积累自己的状态
        CountDownLatch go2 = new CountDownLatch(1);
        List<Future<InvocationResult>> round2 = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String input = "b" + i;
            round2.add(pool.submit(() -> {
                go2.await();
                return runtime.invoke("fn-state", 1, input, null);
            }));
        }
        go2.countDown();
        Set<String> secondRoundInstances = new HashSet<>();
        for (Future<InvocationResult> f : round2) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            assertEquals(InvocationStatus.SUCCESS, r.status());
            assertFalse(r.coldStart(), "第二轮必须复用暖实例，不能冷启动");
            Map<String, Object> v = value(r);
            String instanceId = (String) v.get("instanceId");
            List<String> state = (List<String>) v.get("state");
            log.info("[状态隔离] 暖复用 invocationId={} 实例={} 私有状态={}",
                    r.invocationId(), instanceId, state);
            assertTrue(secondRoundInstances.add(instanceId));
            assertEquals(2, state.size(), "该实例应只积累过自己处理的两次调用");
        }
        pool.shutdown();
        assertEquals(firstRoundInstances, secondRoundInstances, "第二轮必须复用第一轮的实例");

        // 发布新版本：老版本实例必须原样保留并继续按发布时行为服务
        assertTrue(runtime.publish("fn-state", 2, statefulHandler(), 2, 1).published());
        InvocationResult v2call = runtime.invoke("fn-state", 2, "v2-only", null);
        assertEquals(InvocationStatus.SUCCESS, v2call.status());
        assertTrue(v2call.instanceId().contains(":2#"), "新版本必须使用版本隔离的实例");
        assertEquals(List.of("v2-only"), value(v2call).get("state"));

        Map<String, List<String>> details = instanceManager.activeInstanceDetails();
        log.info("[状态隔离] 发布 v2 后实例分布={}", details);
        assertTrue(details.containsKey("fn-state:1"), "老版本实例池必须保留");
        assertTrue(details.containsKey("fn-state:2"), "新版本实例池必须已建立");
        assertEquals(3, details.get("fn-state:1").size(), "老版本 3 个实例一个都不能受影响");

        // 老版本继续可用：复用老实例、状态延续（该实例状态变为 3 条），不带入新版本的任何东西
        InvocationResult oldAgain = runtime.invoke("fn-state", 1, "a-old", null);
        assertEquals(InvocationStatus.SUCCESS, oldAgain.status());
        assertFalse(oldAgain.coldStart());
        assertTrue(firstRoundInstances.contains(oldAgain.instanceId()),
                "老版本必须继续复用发布时就在运行的实例");
        List<String> oldState = (List<String>) value(oldAgain).get("state");
        assertEquals(3, oldState.size());
        assertFalse(oldState.contains("v2-only"), "老版本实例不能看到新版本的数据");
    }

    // ------------------------------------------------------------------
    // 场景二：故障实例淘汰，后续调用不再落到它身上
    // ------------------------------------------------------------------
    @Test
    @SuppressWarnings("unchecked")
    void faultyInstancesAreEvictedAndNeverReused() {
        // 输入以 "boom" 开头即抛异常；带 sleep 前缀则超时
        StatefulFunctionHandler flaky = new StatefulFunctionHandler() {
            @Override
            public Object initInstanceState() {
                return new java.util.concurrent.atomic.AtomicInteger(0);
            }

            @Override
            public Object handle(Object input, InstanceContext ctx) throws Exception {
                String s = String.valueOf(input);
                if (s.startsWith("boom")) {
                    throw new IllegalStateException("intentional boom on " + ctx.instanceId());
                }
                if (s.startsWith("hang")) {
                    Thread.sleep(60_000);
                }
                ((java.util.concurrent.atomic.AtomicInteger) ctx.state()).incrementAndGet();
                Map<String, Object> ok = new HashMap<>();
                ok.put("instanceId", ctx.instanceId());
                ok.put("handled", ((java.util.concurrent.atomic.AtomicInteger) ctx.state()).get());
                return ok;
            }
        };
        runtime.setInvokeTimeoutMillis(200);
        // 2 个实例、每实例并发 1
        assertTrue(runtime.publish("fn-flaky", 1, flaky, 2, 1).published());

        InvocationResult c1 = runtime.invoke("fn-flaky", 1, "boom-1", null);
        assertEquals(InvocationStatus.HANDLER_ERROR, c1.status());
        String badInstance1 = c1.instanceId();
        log.info("[故障淘汰] 异常调用 invocationId={} 实例={} status={} 判定路径=HANDLER_ERROR->淘汰",
                c1.invocationId(), badInstance1, c1.status());

        InvocationResult c2 = runtime.invoke("fn-flaky", 1, "boom-2", null);
        assertEquals(InvocationStatus.HANDLER_ERROR, c2.status());
        String badInstance2 = c2.instanceId();
        log.info("[故障淘汰] 异常调用 invocationId={} 实例={} status={} 判定路径=HANDLER_ERROR->淘汰",
                c2.invocationId(), badInstance2, c2.status());

        // 两个实例先后被淘汰：ID 不同，淘汰计数为 2，池为空
        assertFalse(badInstance1.equals(badInstance2));
        assertEquals(0, instanceManager.activeInstances());
        assertEquals(2, instanceManager.evictionCount());
        for (List<String> ids : instanceManager.activeInstanceDetails().values()) {
            assertFalse(ids.contains(badInstance1));
            assertFalse(ids.contains(badInstance2));
        }

        // 后续调用：冷启动全新实例，ID 绝不能是被淘汰者；该实例状态从 0 开始
        InvocationResult ok1 = runtime.invoke("fn-flaky", 1, "ok-1", null);
        assertEquals(InvocationStatus.SUCCESS, ok1.status());
        assertTrue(ok1.coldStart());
        String healthy = ok1.instanceId();
        log.info("[故障淘汰] 替换调用 invocationId={} 新实例={} 判定路径=池空冷启动",
                ok1.invocationId(), healthy);
        assertFalse(healthy.equals(badInstance1));
        assertFalse(healthy.equals(badInstance2));
        assertEquals(1, value(ok1).get("handled"));

        // 超时同样淘汰：新调用落在健康实例或冷启动，但绝不是被超时的实例
        InvocationResult timeout = runtime.invoke("fn-flaky", 1, "hang", null);
        assertEquals(InvocationStatus.TIMEOUT, timeout.status());
        String timedOutInstance = timeout.instanceId();
        assertEquals(healthy, timedOutInstance, "唯一健康实例承接了该调用");
        log.info("[故障淘汰] 超时调用 invocationId={} 实例={} status={} 判定路径=TIMEOUT->淘汰",
                timeout.invocationId(), timedOutInstance, timeout.status());
        assertEquals(0, instanceManager.activeInstances());
        assertEquals(3, instanceManager.evictionCount());

        InvocationResult ok2 = runtime.invoke("fn-flaky", 1, "ok-2", null);
        assertEquals(InvocationStatus.SUCCESS, ok2.status());
        assertTrue(ok2.coldStart(), "健康实例已被淘汰，必须再次冷启动");
        String replaced = ok2.instanceId();
        assertFalse(replaced.equals(timedOutInstance));
        log.info("[故障淘汰] 替换调用 invocationId={} 新实例={} 判定路径=超时淘汰后冷启动",
                ok2.invocationId(), replaced);
    }

    // ------------------------------------------------------------------
    // 场景三：容量打满时，并发与幂等交叉的结果分类必须分得清
    // ------------------------------------------------------------------
    @Test
    void capacityRejectionVsIdempotentReplay_areClassifiedDistinctly() throws Exception {
        // 2 实例 * 每实例并发 1 = 总在飞容量 2，到顶立即拒绝
        var enter = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var sideEffects = new java.util.concurrent.atomic.AtomicInteger(0);
        StatefulFunctionHandler handler = new StatefulFunctionHandler() {
            @Override
            public Object initInstanceState() {
                return "fresh-state";
            }

            @Override
            public Object handle(Object input, InstanceContext ctx) throws Exception {
                String s = String.valueOf(input);
                if (s.equals("hold")) {
                    enter.countDown();
                    release.await();
                    return "released";
                }
                sideEffects.incrementAndGet();
                Map<String, Object> ok = new HashMap<>();
                ok.put("instanceId", ctx.instanceId());
                ok.put("receipt", "receipt-for-" + s);
                return ok;
            }
        };
        assertTrue(runtime.publish("fn-cap", 1, handler, 2, 1).published());

        // 占满 2 个容量槽
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<InvocationResult>> holders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            holders.add(pool.submit(() -> runtime.invoke("fn-cap", 1, "hold", null)));
        }
        assertTrue(enter.await(5, TimeUnit.SECONDS));

        // 无幂等：到顶立即拒绝，明确状态、不算成功、不携带实例/返回值
        InvocationResult plainReject = runtime.invoke("fn-cap", 1, "x", null);
        log.info("[容量幂等] 无键超限 invocationId={} status={} 判定路径=实例池到顶",
                plainReject.invocationId(), plainReject.status());
        assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, plainReject.status());
        assertFalse(plainReject.success(), "容量被拒不能算成功");
        assertEquals(null, plainReject.instanceId());
        assertEquals(null, plainReject.value());

        // 同幂等键 5 个并发：容量到顶 -> 1 个首领被拒 + 4 个等待者拿到“原始被拒状态”，
        // 全部不是 IDEMPOTENT_REPLAY、全部不算成功，且一次都没执行（副作用 0）
        int callers = 5;
        ExecutorService burst = Executors.newFixedThreadPool(callers);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            futures.add(burst.submit(() -> {
                go.await();
                return runtime.invoke("fn-cap", 1, "order-X", "key-full");
            }));
        }
        go.countDown();
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            log.info("[容量幂等] 同键满容量并发 invocationId={} status={} success={} 判定路径=被拒原样传播",
                    r.invocationId(), r.status(), r.success());
            assertEquals(InvocationStatus.INSTANCE_CAPACITY_EXCEEDED, r.status(),
                    "被拒不能被伪装成成功回放");
            assertFalse(r.success());
            assertNotReplay(r);
        }
        burst.shutdown();
        assertEquals(0, sideEffects.get(), "满容量下该幂等键一次都不应执行");

        // 释放容量后，同一幂等键必须能真正执行一次（被拒时键已被驱逐）
        release.countDown();
        for (Future<InvocationResult> f : holders) {
            assertEquals(InvocationStatus.SUCCESS, f.get(10, TimeUnit.SECONDS).status());
        }
        pool.shutdown();

        InvocationResult real = runtime.invoke("fn-cap", 1, "order-X", "key-full");
        log.info("[容量幂等] 容量释放后同键 invocationId={} status={} instance={} 判定路径=驱逐后重新执行",
                real.invocationId(), real.status(), real.instanceId());
        assertEquals(InvocationStatus.SUCCESS, real.status());
        assertEquals("receipt-for-order-X", value(real).get("receipt"));
        assertEquals(1, sideEffects.get());

        // 真正命中回放：保留 IDEMPOTENT_REPLAY 可识别性，算成功，结果与实例标识一致
        InvocationResult replay = runtime.invoke("fn-cap", 1, "order-X", "key-full");
        log.info("[容量幂等] 回放 invocationId={} status={} instance={} 判定路径=命中已完成幂等键",
                replay.invocationId(), replay.status(), replay.instanceId());
        assertEquals(InvocationStatus.IDEMPOTENT_REPLAY, replay.status());
        assertTrue(replay.success());
        assertEquals(real.value(), replay.value());
        assertEquals(real.instanceId(), replay.instanceId());
        assertEquals(1, sideEffects.get(), "回放不重复执行");

        // 空闲容量下另一个键的并发：恰好 1 次执行，其余全是可识别的成功回放
        int n = 6;
        ExecutorService burst2 = Executors.newFixedThreadPool(n);
        CountDownLatch go2 = new CountDownLatch(1);
        List<Future<InvocationResult>> fs2 = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs2.add(burst2.submit(() -> {
                go2.await();
                return runtime.invoke("fn-cap", 1, "order-Y", "key-ok");
            }));
        }
        go2.countDown();
        int successes = 0, replays = 0;
        for (Future<InvocationResult> f : fs2) {
            InvocationResult r = f.get(10, TimeUnit.SECONDS);
            if (r.status() == InvocationStatus.SUCCESS) {
                successes++;
            } else {
                assertEquals(InvocationStatus.IDEMPOTENT_REPLAY, r.status());
                assertTrue(r.success());
                replays++;
            }
        }
        burst2.shutdown();
        log.info("[容量幂等] 空闲容量同键并发：执行成功={} 回放={} 副作用总数={}",
                successes, replays, sideEffects.get());
        assertEquals(1, successes);
        assertEquals(n - 1, replays);
        assertEquals(2, sideEffects.get(), "key-full 与 key-ok 各执行一次");

        // 未配置容量的老函数行为不变：拒绝类别仍是 CONCURRENCY_LIMIT_EXCEEDED，
        // 且在幂等交叉时同样原样传播、不算成功
        runtime.setMaxConcurrencyPerFunction(1);
        var enter2 = new CountDownLatch(1);
        var release2 = new CountDownLatch(1);
        assertTrue(runtime.publish("fn-legacy", 1, input -> {
            if ("hold".equals(input)) {
                enter2.countDown();
                release2.await();
            }
            return input;
        }).published());
        ExecutorService one = Executors.newSingleThreadExecutor();
        Future<InvocationResult> holder = one.submit(() -> runtime.invoke("fn-legacy", 1, "hold", null));
        assertTrue(enter2.await(5, TimeUnit.SECONDS));
        InvocationResult legacyReject = runtime.invoke("fn-legacy", 1, "z", "legacy-key");
        log.info("[容量幂等] 老函数超限 invocationId={} status={} 判定路径=函数级信号量",
                legacyReject.invocationId(), legacyReject.status());
        assertEquals(InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED, legacyReject.status());
        assertFalse(legacyReject.success());
        assertNotReplay(legacyReject);
        release2.countDown();
        assertEquals(InvocationStatus.SUCCESS, holder.get(10, TimeUnit.SECONDS).status());
        one.shutdown();
    }

    private static void assertNotReplay(InvocationResult r) {
        org.junit.jupiter.api.Assertions.assertNotEquals(InvocationStatus.IDEMPOTENT_REPLAY,
                r.status(), "被拒结果不能带回放状态");
    }

    // ------------------------------------------------------------------
    // 场景四：故障淘汰与替换并发发生时，调用不丢、不重、实例数有上限
    // ------------------------------------------------------------------
    @Test
    @SuppressWarnings("unchecked")
    void evictionAndReplacementUnderConcurrency_neverLosesOrDuplicates() throws Exception {
        runtime.setInvokeTimeoutMillis(300);
        int maxWarm = 3;
        int perInstance = 2;
        var successExecutions = new java.util.concurrent.atomic.AtomicInteger(0);
        var failedExecutions = new java.util.concurrent.atomic.AtomicInteger(0);
        StatefulFunctionHandler worker = new StatefulFunctionHandler() {
            @Override
            public Object initInstanceState() {
                return new java.util.concurrent.atomic.AtomicInteger(0);
            }

            @Override
            public Object handle(Object input, InstanceContext ctx) throws Exception {
                String s = String.valueOf(input);
                if (s.startsWith("boom")) {
                    failedExecutions.incrementAndGet();
                    throw new IllegalStateException("forced failure");
                }
                if (s.startsWith("slow")) {
                    failedExecutions.incrementAndGet();
                    Thread.sleep(2_000); // 超时
                    return null;
                }
                int seen = ((java.util.concurrent.atomic.AtomicInteger) ctx.state())
                        .incrementAndGet();
                successExecutions.incrementAndGet();
                Map<String, Object> ok = new HashMap<>();
                ok.put("instanceId", ctx.instanceId());
                ok.put("seqInInstance", seen);
                return ok;
            }
        };
        assertTrue(runtime.publish("fn-churn", 1, worker, maxWarm, perInstance).published());

        int callers = 24;
        int rounds = 8;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<InvocationResult>> futures = new ArrayList<>();
        for (int r = 0; r < rounds; r++) {
            for (int c = 0; c < callers; c++) {
                int tag = r * callers + c;
                futures.add(pool.submit(() -> {
                    go.await();
                    // 错落到达，且约 1/4 是故障调用，制造持续淘汰/替换
                    Thread.sleep((tag % 6) * 5L);
                    String input = (tag % 4 == 0) ? "boom-" + tag
                            : (tag % 7 == 0) ? "slow-" + tag : "ok-" + tag;
                    return runtime.invoke("fn-churn", 1, input, null);
                }));
            }
        }
        go.countDown();

        int totalCalls = rounds * callers;
        int success = 0;
        int handlerErrors = 0;
        int timeouts = 0;
        int rejected = 0;
        int cold = 0;
        Set<String> seenInstanceIds = new HashSet<>();
        for (Future<InvocationResult> f : futures) {
            InvocationResult r = f.get(30, TimeUnit.SECONDS);
            switch (r.status()) {
                case SUCCESS -> {
                    success++;
                    seenInstanceIds.add(r.instanceId());
                    if (r.coldStart()) {
                        cold++;
                    }
                }
                case HANDLER_ERROR -> handlerErrors++;
                case TIMEOUT -> timeouts++;
                case INSTANCE_CAPACITY_EXCEEDED -> rejected++;
                default -> throw new AssertionError("不允许出现未分类结果: " + r.status());
            }
        }
        pool.shutdown();

        log.info("[淘汰替换] 总调用={} 成功={} 异常={} 超时={} 容量拒绝={} 冷启动={}",
                totalCalls, success, handlerErrors, timeouts, rejected, cold);

        // 判定一：每个调用都有确定分类，加起来严格等于总调用数——不丢
        assertEquals(totalCalls, success + handlerErrors + timeouts + rejected,
                "所有调用必须有确定结果，不能丢失");
        // 判定二：真正执行体运行次数 == 成功 + 异常 + 超时；同一调用绝不执行两次——不重
        assertEquals(success, successExecutions.get(), "成功执行体次数必须与成功调用数相等");
        assertEquals(handlerErrors + timeouts, failedExecutions.get(),
                "失败执行体次数必须与失败调用数相等");
        // 判定三：活跃实例数任何时候都不超过版本上限（此刻）
        assertTrue(instanceManager.activeInstances() <= maxWarm,
                "实例数不能超过上限，实际=" + instanceManager.activeInstances());
        // 判定四：故障期间确实发生过淘汰与冷启动替换，且被淘汰实例确实不在池中
        assertTrue(instanceManager.evictionCount() > 0, "应至少淘汰过一个故障实例");
        Set<String> stillAlive = new HashSet<>();
        instanceManager.activeInstanceDetails().values().forEach(stillAlive::addAll);
        log.info("[淘汰替换] 淘汰实例数={} 当前存活实例={}",
                instanceManager.evictionCount(), stillAlive);
        // 判定五：确实存在成功的暖复用（多实例并发池在淘汰之外也承接了调用）
        assertTrue(success > cold, "除冷启动外必须有暖实例复用成功");
    }
}
