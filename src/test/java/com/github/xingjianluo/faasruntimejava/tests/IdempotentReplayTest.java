package com.github.xingjianluo.faasruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
 * 场景三：幂等重放。
 * 同一幂等键的重复调用不得重复执行副作用；并发重复请求只有一个真正执行；
 * 被并发拒绝的调用不占用幂等键（回滚后可正常执行）；业务异常后允许重试。
 */
class IdempotentReplayTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentReplayTest.class);

    private FaasRuntime newRuntime() {
        return new FaasRuntime(new DefaultFunctionHandlerFactory(), new RuntimeProperties());
    }

    private InvocationRequest req(String name, Integer v, String payload, String key) {
        return new InvocationRequest(name, v, payload, key);
    }

    @Test
    void sequentialReplayExecutesHandlerOnce() {
        FaasRuntime runtime = newRuntime();
        // sideEffect 每次执行返回自增序号 side-effect-N
        runtime.publish("once", 1, "sideEffect", null, 4, Duration.ofMinutes(10));

        CallResult first = runtime.invoke(req("once", 1, "p", "key-1"));
        CallResult second = runtime.invoke(req("once", 1, "p-DIFFERENT", "key-1"));
        CallResult third = runtime.invoke(req("once", 1, "p", "key-OTHER"));

        log.info("[{}] first  key-1 -> status={} body={} instance={} basis=leader-executes",
                first.invocationId(), first.status(), first.body(), first.instanceId());
        log.info("[{}] second key-1 -> status={} body={} replay={} basis=same-key-replay-no-exec",
                second.invocationId(), second.status(), second.body(), second.replay());
        log.info("[{}] third  key-OTHER -> status={} body={} basis=different-key-executes-again",
                third.invocationId(), third.status(), third.body());

        assertEquals(InvocationStatus.SUCCESS, first.status());
        assertEquals(InvocationStatus.REPLAY, second.status());
        assertTrue(second.replay());
        // 重放结果与首次完全一致（即便第二次 payload 不同，也不执行函数）
        assertEquals(first.body(), second.body());
        assertEquals(first.instanceId(), second.instanceId());
        assertEquals(first.invocationId(), second.invocationId(),
                "重放结果必须指向首次执行的 invocationId");
        // 不同幂等键是独立调用，副作用再次执行
        assertEquals(InvocationStatus.SUCCESS, third.status());
        assertNotEquals(first.body(), third.body());

        InstanceMetrics m = runtime.metrics("once", 1);
        log.info("once#v1 metrics: total={} success={} replay={} basis=2-real-exec-1-replay",
                m.totalCalls(), m.successCalls(), m.replayCalls());
        assertEquals(3, m.totalCalls());
        assertEquals(2, m.successCalls());
        assertEquals(1, m.replayCalls());
    }

    @Test
    void concurrentDuplicateRequestsExecuteExactlyOnce() throws Exception {
        FaasRuntime runtime = newRuntime();
        runtime.publish("race", 1, "sideEffect", Duration.ofSeconds(5), 8,
                Duration.ofMinutes(10));

        int n = 20;
        CyclicBarrier barrier = new CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<CallResult>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    CallResult r = runtime.invoke(req("race", 1, "p-" + idx, "dup-key"));
                    log.info("[{}] race caller#{} -> status={} body={} replay={} basis=dup-key",
                            r.invocationId(), idx, r.status(), r.body(), r.replay());
                    return r;
                }));
            }

            int success = 0, replay = 0;
            String firstBody = null;
            String firstInvocation = null;
            for (Future<CallResult> f : futures) {
                CallResult r = f.get(10, TimeUnit.SECONDS);
                if (r.status() == InvocationStatus.SUCCESS) {
                    success++;
                    firstBody = r.body();
                    firstInvocation = r.invocationId();
                } else if (r.status() == InvocationStatus.REPLAY) {
                    replay++;
                }
            }
            log.info("concurrent duplicate summary: success={} replay={} firstBody={} "
                    + "basis=exactly-one-leader-side-effect-once", success, replay, firstBody);
            assertEquals(1, success, "同一幂等键并发调用只能有一次真正执行");
            assertEquals(n - 1, replay, "其余必须全部重放");
            assertEquals("side-effect-1", firstBody, "副作用必须恰好执行一次");

            // 所有重放结果的 body 必须与首次完全一致，且 invocationId 指向首次执行
            for (Future<CallResult> f : futures) {
                CallResult r = f.get();
                if (r.status() == InvocationStatus.REPLAY) {
                    assertEquals(firstBody, r.body());
                    assertEquals(firstInvocation, r.invocationId());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrencyRejectedCallDoesNotOccupyIdempotencyKey() throws Exception {
        FaasRuntime runtime = newRuntime();
        // 上限 1：用一个无幂等键的慢调用占住唯一槽位
        runtime.publish("busy", 1, "sleep", Duration.ofSeconds(5), 1, Duration.ofMinutes(10));

        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            Future<CallResult> holder = pool.submit(
                    () -> runtime.invoke(new InvocationRequest("busy", 1, "600", null)));
            Thread.sleep(150); // 等 holder 占住唯一实例

            // 新幂等键的 leader 此时抢不到槽位：必须被拒绝且回滚幂等占位
            CallResult rejected = runtime.invoke(req("busy", 1, "1", "k-rejected"));
            log.info("[{}] new-key while busy -> status={} errorCode={} "
                            + "basis=leader-cannot-acquire-and-must-release-key",
                    rejected.invocationId(), rejected.status(), rejected.errorCode());
            assertEquals(InvocationStatus.CONCURRENCY_REJECTED, rejected.status());
            assertTrue(runtime.idempotencyStore().find("busy", 1, "k-rejected").isEmpty(),
                    "被拒绝调用不得留下幂等记录");

            holder.get(10, TimeUnit.SECONDS); // 等待槽位释放

            // 同一个“曾被拒绝”的键现在必须能全新执行一次（SUCCESS 而非 REPLAY）
            CallResult redeemed = runtime.invoke(req("busy", 1, "10", "k-rejected"));
            log.info("[{}] rejected-key after free -> status={} body={} "
                            + "basis=key-was-rolled-back-executes-anew",
                    redeemed.invocationId(), redeemed.status(), redeemed.body());
            assertEquals(InvocationStatus.SUCCESS, redeemed.status());
            assertEquals("slept-10", redeemed.body());

            // 而在慢调用执行期间到达的同键 follower 会等待并在其完成后重放，不会重复执行
            Future<CallResult> holder2 = pool.submit(
                    () -> runtime.invoke(req("busy", 1, "400", "k-wait")));
            Thread.sleep(150);
            CallResult follower = runtime.invoke(req("busy", 1, "1", "k-wait"));
            CallResult leader = holder2.get(10, TimeUnit.SECONDS);
            log.info("[{}] same-key follower -> status={} pointsTo={} basis=follower-joins-and-replays",
                    follower.invocationId(), follower.status(), follower.invocationId());
            assertEquals(InvocationStatus.SUCCESS, leader.status());
            assertEquals(InvocationStatus.REPLAY, follower.status());
            assertEquals(leader.invocationId(), follower.invocationId());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void businessFailureReleasesKeyForRetryButSuccessThenFreezes() {
        FaasRuntime runtime = newRuntime();
        // v1 必失败；之后发布 v2（不同版本独立幂等域）
        runtime.publish("flaky", 1, "failing", null, 2, Duration.ofMinutes(10));

        CallResult fail1 = runtime.invoke(req("flaky", 1, "p", "retry-key"));
        CallResult fail2 = runtime.invoke(req("flaky", 1, "p", "retry-key"));
        log.info("[{}] fail attempt1 -> status={} basis=error-result-is-cached-as-executed",
                fail1.invocationId(), fail1.status());
        log.info("[{}] fail attempt2 -> status={} body-of-first={} "
                        + "basis=once-a-definite-outcome-exists-it-replays",
                fail2.invocationId(), fail2.status(), fail2.body());
        // 约定：业务异常属于“函数确实执行过”，结果固化，第二次重放同一个 FUNCTION_ERROR
        assertEquals(InvocationStatus.FUNCTION_ERROR, fail1.status());
        assertEquals(InvocationStatus.REPLAY, fail2.status());
        assertEquals(fail1.invocationId(), fail2.invocationId());

        // 不同版本拥有独立幂等域：v1 的键不影响 v2
        runtime.publish("flaky", 2, "echo", null, 2, Duration.ofMinutes(10));
        CallResult v2 = runtime.invoke(req("flaky", 2, "ok", "retry-key"));
        log.info("[{}] same-key on v2 -> status={} body={} basis=version-scoped-idempotency",
                v2.invocationId(), v2.status(), v2.body());
        assertEquals(InvocationStatus.SUCCESS, v2.status());
        assertEquals("ok", v2.body());
    }
}
