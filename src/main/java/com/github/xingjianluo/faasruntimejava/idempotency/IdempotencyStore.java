package com.github.xingjianluo.faasruntimejava.idempotency;

import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.InvocationStatus;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 幂等存储（进程内、按 函数+版本+幂等键 维度）。
 *
 * <p>调用编排采用“先占幂等槽、再抢并发配额”：
 * <ol>
 *   <li>{@link #begin}：同键首个调用成为 {@link Leader}，其余成为 {@link Follower}，
 *       重复调用全部阻塞等待，不会各自去抢占实例配额（因此不会被误判为超并发）；</li>
 *   <li>Leader 抢到实例并执行；执行成功/业务异常/超时都属于“已确定执行过”，
 *       {@link Leader#complete} 会固化结果供重放——超时结果保守固化，避免重复副作用；</li>
 *   <li>Leader 若因超并发未能执行，必须 {@link Leader#fail} 回滚占位，
 *       保证“被拒绝的调用无副作用、不占用幂等键”，后续同键调用可重试。</li>
 * </ol>
 */
public final class IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyStore.class);

    private record Key(String function, int version, String idempotencyKey) {
    }

    /** begin 的判定结果。 */
    public sealed interface Join permits Leader, Follower {
    }

    /** 首个调用：负责真正执行，并必须恰好调用一次 complete/fail。 */
    public static final class Leader implements Join {
        private final IdempotencyStore store;
        private final Key key;
        private final Slot slot;

        private Leader(IdempotencyStore store, Key key, Slot slot) {
            this.store = store;
            this.key = key;
            this.slot = slot;
        }

        /** 固化“确定执行过”的结果（成功/业务异常/超时），唤醒等待者重放。 */
        public void complete(CallResult result) {
            if (!cacheable(result.status())) {
                fail();
                return;
            }
            slot.completed = result;
            slot.future.complete(result);
            log.info("IDEMPOTENCY-STORE key={}#v{}:{} stored invocationId={} status={}",
                    key.function(), key.version(), key.idempotencyKey(),
                    result.invocationId(), result.status());
        }

        /** 回滚占位（未执行/被并发拒绝），后续同键调用可以重新执行。 */
        public void fail() {
            store.store.remove(key, slot);
            slot.future.completeExceptionally(new IdempotentLeaderFailedException());
            log.info("IDEMPOTENCY-RELEASE key={}#v{}:{} leader did not execute, slot removed",
                    key.function(), key.version(), key.idempotencyKey());
        }
    }

    /** 重复调用：等待 Leader 的确定结果并以 REPLAY 重放。 */
    public static final class Follower implements Join {
        private final CompletableFuture<CallResult> future;

        private Follower(CompletableFuture<CallResult> future) {
            this.future = future;
        }

        /**
         * @return Leader 的确定结果；为空表示 Leader 未执行（占位已回滚），调用方应重新 begin
         */
        public Optional<CallResult> awaitResult() throws InterruptedException {
            try {
                return Optional.of(future.get());
            } catch (ExecutionException e) {
                return Optional.empty();
            }
        }
    }

    /** Leader 未执行时用于唤醒等待者的信号。 */
    private static final class IdempotentLeaderFailedException extends RuntimeException {
        private IdempotentLeaderFailedException() {
            super(null, null, false, false);
        }
    }

    private static final class Slot {
        volatile CallResult completed;
        final CompletableFuture<CallResult> future = new CompletableFuture<>();
    }

    private final ConcurrentHashMap<Key, Slot> store = new ConcurrentHashMap<>();

    /** 开始一次幂等调用：首个为 Leader，重复为 Follower。 */
    public Join begin(String function, int version, String idempotencyKey) {
        Key key = new Key(function, version, idempotencyKey);
        Slot mine = new Slot();
        Slot existing = store.putIfAbsent(key, mine);
        if (existing == null) {
            return new Leader(this, key, mine);
        }
        log.info("IDEMPOTENCY-JOIN key={}#v{}:{} duplicate call joins to wait replay",
                function, version, idempotencyKey);
        return new Follower(existing.completed != null ? CompletableFuture.completedFuture(existing.completed)
                : existing.future);
    }

    /** 仅“确定执行过”的终局才允许重放：成功、业务异常、超时。 */
    private static boolean cacheable(InvocationStatus status) {
        return status == InvocationStatus.SUCCESS
                || status == InvocationStatus.FUNCTION_ERROR
                || status == InvocationStatus.TIMEOUT;
    }

    /** 查询已确定的历史结果（仅用于指标/断言场景）。 */
    public Optional<CallResult> find(String function, int version, String key) {
        if (key == null) {
            return Optional.empty();
        }
        Slot slot = store.get(new Key(function, version, key));
        return slot == null ? Optional.empty() : Optional.ofNullable(slot.completed);
    }
}
