package com.github.xingjianluo.faasruntimejava.faas;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 幂等存储：以幂等键记录首次调用结果。
 *
 * <p>约定：
 * <ul>
 *   <li>同一幂等键只会有一个调用真正执行函数（副作用只发生一次）；</li>
 *   <li>执行期间到达的同键调用阻塞等待，执行完成后拿到与首次调用相同的结果；</li>
 *   <li>执行完成后的同键调用立即回放首次结果：首次成功标记为
 *       {@link InvocationStatus#IDEMPOTENT_REPLAY}（算成功），首次失败（超时/异常）
 *       标记为 {@link InvocationStatus#IDEMPOTENT_REPLAY_FAILURE}
 *       （可识别为回放，但不算成功）——回放与首次在成败上严格一致。</li>
 * </ul>
 */
@Component
public class IdempotencyStore {

    /** 幂等键 -> 首次调用结果（future 完成即结果可见） */
    private final ConcurrentMap<String, CompletableFuture<InvocationResult>> entries =
            new ConcurrentHashMap<>();

    /**
     * 尝试成为某幂等键的执行者。
     *
     * @return 空表示当前调用获得执行权（之后必须调用 {@link #complete}）；
     *         非空表示该键已有结果（可能刚等到），调用方应直接回放
     */
    public Optional<InvocationResult> beginOrAwait(String idempotencyKey) {
        CompletableFuture<InvocationResult> mine = new CompletableFuture<>();
        CompletableFuture<InvocationResult> existing = entries.putIfAbsent(idempotencyKey, mine);
        if (existing == null) {
            return Optional.empty();
        }
        // 已存在：阻塞等待首个调用完成（已完成的 future 立即返回）
        return Optional.of(existing.join());
    }

    /**
     * 记录首次执行结果，并唤醒等待同一键的调用。
     */
    public void complete(String idempotencyKey, InvocationResult result) {
        CompletableFuture<InvocationResult> future = entries.get(idempotencyKey);
        if (future != null) {
            future.complete(result);
        }
    }

    /**
     * 调用在执行前被拒绝（并发/容量打满）时使用：先把拒绝结果告知等待者，
     * 再驱逐该键，使后续同键调用可以重新尝试执行而不是永远回放拒绝结果。
     */
    public void completeAndEvict(String idempotencyKey, InvocationResult result) {
        CompletableFuture<InvocationResult> future = entries.remove(idempotencyKey);
        if (future != null) {
            future.complete(result);
        }
    }

    /**
     * 判定某结果是否属于“执行前因容量/并发被拒”。
     * 被拒结果：不是成功、不算幂等成功回放、必须驱逐幂等键，绝不能伪装成成功回放。
     */
    public static boolean isPreExecutionRejection(InvocationResult result) {
        return result.status() == InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED
                || result.status() == InvocationStatus.INSTANCE_CAPACITY_EXCEEDED;
    }

    /**
     * 判定某结果是否属于“真正执行后的失败”（超时/执行体异常/实例初始化失败）。
     * 这类结果被同键回放时必须保持失败语义：包装为
     * {@link InvocationStatus#IDEMPOTENT_REPLAY_FAILURE}，可识别为回放但不算成功。
     */
    public static boolean isExecutionFailure(InvocationResult result) {
        return result.status() == InvocationStatus.TIMEOUT
                || result.status() == InvocationStatus.HANDLER_ERROR;
    }

    /** 测试与指标用：当前记录的幂等键数量 */
    public int size() {
        return entries.size();
    }
}
