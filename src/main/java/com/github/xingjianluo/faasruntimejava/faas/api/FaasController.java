package com.github.xingjianluo.faasruntimejava.faas.api;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionHandler;
import com.github.xingjianluo.faasruntimejava.faas.FunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.faas.InvocationResult;
import com.github.xingjianluo.faasruntimejava.faas.PublishResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地验证用 REST 入口。
 *
 * <p>由于函数体是代码，发布接口支持几种内置处理器类型，便于用 curl 验证
 * 发布、调用、超时、冷启动、实例隔离与幂等行为：
 * <ul>
 *   <li>{@code echo}：原样返回入参；</li>
 *   <li>{@code uppercase}：将入参字符串转为大写；</li>
 *   <li>{@code sleep}：休眠 sleepMillis 后返回入参（验证超时与故障淘汰）；</li>
 *   <li>{@code failing}：固定抛出异常（验证 HANDLER_ERROR 与故障淘汰）；</li>
 *   <li>{@code counter}：实例级计数工厂，每个实例独立计数，返回 [实例ID, 该实例计数]，
 *       用于验证多实例状态隔离与暖实例复用。</li>
 * </ul>
 *
 * <p>不传容量参数（maxWarmInstances / maxConcurrencyPerInstance）且类型不是 counter 时，
 * 走遗留发布方式，行为与旧版一致；显式传任一容量参数即按版本配置容量。
 */
@RestController
@RequestMapping("/faas")
public class FaasController {

    private final FaasRuntime runtime;

    public FaasController(FaasRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * 发布请求体：
     * handlerType 必选；sleepMillis 仅 sleep 类型使用；
     * maxWarmInstances / maxConcurrencyPerInstance 为按版本容量配置，可省略。
     */
    public record PublishRequest(String handlerType, Long sleepMillis,
                                 Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
    }

    @PostMapping("/functions/{name}/versions/{version}")
    public ResponseEntity<PublishResult> publish(@PathVariable String name,
                                                 @PathVariable int version,
                                                 @RequestBody PublishRequest request) {
        String type = request.handlerType() == null ? "" : request.handlerType();
        Integer maxWarm = request.maxWarmInstances();
        Integer maxPerInstance = request.maxConcurrencyPerInstance();
        boolean capacityConfigured = maxWarm != null || maxPerInstance != null;

        // counter 是实例级状态工厂：每个实例独立持有一个计数器。
        // 配合返回结果里的 instanceId 即可验证多实例状态隔离与暖实例复用。
        if ("counter".equals(type)) {
            FunctionHandlerFactory factory = () -> {
                AtomicInteger count = new AtomicInteger(0);
                return input -> count.incrementAndGet();
            };
            PublishResult result = runtime.publish(name, version, factory, maxWarm, maxPerInstance);
            return toResponse(result);
        }

        FunctionHandler handler = switch (type) {
            case "echo" -> input -> input;
            case "uppercase" -> input -> input == null ? null : input.toString().toUpperCase();
            case "sleep" -> input -> {
                Thread.sleep(request.sleepMillis() == null ? 1000 : request.sleepMillis());
                return input;
            };
            case "failing" -> input -> {
                throw new IllegalStateException("intentional failure for verification");
            };
            default -> null;
        };
        if (handler == null) {
            return ResponseEntity.unprocessableEntity().body(PublishResult.rejected(
                    com.github.xingjianluo.faasruntimejava.faas.PublishStatus.INVALID_ARGUMENT,
                    name, version, "unknown handlerType: " + type));
        }
        PublishResult result = capacityConfigured
                ? runtime.publish(name, version, handler, maxWarm, maxPerInstance)
                : runtime.publish(name, version, handler);
        return toResponse(result);
    }

    private static ResponseEntity<PublishResult> toResponse(PublishResult result) {
        return result.published() ? ResponseEntity.ok(result)
                : ResponseEntity.unprocessableEntity().body(result);
    }

    @PostMapping("/functions/{name}/invoke")
    public InvocationResult invoke(@PathVariable String name,
                                   @RequestParam(defaultValue = "0") int version,
                                   @RequestParam(required = false) String idempotencyKey,
                                   @RequestBody(required = false) Object input) {
        return runtime.invoke(name, version, input, idempotencyKey);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return runtime.metrics();
    }
}
