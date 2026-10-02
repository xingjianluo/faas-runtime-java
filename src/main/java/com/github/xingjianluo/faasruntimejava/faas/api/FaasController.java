package com.github.xingjianluo.faasruntimejava.faas.api;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionHandler;
import com.github.xingjianluo.faasruntimejava.faas.InstanceContext;
import com.github.xingjianluo.faasruntimejava.faas.InvocationResult;
import com.github.xingjianluo.faasruntimejava.faas.PublishResult;
import com.github.xingjianluo.faasruntimejava.faas.StatefulFunctionHandler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 本地验证用 REST 入口。
 *
 * <p>由于函数体是代码，发布接口支持几种内置处理器类型，便于用 curl 验证
 * 发布、调用、超时、冷启动与幂等行为：
 * <ul>
 *   <li>{@code echo}：原样返回入参；</li>
 *   <li>{@code uppercase}：将入参字符串转为大写；</li>
 *   <li>{@code sleep}：休眠 sleepMillis 后返回入参（验证超时与故障淘汰）；</li>
 *   <li>{@code failing}：固定抛出异常（验证 HANDLER_ERROR 与故障淘汰）；</li>
 *   <li>{@code counter}：有状态处理器，把每次入参追加进实例私有的 List 并返回
 *       实例 ID 与状态快照，用于验证多实例状态隔离。</li>
 * </ul>
 * 发布时可选传 {@code maxWarmInstances} 与 {@code maxConcurrencyPerInstance}
 * 配置版本容量；不传则沿用默认行为（单实例 + 函数级并发上限）。
 */
@RestController
@RequestMapping("/faas")
public class FaasController {

    private final FaasRuntime runtime;

    public FaasController(FaasRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * 发布请求体：handlerType 必选；sleepMillis 仅 sleep 类型使用；
     * maxWarmInstances / maxConcurrencyPerInstance 可选，为容量配置（正整数）
     */
    public record PublishRequest(String handlerType, Long sleepMillis,
                                 Integer maxWarmInstances,
                                 Integer maxConcurrencyPerInstance) {
    }

    @PostMapping("/functions/{name}/versions/{version}")
    public ResponseEntity<PublishResult> publish(@PathVariable String name,
                                                 @PathVariable int version,
                                                 @RequestBody PublishRequest request) {
        FunctionHandler handler = switch (request.handlerType() == null ? "" : request.handlerType()) {
            case "echo" -> input -> input;
            case "uppercase" -> input -> input == null ? null : input.toString().toUpperCase();
            case "sleep" -> input -> {
                Thread.sleep(request.sleepMillis() == null ? 1000 : request.sleepMillis());
                return input;
            };
            case "failing" -> input -> {
                throw new IllegalStateException("intentional failure for verification");
            };
            case "counter" -> new StatefulFunctionHandler() {
                @Override
                public Object initInstanceState() {
                    return new java.util.ArrayList<String>();
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object handle(Object input, InstanceContext context) {
                    java.util.List<String> state = (java.util.List<String>) context.state();
                    state.add(String.valueOf(input));
                    java.util.Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
                    snapshot.put("instanceId", context.instanceId());
                    snapshot.put("state", new java.util.ArrayList<>(state));
                    return snapshot;
                }
            };
            default -> null;
        };
        PublishResult result;
        if (request.maxWarmInstances() != null || request.maxConcurrencyPerInstance() != null) {
            int maxWarm = request.maxWarmInstances() == null ? 1 : request.maxWarmInstances();
            int perInstance = request.maxConcurrencyPerInstance() == null
                    ? 1 : request.maxConcurrencyPerInstance();
            result = runtime.publish(name, version, handler, maxWarm, perInstance);
        } else {
            result = runtime.publish(name, version, handler);
        }
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
