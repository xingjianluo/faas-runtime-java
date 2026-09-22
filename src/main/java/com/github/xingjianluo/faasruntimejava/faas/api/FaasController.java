package com.github.xingjianluo.faasruntimejava.faas.api;

import com.github.xingjianluo.faasruntimejava.faas.FaasRuntime;
import com.github.xingjianluo.faasruntimejava.faas.FunctionHandler;
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

/**
 * 本地验证用 REST 入口。
 *
 * <p>由于函数体是代码，发布接口支持几种内置处理器类型，便于用 curl 验证
 * 发布、调用、超时、冷启动与幂等行为：
 * <ul>
 *   <li>{@code echo}：原样返回入参；</li>
 *   <li>{@code uppercase}：将入参字符串转为大写；</li>
 *   <li>{@code sleep}：休眠 sleepMillis 后返回入参（验证超时）；</li>
 *   <li>{@code failing}：固定抛出异常（验证 HANDLER_ERROR）。</li>
 * </ul>
 */
@RestController
@RequestMapping("/faas")
public class FaasController {

    private final FaasRuntime runtime;

    public FaasController(FaasRuntime runtime) {
        this.runtime = runtime;
    }

    /** 发布请求体：handlerType 必选，sleepMillis 仅 sleep 类型使用 */
    public record PublishRequest(String handlerType, Long sleepMillis) {
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
            default -> null;
        };
        PublishResult result = runtime.publish(name, version, handler);
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
