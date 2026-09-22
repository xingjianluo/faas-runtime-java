package com.github.xingjianluo.faasruntimejava.web;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.InvocationRequest;
import com.github.xingjianluo.faasruntimejava.runtime.FaasRuntime;
import java.time.Duration;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地 HTTP 入口：发布 / 调用 / 指标。
 * 注意：调用类失败（超时、函数异常、超并发等）统一返回 HTTP 200，
 * 由响应体 {@link CallResult#status()} 与 errorCode 区分；
 * 只有发布类失败才映射为可区分的 4xx HTTP 状态。
 */
@RestController
@RequestMapping("/faas")
public class FaasController {

    private final FaasRuntime runtime;

    public FaasController(FaasRuntime runtime) {
        this.runtime = runtime;
    }

    @PostMapping("/functions")
    public ResponseEntity<?> publish(@RequestBody PublishRequest req) {
        return ResponseEntity.ok(runtime.publish(req.name(), req.version(), req.handlerType(),
                req.timeoutMs() == null ? null : Duration.ofMillis(req.timeoutMs()),
                req.concurrency(),
                req.idleTtlMs() == null ? null : Duration.ofMillis(req.idleTtlMs())));
    }

    @PostMapping("/functions/{name}/invocations")
    public CallResult invoke(@PathVariable String name, @RequestBody InvokeRequest req) {
        return runtime.invoke(new InvocationRequest(name, req.version(), req.payload(),
                req.idempotencyKey()));
    }

    @GetMapping("/functions/{name}/metrics")
    public Object metrics(@PathVariable String name,
                          @RequestParam(required = false) Integer version) {
        return runtime.metrics(name, version);
    }

    /** 发布类失败映射为可区分的 HTTP 状态与错误码。 */
    @ExceptionHandler(FaasException.class)
    public ResponseEntity<Map<String, String>> handle(FaasException ex) {
        HttpStatus status;
        if (ex instanceof FaasException.VersionAlreadyExistsException) {
            status = HttpStatus.CONFLICT;                 // 409 版本不可覆盖
        } else if (ex instanceof FaasException.FunctionNotFoundException
                || ex instanceof FaasException.VersionNotFoundException) {
            status = HttpStatus.NOT_FOUND;               // 404
        } else if (ex instanceof FaasException.InvalidVersionException
                || ex instanceof FaasException.InvalidHandlerException
                || ex instanceof FaasException.InvalidInvocationException) {
            status = HttpStatus.BAD_REQUEST;             // 400
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;   // 500
        }
        return ResponseEntity.status(status).body(Map.of(
                "errorCode", ex.getClass().getSimpleName(),
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
