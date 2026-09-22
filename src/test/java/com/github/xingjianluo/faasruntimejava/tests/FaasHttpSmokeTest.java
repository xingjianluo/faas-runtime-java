package com.github.xingjianluo.faasruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 端到端冒烟：经真实 HTTP 端口发布 / 调用 / 重复发布 / 幂等重放 / 指标。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FaasHttpSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(FaasHttpSmokeTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders json() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    @Test
    void publishInvokeReplayAndMetricsOverHttp() {
        // 1. 发布 v1
        String body = """
                {"name":"httpFn","version":1,"handlerType":"echo","timeoutMs":2000,"concurrency":3,"idleTtlMs":60000}
                """;
        ResponseEntity<String> pub = rest.postForEntity(url("/faas/functions"),
                new HttpEntity<>(body, json()), String.class);
        assertEquals(HttpStatus.OK, pub.getStatusCode());
        log.info("HTTP publish v1 -> {} basis=first-version", pub.getStatusCode());

        // 2. 重复发布 v1 必须 409 且错误码可区分
        ResponseEntity<String> dup = rest.postForEntity(url("/faas/functions"),
                new HttpEntity<>(body, json()), String.class);
        assertEquals(HttpStatus.CONFLICT, dup.getStatusCode());
        assertTrue(dup.getBody().contains("VersionAlreadyExistsException"));
        log.info("HTTP duplicate publish -> {} body={} basis=immutable-history-version",
                dup.getStatusCode(), dup.getBody());

        // 3. 未知处理器 400
        String bad = """
                {"name":"httpBad","version":1,"handlerType":"nope"}
                """;
        ResponseEntity<String> badResp = rest.postForEntity(url("/faas/functions"),
                new HttpEntity<>(bad, json()), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, badResp.getStatusCode());
        assertTrue(badResp.getBody().contains("InvalidHandlerException"));
        log.info("HTTP bad handler -> {} basis=handler-type-unknown", badResp.getStatusCode());

        // 4. 调用（幂等键）
        String invoke = """
                {"payload":"ping","idempotencyKey":"abc"}
                """;
        ResponseEntity<String> r1 = rest.postForEntity(
                url("/faas/functions/httpFn/invocations"),
                new HttpEntity<>(invoke, json()), String.class);
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        assertTrue(r1.getBody().contains("SUCCESS"));
        assertTrue(r1.getBody().contains("ping"));
        log.info("HTTP invoke#1 -> {} basis=normal success", r1.getBody());

        // 5. 同键重放
        ResponseEntity<String> r2 = rest.postForEntity(
                url("/faas/functions/httpFn/invocations"),
                new HttpEntity<>(invoke, json()), String.class);
        assertTrue(r2.getBody().contains("REPLAY"));
        log.info("HTTP invoke#2 -> {} basis=idempotent replay", r2.getBody());

        // 6. 指标
        ResponseEntity<String> m = rest.getForEntity(
                url("/faas/functions/httpFn/metrics?version=1"), String.class);
        assertEquals(HttpStatus.OK, m.getStatusCode());
        assertNotNull(m.getBody());
        log.info("HTTP metrics -> {}", m.getBody());
        assertTrue(m.getBody().contains("\"totalCalls\":2"));
    }
}
