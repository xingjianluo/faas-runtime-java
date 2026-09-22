package com.github.xingjianluo.faasruntimejava.handler.builtin;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带副作用的计数器：每次执行自增。用于幂等/缩容竞争测试，
 * 断言“同一幂等键不重复执行、缩容竞争下不丢失也不重复”。
 */
public class SideEffectHandler implements FunctionHandler {
    private final AtomicLong counter = new AtomicLong();

    @Override
    public String handle(String payload) {
        long n = counter.incrementAndGet();
        return "side-effect-" + n;
    }

    public long count() {
        return counter.get();
    }
}
