package com.github.xingjianluo.faasruntimejava.handler.builtin;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带“全局副作用”的计数器：所有实例共享一个静态计数器。
 * 用于缩容竞争测试：即使实例被回收、不断冷启动出新实例，
 * 也能可靠判定“调用是否恰好执行一次副作用”。
 */
public class GlobalSideEffectHandler implements FunctionHandler {

    private static final AtomicLong GLOBAL = new AtomicLong();

    /** 测试辅助：重置全局副作用计数。 */
    public static void reset() {
        GLOBAL.set(0);
    }

    /** 当前全局副作用累计次数。 */
    public static long globalCount() {
        return GLOBAL.get();
    }

    @Override
    public String handle(String payload) {
        long n = GLOBAL.incrementAndGet();
        return "global-side-effect-" + n;
    }
}
