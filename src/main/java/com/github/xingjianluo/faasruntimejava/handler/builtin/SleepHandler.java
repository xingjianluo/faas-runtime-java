package com.github.xingjianluo.faasruntimejava.handler.builtin;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;

/** 按 payload 指定毫秒数睡眠，用于超时测试。 */
public class SleepHandler implements FunctionHandler {
    @Override
    public String handle(String payload) throws Exception {
        long ms = Long.parseLong(payload == null ? "0" : payload.trim());
        Thread.sleep(ms);
        return "slept-" + ms;
    }
}
