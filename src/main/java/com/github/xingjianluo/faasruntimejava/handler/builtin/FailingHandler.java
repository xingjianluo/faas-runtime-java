package com.github.xingjianluo.faasruntimejava.handler.builtin;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;

/** 每次执行都抛异常，用于 FUNCTION_ERROR 分类测试。 */
public class FailingHandler implements FunctionHandler {
    @Override
    public String handle(String payload) {
        throw new IllegalStateException("boom: " + payload);
    }
}
