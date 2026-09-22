package com.github.xingjianluo.faasruntimejava.handler.builtin;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;

/** 原样返回输入，用于常规成功路径测试。 */
public class EchoHandler implements FunctionHandler {
    @Override
    public String handle(String payload) {
        return payload;
    }
}
