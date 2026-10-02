package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 一个已发布函数版本的不可变定义。
 *
 * <p>条目一经写入即不可变，因此新版本发布不会影响老版本已在运行的实例，
 * 老版本继续按发布时的处理器与容量配置提供服务。
 *
 * @param functionName                 函数名
 * @param version                      版本号
 * @param sharedHandler                共享处理器（老发布方式与无状态发布），可能为 null
 * @param handlerFactory               实例级处理器工厂（持有实例级状态），可能为 null
 * @param capacity                     生效容量（遗留容量或显式配置）
 * @param legacy                       是否为未显式配置容量的老函数：
 *                                     true 时使用函数级并发信号量，行为与旧版一致
 */
public record FunctionDefinition(String functionName,
                                 int version,
                                 FunctionHandler sharedHandler,
                                 FunctionHandlerFactory handlerFactory,
                                 Capacity capacity,
                                 boolean legacy) {

    /**
     * 冷启动一个新实例时获取该实例独占的处理器。
     *
     * @throws Exception 工厂创建失败时由运行时归类为 HANDLER_ERROR
     */
    public FunctionHandler newInstanceHandler() throws Exception {
        if (handlerFactory != null) {
            return handlerFactory.create();
        }
        return sharedHandler;
    }
}
