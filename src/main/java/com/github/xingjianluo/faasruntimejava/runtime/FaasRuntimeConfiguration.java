package com.github.xingjianluo.faasruntimejava.runtime;

import com.github.xingjianluo.faasruntimejava.handler.DefaultFunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.handler.FunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.scaling.IdleReaper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Spring 装配：运行时既能容器内使用，也能纯 Java 单测直接 new。 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(RuntimeProperties.class)
public class FaasRuntimeConfiguration {

    @Bean
    public FunctionHandlerFactory functionHandlerFactory() {
        return new DefaultFunctionHandlerFactory();
    }

    @Bean
    public FaasRuntime faasRuntime(FunctionHandlerFactory factory, RuntimeProperties properties) {
        return new FaasRuntime(factory, properties);
    }

    /** 注册为受管 Bean，@Scheduled 缩容才会触发。 */
    @Bean
    public IdleReaper idleReaper(FaasRuntime runtime) {
        return runtime.reaper();
    }
}
