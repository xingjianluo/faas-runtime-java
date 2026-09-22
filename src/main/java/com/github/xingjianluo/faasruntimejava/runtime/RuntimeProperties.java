package com.github.xingjianluo.faasruntimejava.runtime;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

/** 运行时可调参数（application.properties 可覆盖，骨架）。 */
@ConfigurationProperties(prefix = "faas")
public class RuntimeProperties {
    private Duration reaperInterval = Duration.ofSeconds(5);

    public Duration getReaperInterval() { return reaperInterval; }
    public void setReaperInterval(Duration reaperInterval) { this.reaperInterval = reaperInterval; }
}
