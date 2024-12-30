package com.tapestry.dataintegration.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PipelineConfig.class)
public class YamlConfig {
    @Bean
    public PipelineConfig pipelineConfig() {
        return new PipelineConfig();
    }
}
