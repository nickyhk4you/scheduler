package com.tapestry.dataintegration.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
@Component
@ConfigurationProperties(prefix = "")
@Data
public class PipelineConfig {
    private List<Pipeline> pipelines;

    @Data
    public static class Pipeline {
        private String plName;
        private String schedule;
        private Source source;
        private Target target;
        private String operation;
        private String snsTopic;
        private List<String> sourceKeys;
    }

    @Data
    public static class Source {
        private String path;
    }

    @Data
    public static class Target {
        private String path;
    }
}