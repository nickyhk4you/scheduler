package com.tapestry.dataintegration;

import com.tapestry.dataintegration.config.PipelineConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }

//    @Bean
//    CommandLineRunner runner(PipelineConfig pipelineConfig) {
//        return args -> {
//            System.out.println("Loaded Pipelines: " + pipelineConfig.getPipelines().size());
//            pipelineConfig.getPipelines().forEach(pipeline -> {
//                System.out.println("Pipeline Name: " + pipeline.getPlName());
//                System.out.println("Schedule: " + pipeline.getSchedule());
//                System.out.println("Operation: " + pipeline.getOperation());
//            });
//        };
//    }
}
