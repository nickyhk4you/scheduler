package com.tapestry.dataintegration;

import com.tapestry.dataintegration.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.springframework.scheduling.support.CronExpression;

@Component
@Slf4j
public class DataIntegrationTaskScheduler {

    private final PipelineConfig pipelineConfig;

    public DataIntegrationTaskScheduler(PipelineConfig pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
    }

    @Scheduled(fixedRate = 60000) // Check every minute
    public void executeScheduledTasks() {
        LocalDateTime now = LocalDateTime.now();
        String dateFolder = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        pipelineConfig.getPipelines().forEach(pipeline -> {
            // Convert Unix cron to Spring cron by adding seconds
            String springCron = "0 " + pipeline.getSchedule();
            CronExpression cronExpression = CronExpression.parse(springCron);
            LocalDateTime nextExecutionTime = cronExpression.next(now);

            if (nextExecutionTime != null &&
                    nextExecutionTime.minusMinutes(1).isBefore(now) &&
                    nextExecutionTime.plusMinutes(1).isAfter(now)) {

                String sourcePath = pipeline.getSource().getPath().replace("{date_folder}", dateFolder);
                String targetPath = pipeline.getTarget().getPath().replace("{date_folder}", dateFolder);

                log.info("Executing pipeline: {}", pipeline.getPlName());
                log.info("From: {}", sourcePath);
                log.info("To: {}", targetPath);

                if ("copy".equals(pipeline.getOperation())) {
                    copyFiles(sourcePath, targetPath);
                } else if ("move".equals(pipeline.getOperation())) {
                    moveFiles(sourcePath, targetPath);
                }
            }
        });
    }

    private void copyFiles(String sourcePath, String targetPath) {
        log.info("Copying files from {} to {}", sourcePath, targetPath);
    }

    private void moveFiles(String sourcePath, String targetPath) {
        log.info("Moving files from {} to {}", sourcePath, targetPath);
    }
}
