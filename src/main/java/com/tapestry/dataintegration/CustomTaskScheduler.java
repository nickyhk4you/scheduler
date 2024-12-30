package com.tapestry.dataintegration;

import com.tapestry.dataintegration.config.PipelineConfig;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.springframework.scheduling.support.CronExpression;

@Component
public class CustomTaskScheduler {

    private final PipelineConfig pipelineConfig;

    public CustomTaskScheduler(PipelineConfig pipelineConfig) {
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

                System.out.println("Executing pipeline: " + pipeline.getPlName());
                System.out.println("From: " + sourcePath);
                System.out.println("To: " + targetPath);

                if ("copy".equals(pipeline.getOperation())) {
                    copyFiles(sourcePath, targetPath);
                } else if ("move".equals(pipeline.getOperation())) {
                    moveFiles(sourcePath, targetPath);
                }
            }
        });
    }

    private void copyFiles(String sourcePath, String targetPath) {
        System.out.println("Copying files from " + sourcePath + " to " + targetPath);
    }

    private void moveFiles(String sourcePath, String targetPath) {
        System.out.println("Moving files from " + sourcePath + " to " + targetPath);
    }
}
