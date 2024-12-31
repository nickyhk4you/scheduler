package com.tapestry.dataintegration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.tapestry.dataintegration.config.AwsProperties;
import com.tapestry.dataintegration.config.JobExecution;
import com.tapestry.dataintegration.config.PipelineConfig;
import com.tapestry.dataintegration.service.JobExecutionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class DataIntegrationTaskScheduler {

    private final PipelineConfig pipelineConfig;
    private final AwsProperties awsProperties;
    private AmazonS3 s3Client;

    private final JobExecutionRepository jobExecutionRepository;

    public DataIntegrationTaskScheduler(PipelineConfig pipelineConfig,
                                        AwsProperties awsProperties,
                                        JobExecutionRepository jobExecutionRepository) {
        this.pipelineConfig = pipelineConfig;
        this.awsProperties = awsProperties;
        this.jobExecutionRepository = jobExecutionRepository;
    }

    private synchronized AmazonS3 getS3Client() {
        if (s3Client == null) {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(awsProperties.getRegion())
                    .build();
        }
        return s3Client;
    }

    @Scheduled(fixedRate = 60000)
    public void executeScheduledTasks() {
        LocalDateTime now = LocalDateTime.now();
        String dateFolder = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        pipelineConfig.getPipelines().forEach(pipeline -> {
            String springCron = "0 " + pipeline.getSchedule();
            CronExpression cronExpression = CronExpression.parse(springCron);
            LocalDateTime nextExecutionTime = cronExpression.next(now);

            if (isWithinExecutionWindow(now, nextExecutionTime)) {
                String sourcePath = pipeline.getSource().getPath().replace("{date_folder}", dateFolder);
                String targetPath = pipeline.getTarget().getPath().replace("{date_folder}", dateFolder);

                log.info("Executing pipeline: {}", pipeline.getPlName());
                log.info("From: {}", sourcePath);
                log.info("To: {}", targetPath);

                executeOperation(pipeline.getOperation(), sourcePath, targetPath,
                        pipeline.getPlName(), pipeline.getSourceKeys());
            }
        });
    }


    private boolean isWithinExecutionWindow(LocalDateTime now, LocalDateTime nextExecutionTime) {
        return nextExecutionTime != null &&
                nextExecutionTime.minusMinutes(1).isBefore(now) &&
                nextExecutionTime.plusMinutes(1).isAfter(now);
    }

    private void copyFiles(String sourcePath, String targetPath, List<String> sourceKeys, JobExecution jobExecution) {
        log.info("Copying files from {} to {}", sourcePath, targetPath);
        if (isS3Path(sourcePath) || isS3Path(targetPath)) {
            copyS3Files(sourcePath, targetPath, sourceKeys, jobExecution);
        } else {
            copyLocalFiles(sourcePath, targetPath, sourceKeys, jobExecution);
        }
    }

    private void moveFiles(String sourcePath, String targetPath, List<String> sourceKeys, JobExecution jobExecution) {
        log.info("Moving files from {} to {}", sourcePath, targetPath);
        copyFiles(sourcePath, targetPath, sourceKeys, jobExecution);
        deleteSource(sourcePath, sourceKeys);
    }

    private void copyS3Files(String sourcePath, String targetPath, List<String> sourceKeys, JobExecution jobExecution) {
        List<String> processedSourceFiles = new ArrayList<>();
        List<String> processedTargetFiles = new ArrayList<>();

        S3Location source = parseS3Path(sourcePath);
        S3Location target = parseS3Path(targetPath);

        if (sourceKeys != null && !sourceKeys.isEmpty()) {
            for (String key : sourceKeys) {
                String sourceKey = source.key + key;
                String targetKey = target.key + key;

                try {
                    getS3Client().copyObject(source.bucket, sourceKey, target.bucket, targetKey);
                    processedSourceFiles.add(sourceKey);
                    processedTargetFiles.add(targetKey);
                    log.info("Copied file: {}", key);
                } catch (Exception e) {
                    log.error("Failed to copy file {}: {}", key, e.getMessage());
                }
            }
        } else {
            ListObjectsV2Request listRequest = new ListObjectsV2Request()
                    .withBucketName(source.bucket)
                    .withPrefix(source.key);

            ListObjectsV2Result listing = getS3Client().listObjectsV2(listRequest);
            for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                String sourceKey = summary.getKey();
                String targetKey = target.key + sourceKey.substring(source.key.length());
                try {
                    getS3Client().copyObject(source.bucket, sourceKey, target.bucket, targetKey);
                    processedSourceFiles.add(sourceKey);
                    processedTargetFiles.add(targetKey);
                    log.info("Copied file: {}", sourceKey);
                } catch (Exception e) {
                    log.error("Failed to copy file {}: {}", sourceKey, e.getMessage());
                }
            }
        }

        jobExecution.setSourceFiles(processedSourceFiles);
        jobExecution.setTargetFiles(processedTargetFiles);
        jobExecution.setFilesProcessed(processedSourceFiles.size());
    }

    private void executeOperation(String operation, String sourcePath, String targetPath,
                                  String pipelineName, List<String> sourceKeys) {
        JobExecution jobExecution = new JobExecution();
        jobExecution.setPipelineName(pipelineName);
        jobExecution.setSourcePath(sourcePath);
        jobExecution.setTargetPath(targetPath);
        jobExecution.setOperation(operation);
        jobExecution.setStartTime(LocalDateTime.now());

        try {
            if ("copy".equals(operation)) {
                copyFiles(sourcePath, targetPath, sourceKeys, jobExecution);
            } else if ("move".equals(operation)) {
                moveFiles(sourcePath, targetPath, sourceKeys, jobExecution);
            }
            jobExecution.setStatus("SUCCESS");
        } catch (Exception e) {
            jobExecution.setStatus("FAILED");
            jobExecution.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            jobExecution.setEndTime(LocalDateTime.now());
            jobExecutionRepository.save(jobExecution);
        }
    }

    private void copyLocalFiles(String sourcePath, String targetPath, List<String> sourceKeys, JobExecution jobExecution) {
        List<String> processedSourceFiles = new ArrayList<>();
        List<String> processedTargetFiles = new ArrayList<>();

        try {
            if (sourceKeys != null && !sourceKeys.isEmpty()) {
                for (String key : sourceKeys) {
                    String sourceFile = sourcePath + key;
                    String targetFile = targetPath + key;
                    Files.copy(Paths.get(sourceFile), Paths.get(targetFile), StandardCopyOption.REPLACE_EXISTING);
                    processedSourceFiles.add(sourceFile);
                    processedTargetFiles.add(targetFile);
                    log.info("Copied file: {}", key);
                }
            } else {
                Files.copy(Paths.get(sourcePath), Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING);
                processedSourceFiles.add(sourcePath);
                processedTargetFiles.add(targetPath);
            }

            jobExecution.setSourceFiles(processedSourceFiles);
            jobExecution.setTargetFiles(processedTargetFiles);
            jobExecution.setFilesProcessed(processedSourceFiles.size());
        } catch (IOException e) {
            log.error("Failed to copy local files: {}", e.getMessage());
            throw new RuntimeException("Failed to copy local files", e);
        }
    }

    private void deleteSource(String sourcePath, List<String> sourceKeys) {
        try {
            if (isS3Path(sourcePath)) {
                S3Location source = parseS3Path(sourcePath);
                if (sourceKeys != null && !sourceKeys.isEmpty()) {
                    for (String key : sourceKeys) {
                        String sourceKey = source.key + key;
                        getS3Client().deleteObject(source.bucket, sourceKey);
                        log.info("Deleted file: {}", key);
                    }
                } else {
                    ListObjectsV2Request listRequest = new ListObjectsV2Request()
                            .withBucketName(source.bucket)
                            .withPrefix(source.key);

                    ListObjectsV2Result listing = getS3Client().listObjectsV2(listRequest);
                    for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                        getS3Client().deleteObject(source.bucket, summary.getKey());
                        log.info("Deleted file: {}", summary.getKey());
                    }
                }
            } else {
                if (sourceKeys != null && !sourceKeys.isEmpty()) {
                    for (String key : sourceKeys) {
                        Files.delete(Paths.get(sourcePath + key));
                        log.info("Deleted file: {}", key);
                    }
                } else {
                    Files.delete(Paths.get(sourcePath));
                }
            }
        } catch (IOException e) {
            log.error("Failed to delete source: {}", e.getMessage());
            throw new RuntimeException("Failed to delete source", e);
        }
    }

    private boolean isS3Path(String path) {
        return path != null && path.startsWith("s3://");
    }

    private S3Location parseS3Path(String path) {
        if (!isS3Path(path)) {
            return null;
        }
        String s3Path = path.substring(5); // Remove "s3://"
        int slashIndex = s3Path.indexOf('/');
        return new S3Location(
                s3Path.substring(0, slashIndex),
                s3Path.substring(slashIndex + 1)
        );
    }

    private static class S3Location {
        final String bucket;
        final String key;

        S3Location(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }
}
