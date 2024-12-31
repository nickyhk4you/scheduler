package com.tapestry.dataintegration.service;


import com.tapestry.dataintegration.DataIntegrationTaskScheduler;
import com.tapestry.dataintegration.config.JobExecution;
import com.tapestry.dataintegration.config.PipelineConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobExecutionController {
    private final JobExecutionRepository jobExecutionRepository;
    private final DataIntegrationTaskScheduler taskScheduler;

    @GetMapping
    public List<JobExecution> getAllJobs() {
        return jobExecutionRepository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobExecution> getJobById(@PathVariable Long id) {
        return jobExecutionRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/pipeline/{pipelineName}")
    public List<JobExecution> getJobsByPipeline(@PathVariable String pipelineName) {
        return jobExecutionRepository.findByPipelineName(pipelineName);
    }

    @GetMapping("/search")
    public List<JobExecution> searchJobs(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime) {
        return jobExecutionRepository.findByStartTimeBetween(startTime, endTime);
    }

    @PostMapping("/execute/{pipelineName}")
    public ResponseEntity<JobExecution> executePipeline(@PathVariable String pipelineName) {
        Optional<PipelineConfig.Pipeline> pipeline = taskScheduler.findPipelineByName(pipelineName);

        if (pipeline.isPresent()) {
            String dateFolder = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            PipelineConfig.Pipeline p = pipeline.get();

            String sourcePath = p.getSource().getPath().replace("{date_folder}", dateFolder);
            String targetPath = p.getTarget().getPath().replace("{date_folder}", dateFolder);

            JobExecution execution = taskScheduler.executeOperation(
                    p.getOperation(),
                    sourcePath,
                    targetPath,
                    pipelineName,
                    p.getSourceKeys()
            );

            return ResponseEntity.ok(execution);
        }

        return ResponseEntity.notFound().build();
    }
}
