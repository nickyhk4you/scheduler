package com.tapestry.dataintegration.service;


import com.tapestry.dataintegration.config.JobExecution;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobExecutionController {
    private final JobExecutionRepository jobExecutionRepository;

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
}
