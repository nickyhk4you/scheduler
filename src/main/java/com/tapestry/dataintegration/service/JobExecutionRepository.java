package com.tapestry.dataintegration.service;

import com.tapestry.dataintegration.config.JobExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecution, Long> {
    List<JobExecution> findByPipelineName(String pipelineName);
    List<JobExecution> findByStartTimeBetween(LocalDateTime start, LocalDateTime end);
    // Optional: Add query for endTime if needed
    List<JobExecution> findByEndTimeBetween(LocalDateTime start, LocalDateTime end);
}