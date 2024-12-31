package com.tapestry.dataintegration.config;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "job_executions")
@Data
public class JobExecution {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String pipelineName;
    private String sourcePath;
    private String targetPath;
    private String operation;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String status;
    private String errorMessage;
    private Integer filesProcessed;

    @ElementCollection
    @CollectionTable(name = "job_execution_files")
    private List<String> sourceFiles;

    @ElementCollection
    @CollectionTable(name = "job_execution_target_files")
    private List<String> targetFiles;
}
