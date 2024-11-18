package com.colak.springtutorial.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MultiJobLauncherConfig {

    private final JobLauncher jobLauncher;
    private final Job job; // First job
    private final Job cursorJob; // Second job

    public MultiJobLauncherConfig(JobLauncher jobLauncher, Job job, Job cursorJob) {
        this.jobLauncher = jobLauncher;
        this.job = job;
        this.cursorJob = cursorJob;
    }

    @Bean
    public CommandLineRunner runJobs() {
        return args -> {
            // Launch first job
            jobLauncher.run(job, new JobParametersBuilder()
                    .toJobParameters());

            // Launch second job
            jobLauncher.run(cursorJob, new JobParametersBuilder()
                    .toJobParameters());
        };
    }
}

