package com.colak.springtutorial.config;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_one", jobRepository)
                .tasklet((_, _) -> {
                    System.out.println("STEP1 EXECUTED");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_two", jobRepository)
                .tasklet((_, _) -> {
                    System.out.println("STEP2 EXECUTED");
                    throw new Exception("Exception occurred!!");
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_four", jobRepository)
                .tasklet((_, _) -> {
                    System.out.println("STEP4 EXECUTED");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_three", jobRepository)
                .tasklet((_, _) -> {
                    System.out.println("STEP3 EXECUTED");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("job1", jobRepository)
                .start(step1(jobRepository, transactionManager))
                .on(ExitStatus.COMPLETED.getExitCode())
                .to(step2(jobRepository, transactionManager))

                // If step2 fails go to step4
                .from(step2(jobRepository, transactionManager))
                .on(ExitStatus.FAILED.getExitCode())
                .to(step4(jobRepository, transactionManager))

                // If step2 completes go to step2
                .from(step2(jobRepository, transactionManager))
                .on(ExitStatus.COMPLETED.getExitCode())
                .to(step3(jobRepository, transactionManager))
                .end().build();
    }

}
