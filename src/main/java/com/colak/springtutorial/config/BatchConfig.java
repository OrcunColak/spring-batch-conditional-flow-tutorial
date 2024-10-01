package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Configuration
@Slf4j
public class BatchConfig {

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_one", jobRepository)
                .tasklet((_, _) -> {
                    log.info("STEP1 EXECUTED");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("step_two", jobRepository)
                .tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
                    // Example logic: use a job parameter to determine even or odd
                    StepContext stepContext = chunkContext.getStepContext();
                    Map<String, Object> jobParameters = stepContext.getJobParameters();
                    Long runDecision = (Long) jobParameters.get("runDecision");

                    if (runDecision % 2 == 0) {
                        log.info("Even number detected : {}" , runDecision);
                        contribution.setExitStatus(new ExitStatus("EVEN"));
                    } else {
                        log.info("Odd number detected : {}" , runDecision);
                        contribution.setExitStatus(new ExitStatus("ODD"));
                    }

                    log.info("STEP2 EXECUTED");
                    return RepeatStatus.FINISHED;

                }, transactionManager)
                .build();
    }

    @Bean
    public Step stepOdd(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_four", jobRepository)
                .tasklet((_, _) -> {
                    log.info("STEP ODD EXECUTED");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step stepEven(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_three", jobRepository)
                .tasklet((_, _) -> {
                    log.info("STEP EVEN EXECUTED");
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
                .on("EVEN").to(stepEven(jobRepository, transactionManager)) // If EVEN, go to evenStep

                // If step2 completes go to step2
                .from(step2(jobRepository, transactionManager))
                .on("ODD").to(stepOdd(jobRepository, transactionManager)) // If EVEN, go to evenStep
                .end()// End the job
                .build();
    }
}
