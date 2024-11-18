package com.colak.springtutorial.config;

import com.colak.springtutorial.model.Person;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class CursorReaderBatchConfig {

    private final DataSource dataSource;

    @Bean
    public Job cursorJob(JobRepository jobRepository, Step cursorStep) {
        return new JobBuilder("cursor-job", jobRepository)
                .start(cursorStep)
                .build();
    }

    @Bean
    public Step cursorStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcCursorItemReader<Person> cursorReader,
            JdbcBatchItemWriter<Person> writer2) {

        return new StepBuilder("cursor-step", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(cursorReader)
                .processor(person -> {
                    person.setName(person.getName().toUpperCase()); // Transform the name
                    return person;
                })
                .writer(writer2)
                .build();
    }

    @Bean
    public JdbcCursorItemReader<Person> cursorReader() {
        JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT id, name, age FROM PERSON"); // SQL query to fetch data
        reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));

        return reader;
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer2() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO person_processed2 (id, name, age) VALUES (:id, :name, :age)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

}

