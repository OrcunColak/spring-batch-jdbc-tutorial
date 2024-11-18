package com.colak.springtutorial.config;

import com.colak.springtutorial.jpa.Person;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Collections;

@Configuration
@RequiredArgsConstructor
public class PagingBatchConfig {

    private final DataSource dataSource;

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder("job", jobRepository)
                .start(step)
                .build();
    }

    @Bean
    public Step step(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcPagingItemReader<Person> reader,
            JdbcBatchItemWriter<Person> writer) {

        return new StepBuilder("step", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(reader)
                .processor(person -> {
                    person.setName(person.getName().toUpperCase()); // Transform the name
                    return person;
                })
                .writer(writer)
                .build();
    }

    @Bean
    public JdbcPagingItemReader<Person> reader() {
        JdbcPagingItemReader<Person> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(10);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));

        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name, age");
        queryProvider.setFromClause("FROM PERSON");
        queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));

        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO person_processed (id, name, age) VALUES (:id, :name, :age)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

}

