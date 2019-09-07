package com.batch.configuration;

import com.batch.mapper.RecordRowMapper;
import com.batch.step.KafkaItemWriter;
import com.batch.model.Bill;
import com.batch.step.DataProcessor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableTask
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;


    @Bean
    public Job springBatchJob() {
        return jobBuilderFactory.get("databaseToKafkaJob")
                .incrementer(getRunIdIncrementer())
                .start(dataProcessStep())
                .build();
    }

    @Bean
    public RunIdIncrementer getRunIdIncrementer() {
        return new RunIdIncrementer();
    }

    @Bean
    public Step dataProcessStep() {
        return stepBuilderFactory.get("MyProcessorStepName")
                .<Bill, Bill>chunk(1)
                .reader(jdbcItemStreamReader())
                .processor(dataProcessor())
                .writer(kafkaItemWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemStreamReader<Bill> jdbcItemStreamReader() {
        JdbcCursorItemReader<Bill> reader = new JdbcCursorItemReader<Bill>();
        String sql = "select * from BILL_STATEMENTS";
        reader.setSql(sql);
        reader.setDataSource(dataSource);
        reader.setRowMapper(rowMapper());
        return reader;
    }

    @Bean
    public RowMapper<Bill> rowMapper() {
        return new RecordRowMapper();
    }

    @Bean
    ItemProcessor<Bill, Bill> dataProcessor() {
        return new DataProcessor();
    }

    @Bean
    ItemWriter<Bill> kafkaItemWriter() {
        return new KafkaItemWriter();
    }

	@Bean
	public ProducerFactory<String, Bill> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "scdf-release-kafka:9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, Bill> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}



}