package com.batch.step;

import com.batch.model.Bill;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class KafkaItemWriter implements ItemWriter<Bill> {
    @Autowired
    private KafkaTemplate<String, Bill> kafkaTemplate;

    private static final String TOPIC = "bill_details";

    @Override
    public void write(List<? extends Bill> list) throws Exception {
            for(Bill data : list)
            kafkaTemplate.send(TOPIC, data);
    }
}
