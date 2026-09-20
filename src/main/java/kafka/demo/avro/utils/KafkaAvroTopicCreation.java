package kafka.demo.avro.utils;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
public class KafkaAvroTopicCreation {

    @Bean
    public NewTopic createJobTopic() {
        return new NewTopic(KafkaAvroUtils.KAFKA_TOPIC_NAME, KafkaAvroUtils.KAFKA_TOPIC_NAME_PARTITIONS, (short) 1);
    }
}
