package kafka.demo.avro.utils;

import org.springframework.stereotype.Component;

@Component
public class KafkaAvroUtils {
    public static final String KAFKA_TOPIC_NAME = "job-kafka-creator";
    public static final int KAFKA_TOPIC_NAME_PARTITIONS = 3;
}
