package kafka.demo.kafkaConfig;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties.Producer;
import org.springframework.kafka.core.ProducerFactory;

import kafka.demo.utils.KafkaUtils;

public class KafkaAdmin {
    // Create Topics and Consumers manually
    // Steps to create the topics and Consumer manully
    // 1. Create AdminController so that we can configure the kafka based on that
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 1. Create AdminClient using the Properties;
        try (AdminClient adminClient = AdminClient.create(props)) {

            // 2. Create Topics manually.
            NewTopic kafkaJobTopic = new NewTopic(KafkaUtils.KAFKA_JOB_TOPIC_NAME, 3, (short) 1);
            adminClient.createTopics(Collections.singleton(kafkaJobTopic));

            // 3. describe the topics.
            // adminClient.describeTopics(Collections.singleton(kafkaJobTopic));
        }

    }
}
