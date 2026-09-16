package kafka.demo.kafkaConfig;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import kafka.demo.utils.KafkaUtils;

@Configuration
public class KafkaAdminDev {
    // Create Topics and Consumers manually
    // Steps to create the topics and Consumer manully
    // 1. Create AdminController so that we can configure the kafka based on that
    // public static void main(String[] args) {
    // Properties props = new Properties();
    // props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // // 1. Create AdminClient using the Properties;
    // try (AdminClient adminClient = AdminClient.create(props)) {

    // // 2. Create Topics manually.
    // NewTopic kafkaJobTopic = new NewTopic(KafkaUtils.KAFKA_JOB_TOPIC_NAME, 3,
    // (short) 1);
    // adminClient.createTopics(Collections.singleton(kafkaJobTopic));

    // // 3. describe the topics.
    // // adminClient.describeTopics(Collections.singleton(kafkaJobTopic));
    // }

    // }

    // setting up a default error handler for the messages that are being failed.
    @Bean
    public DefaultErrorHandler errorHandler() {
        // ExponentialBackOff exponentialBackOff = new ExponentialBackOff(1000L, 2);
        return new DefaultErrorHandler(new ExponentialBackOff(1000L, 2));
    }
}
