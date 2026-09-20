package kafka.demo.avro.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.demo.avro.JobEvent;

@Configuration
public class KafkaAvroProducerSerializer {
    @Bean
    public ProducerFactory<String, JobEvent> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 1. Bootstrap servers
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 2. Acks config (start with "1", later test "all") {ref - WIL-15-06-26.md}
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // 3. Enable idempotence (forces acks=all and retries internally)
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 4. Key and Value Serializers
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", "http://localhost:8081");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, JobEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
