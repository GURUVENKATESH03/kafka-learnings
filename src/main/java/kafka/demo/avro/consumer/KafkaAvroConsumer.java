package kafka.demo.avro.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import kafka.demo.avro.utils.KafkaAvroUtils;
import kafka.demo.entity.JobEvent;

public class KafkaAvroConsumer {

    private Logger log = LoggerFactory.getLogger(KafkaAvroConsumer.class);

    @KafkaListener(topics = KafkaAvroUtils.KAFKA_TOPIC_NAME)
    private void jobConsumerListener(ConsumerRecord<String, JobEvent> consumerRecord) {
        log.info("Event Recieved from ={} and the event is ={}", KafkaAvroUtils.KAFKA_TOPIC_NAME, consumerRecord);
    }
}
