package kafka.demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import kafka.demo.utils.ResponseCreater;

@Component
public class KafkaListener {

    private final Logger log = LoggerFactory.getLogger(KafkaListener.class);

    @org.springframework.kafka.annotation.KafkaListener(topics = ResponseCreater.KAFKA_TOPIC)
    public <T> void kafkaListener(ResponseCreater<T> response) {
        log.info("Received message from consumer = {}", response);
    }
}
