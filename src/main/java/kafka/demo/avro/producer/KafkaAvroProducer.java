package kafka.demo.avro.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import kafka.demo.avro.JobEvent;
import kafka.demo.avro.utils.KafkaAvroUtils;

@Service
public class KafkaAvroProducer {

    private final KafkaTemplate<String, JobEvent> kafkaTemplate;

    public KafkaAvroProducer(KafkaTemplate<String, JobEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(JobEvent jobEvent) {

        CompletableFuture<SendResult<String, JobEvent>> future = kafkaTemplate.send(
                KafkaAvroUtils.KAFKA_TOPIC_NAME,
                jobEvent.getJobDefId().toString(),
                jobEvent);

        future.whenComplete((result, exception) -> {

            if (exception != null) {
                System.err.println(
                        "Exception = " + exception.getMessage());
            } else {
                System.out.println(
                        "Event = " + jobEvent);
            }
        });
    }
}