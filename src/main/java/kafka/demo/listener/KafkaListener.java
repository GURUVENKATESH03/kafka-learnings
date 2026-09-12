package kafka.demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import kafka.demo.entity.JobEvent;
import kafka.demo.service.JobTable;
import kafka.demo.utils.JobStatus;
import kafka.demo.utils.KafkaUtils;
import kafka.demo.utils.ResponseCreater;

@Component
public class KafkaListener {

    private final Logger log = LoggerFactory.getLogger(KafkaListener.class);

    @Autowired
    private JobTable jobTable;

    @org.springframework.kafka.annotation.KafkaListener(topics = ResponseCreater.KAFKA_TOPIC)
    public <T> void kafkaListener(ResponseCreater<T> response) {
        log.info("Received message from consumer = {}", response);
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = KafkaUtils.KAFKA_JOB_TOPIC_NAME)
    public void jobKafkaListener(JobEvent response) {
        log.info("Received message from consumer = {}", response);
        jobTable.jobStatusChanger(response, JobStatus.IN_PROGRESS);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Consumer thread was interrupted", e);
        }
        jobTable.jobStatusChanger(response, JobStatus.COMPLETED);
    }

}