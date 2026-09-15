package kafka.demo.controller;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Callback;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import kafka.demo.entity.JobEvent;
import kafka.demo.service.JobTable;
import kafka.demo.utils.JobDefCreator;
import kafka.demo.utils.JobStatus;
import kafka.demo.utils.KafkaUtils;
import kafka.demo.utils.ResponseCreater;

@RestController
@RequestMapping("/api")
public class KafkaController {

        private final RestTemplate restTemplate;
        private final KafkaTemplate<String, Object> kafkaTemplate;
        @Autowired
        private JobTable jobTable;
        @Autowired
        private JobDefCreator jobDefCreator;

        public KafkaController(KafkaTemplate<String, Object> kafkaTemplate) {
                this.kafkaTemplate = kafkaTemplate;
                this.restTemplate = new RestTemplate();
        }

        @PostMapping("/streamLogs")
        public ResponseCreater<String> createKafkaMessages(@RequestParam("targetUrl") String targetUrlString) {
                HttpHeaders headers = new HttpHeaders();
                headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) KafkaClient/1.0");
                HttpEntity<String> entity = new HttpEntity<>(headers);

                ResponseEntity<String> response = restTemplate.exchange(
                                targetUrlString,
                                org.springframework.http.HttpMethod.GET,
                                entity,
                                String.class);

                String logData = response.getBody();
                String topicName = "ApiLogs";
                ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, logData);
                kafkaTemplate.send(record);

                return ResponseCreater.<String>builder()
                                .success(true)
                                .messages(new String[] { "Data has been successfully sent" })
                                .data(logData)
                                .build();
        }

        @PostMapping("/job/create/batch")
        public ResponseCreater<JobEvent> createSchedule() {
                UUID newScheduleId = UUID.randomUUID();
                UUID newJobId = UUID.randomUUID();
                // job creation
                JobEvent jobEvent = JobEvent.builder()
                                .jobDefId(jobDefCreator.createJobDefId(UUID.randomUUID()))
                                .jobId(newJobId)
                                .scheduleId(newScheduleId)
                                .jobStatus(JobStatus.QUEUED)
                                .build();
                // db insertion
                jobTable.jobInsertion(jobEvent);

                // dont create a record without a key
                // in this code we can provide the jobDef Id which is unique for which bath/job
                // is running.
                // refer WIL-13-09-26.md
                ProducerRecord<String, Object> record = new ProducerRecord<>(
                                KafkaUtils.KAFKA_JOB_TOPIC_NAME,
                                jobEvent.getJobDefId(),
                                jobEvent);

                CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> future = kafkaTemplate
                                .send(record);

                // 2. Handle success/failure asynchronously using whenComplete
                future.whenComplete((result, exception) -> {
                        if (exception != null) {
                                System.err.println("Failed to publish record to topic " + record.topic() +
                                                " with key " + record.key());
                                exception.printStackTrace();
                        } else {
                                System.out.printf(
                                                "Successfully published message to topic %s [partition %d] at offset %d%n",
                                                result.getRecordMetadata().topic(),
                                                result.getRecordMetadata().partition(),
                                                result.getRecordMetadata().offset());
                        }
                });

                return ResponseCreater.<JobEvent>builder()
                                .success(true)
                                .messages(new String[] { "Job scheduled successfully" })
                                .data(jobEvent)
                                .build();
        }
}