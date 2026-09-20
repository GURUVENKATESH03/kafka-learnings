package kafka.demo.avro.controller;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import kafka.demo.avro.producer.KafkaAvroProducer;
import kafka.demo.avro.JobEvent;
import kafka.demo.utils.JobDefCreator;
import kafka.demo.utils.JobStatus;
import kafka.demo.utils.ResponseCreater;

@RestController
public class KafkaAvroController {

    @Autowired
    private JobDefCreator jobDefCreator;

    @Autowired
    private KafkaAvroProducer kafkaAvroProducer;

    @PostMapping("/api/avroController")
    private ResponseCreater<JobEvent> avroController() {
        UUID newScheduleId = UUID.randomUUID();
        UUID newJobId = UUID.randomUUID();

        // job creation - fixed trailing semicolon to dot
        JobEvent jobEvent = JobEvent.newBuilder()
                .setJobDefId(jobDefCreator.createJobDefId(UUID.randomUUID()).toString())
                .setJobId(newJobId.toString())
                .setScheduleId(newScheduleId.toString())
                .setJobStatus(JobStatus.COMPLETED.name())
                .build();

        kafkaAvroProducer.send(jobEvent);

        return ResponseCreater.<JobEvent>builder().success(true).data(jobEvent).build();
    }
}