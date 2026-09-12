package kafka.demo.controller;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import kafka.demo.utils.ResponseCreater;

@RestController
@RequestMapping("/api")
public class KafkaController {

    private final RestTemplate restTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.restTemplate = new RestTemplate();
    }

    @PostMapping("/streamLogs")
    public ResponseCreater<String> createKafkaMessages(@RequestParam("targetUrl") String targetUrlString) {

        // 1. Set headers to prevent 403 Forbidden blocks from strict servers
        HttpHeaders headers = new HttpHeaders();
        headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) KafkaClient/1.0");
        HttpEntity<String> entity = new HttpEntity<>(headers);

        // 2. Fetch data using exchange to include the headers
        ResponseEntity<String> response = restTemplate.exchange(
                targetUrlString,
                org.springframework.http.HttpMethod.GET,
                entity,
                String.class);

        String logData = response.getBody();
        String topicName = "ApiLogs";
        // 3. Publish to Kafka using ProducerRecord
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, logData);
        kafkaTemplate.send(record);

        return ResponseCreater.<String>builder()
                .success(true)
                .messages(new String[] { "Data has been successfully sent" })
                .data(logData)
                .build();
    }
}
