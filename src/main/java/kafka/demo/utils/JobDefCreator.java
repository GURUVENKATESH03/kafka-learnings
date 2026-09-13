package kafka.demo.utils;

import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public class JobDefCreator {
    public String createJobDefId(UUID randomUUID) {
        return "nn-kafka-job-".concat(randomUUID.toString());
    }
}
