package kafka.demo.utils;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ResponseCreater<T> {
    private boolean success;
    private String[] messages;
    private T data;

    public static final String KAFKA_TOPIC = "ApiLogs";
}