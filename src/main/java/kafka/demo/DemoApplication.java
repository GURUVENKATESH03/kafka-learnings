package kafka.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public CommandLineRunner produce(KafkaTemplate<String, String> template) {
		return args -> template.send("test-topic", "Hello from Spring Boot!");
	}

	@KafkaListener(topics = "test-topic", groupId = "demo-group")
	public void consume(String message) {
		System.out.println("Received: " + message);
	}

}
