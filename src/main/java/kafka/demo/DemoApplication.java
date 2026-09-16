package kafka.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	// @Bean
	// public DefaultErrorHandler errorHandler() {
	// ExponentialBackOff exponentialBackOff = new ExponentialBackOff(1000L, 2);
	// return new DefaultErrorHandler(exponentialBackOff);
	// }

}
