package com.example;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.runtime.Startup;
import io.smallrye.config.ConfigMapping;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

@ApplicationScoped
@Startup
public class PersonStreamConsumerGroupReader extends StreamConsumerGroupReader<Person> {

	private static final Logger log = LoggerFactory.getLogger(PersonStreamConsumerGroupReader.class);

	private final Random rand = new Random();
	private static final double FAILURE_PROBABILITY = 0.3; // 30% chance of failure

	PersonStreamConsumerGroupReader() {
		super(null, null);
	}

	@Inject
	public PersonStreamConsumerGroupReader(ReactiveRedisDataSource ds, PersonConsumerConfig config) {
		super(ds.stream(Person.class), config);
	}

	@Override
	protected Uni<Void> process(StreamMessage<String, String, Person> message) {
		return Uni.createFrom().item(Unchecked.supplier(() -> {
					// Simulate a random failure
					if (rand.nextDouble() < FAILURE_PROBABILITY) {
						throw new RuntimeException("Simulated processing failure for message: " + message.id());
					}
					log.info("Processing message - id: {}, key: {}, payload: {}", message.id(), message.key(), message.payload());
					return null;
				}))
				.onItem().delayIt().by(Duration.ofMillis(rand.nextInt(6000))) // simulate random processing delay
				.replaceWithVoid()
				.onFailure().invoke(ex -> log.error("Error processing message - id: {}, error: {}", message.id(), ex.getMessage()));
	}

	@ConfigMapping(prefix = "person-consumer")
	public interface PersonConsumerConfig extends ConsumerConfig {
	}
}
