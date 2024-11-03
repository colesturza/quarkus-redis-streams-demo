package com.example;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.stream.ReactiveStreamCommands;
import io.quarkus.runtime.Startup;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@ApplicationScoped
@Startup
public class StreamProducer {

	private static final Logger log = LoggerFactory.getLogger(StreamProducer.class);
	private final ReactiveStreamCommands<String, String, Person> commands;
	private final ProducerConfig config;

	@Inject
	public StreamProducer(ReactiveRedisDataSource ds, ProducerConfig config) {
		this.commands = ds.stream(Person.class);
		this.config = config;
	}

	public Uni<String> sendMessage(Person person) {
		return commands.xadd(config.streamName(), Map.of(
						"data", person
				))
				.invoke(messageId -> log.info("Produced message to stream {} with ID {}", config.streamName(), messageId))
				.onFailure().invoke(error -> log.error("Failed to produce message to stream {}", config.streamName(), error));
	}

	@ConfigMapping(prefix = "producer")
	public interface ProducerConfig {

		@WithName("stream-name")
		String streamName();
	}
}
