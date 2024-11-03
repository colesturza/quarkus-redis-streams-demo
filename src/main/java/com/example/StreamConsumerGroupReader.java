package com.example;

import io.quarkus.redis.datasource.stream.ReactiveStreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XGroupCreateArgs;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StreamConsumerGroupReader<T> {

	private static final String READ_FROM_BEGINNING_OF_STREAM = "0";
	private static final String READ_UNDELIVERED_MESSAGES_ONLY = ">";

	private static final Logger log = LoggerFactory.getLogger(StreamConsumerGroupReader.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final ReactiveStreamCommands<String, String, T> commands;
	private final ConsumerConfig config;

	public StreamConsumerGroupReader(ReactiveStreamCommands<String, String, T> commands, ConsumerConfig config) {
		this.commands = commands;
		this.config = config;
	}

	protected abstract Uni<Void> process(StreamMessage<String, String, T> message);

	protected void handleProcessedMessage(ProcessedMessageResult<T> result) {
	}

	protected void handleProcessingError(Throwable failure) {
		log.error("Stream consumption failed. Will automatically retry.", failure);
	}

	@PostConstruct
	void initialize() {
		initializeStreamAndGroup()
				.onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(10)).atMost(5)
				.subscribe().with(
						unused -> startConsuming(),
						failure -> log.error("Failed to initialize stream and consumer group", failure)
				);
	}

	@PreDestroy
	void cleanup() {
		closed.set(true);
	}

	private Uni<Void> initializeStreamAndGroup() {
		return commands.xgroupCreate(
				config.streamName(),
				config.groupName(),
				READ_FROM_BEGINNING_OF_STREAM,
				new XGroupCreateArgs().mkstream() // create stream if it doesn't already exist
		).onFailure().invoke(failure -> log.error("Stream or group creation failed", failure));
	}

	private void startConsuming() {
		Multi.createBy().repeating()
				.uni(this::claimOrReadMessages)
				.until(__ -> closed.get())
				.flatMap(messages -> Multi.createFrom().iterable(messages))
				.onItem().transformToUniAndMerge(this::processMessageSafely)
				.subscribe().with(this::handleProcessedMessage, this::handleProcessingError);
	}

	private Uni<List<StreamMessage<String, String, T>>> claimOrReadMessages() {
		return commands.xautoclaim(
				config.streamName(),
				config.groupName(),
				config.consumerName(),
				Duration.ofSeconds(config.claimIntervalSeconds()),
				READ_FROM_BEGINNING_OF_STREAM,
				config.batchSize()
		).chain(claimed -> {
			if (!claimed.getMessages().isEmpty()) {
				return Uni.createFrom().item(claimed.getMessages());
			}
			return commands.xreadgroup(
					config.groupName(),
					config.consumerName(),
					config.streamName(),
					READ_UNDELIVERED_MESSAGES_ONLY,
					new XReadGroupArgs()
							.block(Duration.ofSeconds(5))
							.count(config.batchSize())
			);
		});
	}

	private Uni<ProcessedMessageResult<T>> processMessageSafely(StreamMessage<String, String, T> message) {
		CompletableFuture<Boolean> processingFuture = new CompletableFuture<>();
		startReclaimTask(message.id(), processingFuture);

		Uni<Void> processingUni = process(message).chain(() -> acknowledgeMessage(message.id()));

		if (config.maxProcessingTimeSeconds() > 0) {
			Duration processingTimeout = Duration.ofSeconds(config.maxProcessingTimeSeconds());

			processingUni = processingUni
					.ifNoItem().after(processingTimeout).failWith(Unchecked.supplier(() -> {
						processingFuture.cancel(true);
						throw new MessageProcessingCancelledException(message.id());
					}));
		}

		return processingUni.map(__ -> {
					processingFuture.complete(true);
					return new ProcessedMessageResult<>(message); // Success, no error
				})
				.onFailure().recoverWithItem(throwable -> {
					processingFuture.completeExceptionally(throwable);
					log.error("Failed to process message with id: {}", message.id(), throwable);
					return new ProcessedMessageResult<>(message, throwable); // Failure, with error
				});
	}

	private void startReclaimTask(String messageId, CompletableFuture<Boolean> processingFuture) {
		Multi.createBy().repeating()
				.uni(() -> Uni.createFrom()
						.voidItem()
						.onItem().delayIt().by(Duration.ofMillis(1000))
						.chain(unused -> commands.xclaim(
										config.streamName(),
										config.groupName(),
										config.consumerName(),
										Duration.ofMillis(1000),
										messageId)
								.onFailure().retry()
								.withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(5))
								.atMost(config.retryAttempts())))
				.until(__ -> processingFuture.isDone())
				.subscribe().with(
						ignored -> log.info("Reclaimed task for message - id: {}", messageId),
						throwable -> log.error("Error in reclaim task for message - id: {}", messageId, throwable));
	}

	private Uni<Void> acknowledgeMessage(String messageId) {
		return commands.xack(config.streamName(), config.groupName(), messageId)
				.onFailure().retry()
				.withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(5))
				.atMost(config.retryAttempts())
				.invoke(unused -> log.info("Acknowledged message with id: {}, {}", messageId, unused))
				.onFailure().invoke(failure -> log.error("Failed to acknowledge message with id: {}", messageId, failure))
				.replaceWithVoid();
	}

	public interface ConsumerConfig {

		@WithName("stream-name")
		String streamName();

		@WithName("group-name")
		String groupName();

		@WithName("consumer-name")
		String consumerName();

		@WithName("claim-interval-seconds")
		@WithDefault("5")
		int claimIntervalSeconds();

		@WithName("batch-size")
		@WithDefault("10")
		int batchSize();

		@WithName("retry-attempts")
		@WithDefault("1")
		int retryAttempts();

		@WithName("max-processing-time-seconds")
		@WithDefault("0")
		int maxProcessingTimeSeconds();
	}

	public static class ProcessedMessageResult<T> {
		private final StreamMessage<String, String, T> message;
		private final Throwable throwable;

		public ProcessedMessageResult(StreamMessage<String, String, T> message) {
			this(message, null);
		}

		private ProcessedMessageResult(StreamMessage<String, String, T> message, Throwable throwable) {
			this.message = message;
			this.throwable = throwable;
		}

		public StreamMessage<String, String, T> getMessage() {
			return message;
		}

		public Optional<Throwable> getThrowable() {
			return Optional.ofNullable(throwable);
		}
	}

	public static class MessageProcessingCancelledException extends RuntimeException {
		private final String messageId;

		public MessageProcessingCancelledException(String messageId) {
			super("Processing cancelled for message - id: " + messageId);
			this.messageId = messageId;
		}

		public String getMessageId() {
			return messageId;
		}
	}
}
