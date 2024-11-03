package com.example;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/person")
public class PersonResource {

	private static final Logger log = LoggerFactory.getLogger(PersonResource.class);

	@Inject
	StreamProducer producer;

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Uni<Response> publishPerson(Person person) {
		return producer.sendMessage(person)
				.onItem().transform(messageId -> {
					log.info("Published person with name {} to stream", person.name());
					return Response.ok("Person published with message name: " + messageId).build();
				})
				.onFailure().recoverWithItem(error -> {
					log.error("Failed to publish person to stream", error);
					return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
							.entity("Failed to publish person").build();
				});
	}
}
