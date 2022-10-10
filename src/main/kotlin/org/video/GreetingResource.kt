package org.video

import io.minio.MinioClient
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

@Path("/hello")
class GreetingResource(val minioClient: MinioClient) {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    suspend fun hello() = "Hello from RESTEasy Reactive"
}
