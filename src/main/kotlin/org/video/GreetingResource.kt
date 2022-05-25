package org.video

import io.minio.GetObjectArgs
import io.minio.MinioClient
import io.minio.errors.MinioException
import java.io.InputStream
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response


@Path("/hello")
class GreetingResource(val minioClient: MinioClient) {


    @GET
    @Path("video/big.mp4")
    suspend fun playVideo() =
        minioClient.getObject(
            GetObjectArgs.builder()
                .bucket("buck")
                .`object`("video/big.mp4")
                .build()
        ).let { it as InputStream }
            .let {
                Response.ok(it).status(206).header("Content-Type", "video/mp4").header("Accept-Ranges", "bytes")
                    .header("Content-Disposition", "inline; filename=\"QQQQQ.mp4\"").build()
            }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    suspend fun hello() = "Hello from RESTEasy Reactive"
}