package org.video

import io.ktor.utils.io.jvm.javaio.*
import io.minio.GetObjectArgs
import io.minio.MinioClient
import io.minio.StatObjectArgs
import kotlinx.coroutines.future.await
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.core.Response

@Path("/video")
class VideoStreamService(val minioClient: MinioClient, val s3AsyncClient: S3AsyncClient) {

    companion object {
        const val CONTENT_TYPE = "Content-Type"
        const val CONTENT_LENGTH = "Content-Length"
        const val VIDEO_CONTENT = "video/mp4"
        const val CONTENT_RANGE = "Content-Range"
        const val ACCEPT_RANGES = "Accept-Ranges"
        const val BYTES = "bytes"
        const val CHUNK_SIZE = 4000000L
    }

    @GET
    @Path("/{fileKey}")
    suspend fun prepareContent(fileKey: String, @HeaderParam("range") rangeHeader: String?): Response {
        val range = rangeHeader ?: "bytes=0-"
        val ranges = range.split("-".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val rangeStart = ranges[0].substring(6).toLong()
        val fileSize = readLength(fileKey)
        val rangeEnd = when (ranges.size) {
            2 -> ranges[1].toLong()
            else -> rangeStart + CHUNK_SIZE
        }.coerceAtMost(fileSize - 1)
        val contentLength = (rangeEnd - rangeStart + 1)
        val inputStream = readInputStreamRange(fileKey, rangeStart, contentLength)

        return Response.ok(inputStream).status(206)
            .header(CONTENT_TYPE, VIDEO_CONTENT)
            .header(ACCEPT_RANGES, BYTES)
            .header(CONTENT_LENGTH, contentLength.toString())
            .header(CONTENT_RANGE, "$BYTES $rangeStart-$rangeEnd/$fileSize")
            .build()
    }

    @POST
    @Path("/{fileKey}")
    suspend fun upload(fileKey: String, file: File): String {
        s3AsyncClient.putObject(
            PutObjectRequest.builder()
                .bucket("buck")
                .key(fileKey)
                .build(),
            AsyncRequestBody.fromFile(file)
        ).await()

        return "OK"
    }

    suspend fun readInputStreamRange(filename: String, start: Long, contentLength: Long) =

        minioClient.getObject(
            GetObjectArgs.builder()
                .bucket("buck")
                .`object`("$filename")
                .offset(start).length(contentLength)
                .build()
        )!!

    suspend fun readLength(filename: String): Long {
        return minioClient.statObject(
            StatObjectArgs.builder()
                .bucket("buck")
                .`object`("video/$filename")
                .build()
        ).size()
    }
}
