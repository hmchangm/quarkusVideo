package org.video

import io.minio.GetObjectArgs
import io.minio.MinioClient
import io.minio.StatObjectArgs
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.Path
import javax.ws.rs.core.Response

@Path("/video")
class VideoStreamService(val minioClient: MinioClient) {

    companion object {
        val CONTENT_TYPE = "Content-Type"
        val CONTENT_LENGTH = "Content-Length"
        val VIDEO_CONTENT = "video/mp4"
        val CONTENT_RANGE = "Content-Range"
        val ACCEPT_RANGES = "Accept-Ranges"
        val BYTES = "bytes"
        val CHUNK_SIZE = 3000000L
    }


    @GET
    @Path("big.mp4")
    suspend fun prepareContent(@HeaderParam("range") rangeHeader: String?): Response {
        val fileKey = "big.mp4"
        val range = rangeHeader ?: "bytes=0-"
        val ranges = range.split("-".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val rangeStart = ranges[0].substring(6).toLong()
        val fileSize = readLength(fileKey)
        val rangeEnd = when(ranges.size){
            2 -> ranges[1].toLong()
            else -> rangeStart + CHUNK_SIZE
        }.coerceAtMost(fileSize - 1)
        val inputStream = readInputStreamRange(fileKey, rangeStart, rangeEnd)
        val contentLength = (rangeEnd - rangeStart + 1).toString()
        return Response.ok(inputStream).status(206)
            .header(CONTENT_TYPE, VIDEO_CONTENT)
            .header(ACCEPT_RANGES, BYTES)
            .header(CONTENT_LENGTH, contentLength)
            .header(CONTENT_RANGE, "$BYTES $rangeStart-$rangeEnd/$fileSize")
            .build()

    }


    suspend fun readInputStreamRange(filename: String?, start: Long, end: Long) =
        minioClient.getObject(
            GetObjectArgs.builder()
                .bucket("buck")
                .`object`("video/big.mp4")
                .offset(start).length(end)
                .build()
        )


    suspend fun readLength(filename: String?): Long {
        return minioClient.statObject(
            StatObjectArgs.builder()
                .bucket("buck")
                .`object`("video/big.mp4")
                .build()
        ).size()
    }
}