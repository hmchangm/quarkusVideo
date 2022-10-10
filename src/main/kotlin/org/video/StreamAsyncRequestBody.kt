package org.video

import io.ktor.util.cio.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncRequestBody
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.*

@OptIn(DelicateCoroutinesApi::class)
class StreamAsyncRequestBody(
    inputStream: InputStream,
    private val coroutineScope: CoroutineScope = GlobalScope
) :
    AsyncRequestBody {
    private val inputChannel =
        inputStream.toByteReadChannel(context = coroutineScope.coroutineContext)

    override fun subscribe(subscriber: Subscriber<in ByteBuffer>) {
        subscriber.onSubscribe(object : Subscription {
            private var done: Boolean = false
            override fun request(n: Long) {
                if (done) return
                if (inputChannel.isClosedForRead) {
                    complete()
                }
                coroutineScope.launch {
                    inputChannel.read {
                        subscriber.onNext(it)
                        if (inputChannel.isClosedForRead) {
                            complete()
                        }
                    }
                }
            }

            private fun complete() {
                subscriber.onComplete()
                synchronized(this) {
                    done = true
                }
            }

            override fun cancel() {
                synchronized(this) {
                    done = true
                }
            }
        })
    }

    override fun contentLength(): Optional<Long> = Optional.empty()
}
