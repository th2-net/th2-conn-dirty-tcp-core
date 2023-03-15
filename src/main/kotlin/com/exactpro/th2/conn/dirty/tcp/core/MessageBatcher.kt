/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.conn.dirty.tcp.core

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.toGroup
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import java.util.ArrayDeque
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

val GROUP_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionGroup }
val ALIAS_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionAlias to it.direction }

class MessageBatcher(
    private val maxBatchSize: Int = 100,
    private val minFlushTime: Long = MILLISECONDS.toNanos(100),
    private val maxFlushTime: Long = MILLISECONDS.toNanos(1000),
    private val batchSelector: (RawMessage.Builder) -> Any,
    private val executor: ScheduledExecutorService,
    private val onBatch: (MessageGroupBatch) -> Unit,
) : AutoCloseable {
    private val batches = ConcurrentHashMap<Any, Batch>()

    fun onMessage(message: RawMessage.Builder): Boolean = batches.getOrPut(batchSelector(message), ::Batch).add(message)

    override fun close(): Unit = batches.values.forEach(Batch::close)

    private inner class Batch : AutoCloseable {
        private val lock = ReentrantLock()
        private val batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)
        private val messages = ArrayDeque<RawMessage>()
        private var timestamp = Timestamps.MIN_VALUE

        fun add(message: RawMessage.Builder): Boolean = lock.withLock {
            if (timestamp - message.timestamp > 0) return false

            messages += message.build()

            when (messages.size) {
                1 -> future = executor.schedule(::send, maxFlushTime, NANOSECONDS)
                maxBatchSize -> send()
            }

            return true
        }

        private fun send(): Unit = lock.withLock {
            val currentTime = timestamp()

            val flushed = messages.removeIf { message ->
                if (batch.groupsCount == maxBatchSize) return@removeIf false
                val messageTime = message.timestamp
                if (currentTime - messageTime < minFlushTime) return@removeIf false
                batch.addGroups(message.toGroup())
                timestamp = maxOf(timestamp, messageTime, Timestamps.comparator())
                return@removeIf true
            }

            future.cancel(false)

            if (flushed) {
                batch.build().runCatching(onBatch).onFailure { LOGGER.error(it) { "Failed to publish batch: ${batch.toJson()}" } }
                batch.clearGroups()
            }

            if (messages.isNotEmpty()) {
                future = executor.schedule(::send, minFlushTime, NANOSECONDS)
            }
        }

        override fun close() = send()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private operator fun Timestamp.minus(other: Timestamp) = Timestamps.toNanos(this) - Timestamps.toNanos(other)
        private fun timestamp() = Timestamps.fromMillis(System.currentTimeMillis())
        private val RawMessageOrBuilder.timestamp get() = metadata.timestamp
    }
}