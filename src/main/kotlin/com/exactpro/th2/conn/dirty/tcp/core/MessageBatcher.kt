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
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.conn.dirty.tcp.core.util.toGroup
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

val GROUP_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionGroup }
val ALIAS_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionAlias to it.direction }

class MessageBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val batchSelector: (RawMessage.Builder) -> Any,
    private val executor: ScheduledExecutorService,
    private val onBatch: (MessageGroupBatch) -> Unit,
) : AutoCloseable {
    private val batches = ConcurrentHashMap<Any, Batch>()

    fun onMessage(message: RawMessage.Builder): Unit = batches.getOrPut(batchSelector(message), ::Batch).add(message)

    override fun close(): Unit = batches.values.forEach(Batch::close)

    private inner class Batch : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(message: RawMessage.Builder) = lock.withLock {
            message.metadataBuilder.timestamp = Instant.now().toTimestamp()
            batch.addGroups(message.toGroup())

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsCount == 0) return
            batch.build().runCatching(onBatch).onFailure { LOGGER.error(it) { "Failed to publish batch: ${batch.toJson()}" } }
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}