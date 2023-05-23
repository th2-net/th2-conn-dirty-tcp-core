/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.conn.dirty.tcp.core.util.toGroup
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TransportMessageBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val book: String,
    batchByGroup: Boolean,
    private val executor: ScheduledExecutorService,
    private val onBatch: (GroupBatch) -> Unit,
) : AutoCloseable {
    private val batchSelector = if (batchByGroup) GROUP_SELECTOR else ALIAS_SELECTOR
    private val batches = ConcurrentHashMap<Any, Batch>()

    fun onMessage(message: RawMessage.Builder, group: String): Unit =
        batches.getOrPut(batchSelector(message, group)) { Batch(book, group) }.add(message)

    override fun close(): Unit = batches.values.forEach(Batch::close)

    private inner class Batch(
        book: String,
        group: String,
    ) : AutoCloseable {
        private val lock = ReentrantLock()
        private val newBatch: () -> GroupBatch.Builder = {
            GroupBatch.builder().apply {
                setBook(book)
                setSessionGroup(group)
            }
        }
        private var batch = newBatch()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(message: RawMessage.Builder) = lock.withLock {
            message.idBuilder().setTimestamp(Instant.now())
            batch.addGroup(message.build().toGroup())

            when (batch.groupsBuilder().size) {
                1 -> future = executor.schedule(::send, maxFlushTime, TimeUnit.MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsBuilder().size == 0) return
            batch.build().runCatching(onBatch)
                .onFailure { LOGGER.error(it) { "Failed to publish batch: $batch" } }
            batch = newBatch()
            future.cancel(false)
        }

        override fun close() = send()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val GROUP_SELECTOR: (RawMessage.Builder, String) -> Any = { _, group -> group }
        private val ALIAS_SELECTOR: (RawMessage.Builder, String) -> Any = { message, _ ->
            message.idBuilder().sessionAlias to message.idBuilder().direction
        }
    }
}