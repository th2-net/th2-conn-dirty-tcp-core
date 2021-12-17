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
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.conn.dirty.tcp.core.util.toGroup
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class MessageBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onBatch: (MessageGroupBatch) -> Unit
) : AutoCloseable {
    private val batches = ConcurrentHashMap<String, MessageBatch>()

    fun onMessage(message: RawMessage) {
        batches.getOrPut(message.sessionAlias, ::MessageBatch).add(message)
    }

    override fun close() = batches.values.forEach(MessageBatch::close)

    private inner class MessageBatch : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(message: RawMessage) = lock.withLock {
            batch.addGroups(message.toGroup())

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsCount == 0) return
            batch.build().runCatching(onBatch)
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }
}