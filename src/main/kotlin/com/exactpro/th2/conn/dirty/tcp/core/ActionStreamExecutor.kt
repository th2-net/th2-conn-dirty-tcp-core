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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

/**
 * Executor which ensures that actions of a single stream are executed in their submission order.
 *
 * Actions of different streams can be executed concurrently
 *
 * @param executor executor to run actions on
 * @param maxStreamSize max stream size for bounded streams
 * @param onError callback for failed executions
 */
class ActionStreamExecutor(
    private val executor: Executor,
    private val maxStreamSize: Int = 1000,
    private val onError: (stream: String, error: Throwable) -> Unit
) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val scope = CoroutineScope(executor.asCoroutineDispatcher())
    private val streams = ConcurrentHashMap<String, SendChannel<Runnable>>()

    /**
     * Submits [action] for execution in the specified [stream].
     *
     * Streams can be bounded and unbounded.
     *
     * Submissions to bounded streams will be blocked while max stream size is exceeded.
     *
     * During submission action will be scheduled for execution immediately if the previous action has been already executed.
     *
     * Otherwise, it will be linked to the previous action which will schedule it for execution after completion.
     *
     * @param stream action stream
     * @param bounded tells whether stream is bounded or not
     * @param action action to execute in the stream
     */
    fun execute(stream: String, bounded: Boolean = true, action: Runnable) {
        val name = if (bounded) stream else "$stream-unbounded"
        val size = if (bounded) maxStreamSize else Channel.UNLIMITED

        val channel = streams.computeIfAbsent(name) {
            Channel<Runnable>(size).apply {
                scope.launch {
                    while (isActive) {
                        runCatching(receive()::run).onFailure { onError(name, it) }
                    }
                }.invokeOnCompletion {
                    cancel()
                }
            }
        }

        if (channel.trySend(action).isFailure) {
            logger.debug { "Stream is full: $name" }
            channel.trySendBlocking(action).getOrThrow()
            logger.debug { "Added action to stream: $name" }
        }
    }

    override fun close() = scope.cancel("Executor is being shutdown")
}