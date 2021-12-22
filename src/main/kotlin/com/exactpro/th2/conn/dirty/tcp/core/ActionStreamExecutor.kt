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

import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

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
) {
    private val logger = KotlinLogging.logger {}
    private val streams = ConcurrentHashMap<String, Stream>()

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
        val stream = when (bounded) {
            true -> streams.computeIfAbsent(stream) { Stream(stream, maxStreamSize) }
            else -> streams.computeIfAbsent("$stream-unbounded", ::Stream)
        }

        if (!stream.offer(action)) {
            logger.warn { "Stream is full: ${stream.name}" }
            stream.put(action)
            logger.debug { "Added action to stream: ${stream.name}" }
        }
    }

    private inner class Stream(val name: String, val capacity: Int = Int.MAX_VALUE) {
        private val tail = AtomicReference(NIL)
        private val size = AtomicInteger()

        fun put(action: Runnable) {
            while (!offer(action)) LockSupport.parkNanos(1_000_000)
        }

        fun offer(action: Runnable): Boolean {
            if (size.get() >= capacity) return false

            size.incrementAndGet()

            val next = AtomicReference(NOP)
            val prev = tail.getAndSet(next)

            val task = Runnable {
                runCatching(action::run).recoverCatching { onError(name, it) }
                size.decrementAndGet()
                executor.execute(next.getAndSet(null))
            }

            if (!prev.compareAndSet(NOP, task)) executor.execute(task)

            return true
        }
    }

    companion object {
        private val NOP = Runnable {}
        private val NIL = AtomicReference<Runnable>()
    }
}