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
 * @param boundedStreamSize max stream size for streams with backpressure
 * @param onError callback for failed executions
 */
class ActionStreamExecutor(
    private val executor: Executor,
    private val boundedStreamSize: Int = 1000,
    private val onError: (stream: String, error: Throwable) -> Unit
) {
    private val actions = ConcurrentHashMap<String, Action>()
    private val sizes = ConcurrentHashMap<String, AtomicInteger>() // use semaphores instead?

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
        val stream = if (bounded) stream else "$stream-unbounded"
        val size = sizes.getOrPut(stream, ::AtomicInteger)

        if (bounded) {
            while (size.get() > boundedStreamSize) {
                LockSupport.parkNanos(1_000_000)
            }
        }

        size.incrementAndGet()
        actions.compute(stream) { _, previous -> Action(action, stream, size, previous) }
    }

    private inner class Action(
        private val action: Runnable,
        private val stream: String,
        private val size: AtomicInteger,
        previous: Action? = null
    ) : Runnable {
        private val next = AtomicReference(NOP)

        init {
            if (previous == null || !previous.next.compareAndSet(NOP, this)) {
                run()
            }
        }

        override fun run() = executor.execute {
            runCatching(action::run).recoverCatching { onError(stream, it) }
            size.decrementAndGet()
            next.getAndSet(null).run()
        }
    }

    companion object {
        private val NOP = Runnable {}
    }
}