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

import com.exactpro.th2.conn.dirty.tcp.core.TaskSequencePool.WaitStrategy.AUTO
import mu.KotlinLogging
import org.jctools.maps.NonBlockingHashMap
import org.jctools.queues.MpscChunkedArrayQueue
import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.atomic.AtomicInteger

class TaskSequencePool(private val executor: Executor) : AutoCloseable {
    private val sequences = NonBlockingHashMap<String, TaskSequence>()

    fun create(sequence: String, capacity: Int = 1024, strategy: WaitStrategy = AUTO): TaskSequence {
        require(sequence.isNotBlank()) { "Sequence name cannot be blank" }
        require(capacity > 0) { "Capacity must be positive" }

        return sequences.compute(sequence) { _, previous ->
            when {
                previous == null || !previous.isOpen -> TaskSequence(TaskQueue(sequence, executor, capacity, strategy))
                else -> error("Sequence already exists: $sequence")
            }
        }!!
    }

    operator fun get(sequence: String): TaskSequence = sequences[sequence] ?: error("Sequences does not exist: $sequence")

    override fun close() = sequences.values.forEach(TaskSequence::close)

    internal class TaskQueue(
        private val name: String,
        private val executor: Executor,
        private val capacity: Int = Int.MAX_VALUE,
        private val strategy: WaitStrategy = AUTO,
    ) : Runnable, AutoCloseable {
        private val queue = MpscChunkedArrayQueue<Runnable>(capacity.coerceIn(4, Int.MAX_VALUE shr 1))
        private val size = AtomicInteger()
        @Volatile var isOpen = true
            private set

        fun put(task: Runnable) {
            var cycles = 0
            while (!offer(task)) strategy.await(cycles++)
        }

        fun offer(task: Runnable): Boolean {
            check(isOpen) { "Queue is closed: $name" }
            if (size.get() >= capacity || !queue.offer(task)) return false

            when (size.incrementAndGet()) {
                1 -> executor.execute(this)
                capacity -> LOGGER.trace { "Queue is full: $name" }
            }

            return true
        }

        override fun run() {
            while (isOpen) {
                try {
                    queue.poll().run()
                } catch (e: Error) {
                    throw e
                } catch (t: Throwable) {
                    LOGGER.error(t) { "Failed to execute task from queue: $name" }
                } finally {
                    if (size.decrementAndGet() == 0) break
                }
            }
        }

        override fun close() {
            isOpen = false
            queue.clear()
        }
    }

    class TaskSequence internal constructor(private val queue: TaskQueue) : AutoCloseable {
        val isOpen: Boolean
            get() = queue.isOpen

        fun execute(task: Runnable) = queue.put(task)
        fun submit(task: Runnable): Future<*> = FutureTask(task, null).apply(queue::put)
        fun <V> submit(task: Callable<V>): Future<V> = FutureTask(task).apply(queue::put)

        override fun close() = queue.close()
    }

    enum class WaitStrategy {
        SPIN {
            override fun await(cycles: Int) = Unit
        },
        YIELD {
            override fun await(cycles: Int) = Thread.yield()
        },
        SLEEP {
            override fun await(cycles: Int) = Thread.sleep(1)
        },
        AUTO {
            override fun await(cycles: Int) = when {
                cycles < 1_000 -> {}
                cycles < 1_000_000 -> Thread.yield()
                else -> Thread.sleep(1)
            }
        };

        abstract fun await(cycles: Int)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}