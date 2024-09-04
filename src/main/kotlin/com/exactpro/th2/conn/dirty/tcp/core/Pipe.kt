/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jctools.queues.MpscChunkedArrayQueue
import java.util.Queue
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

class Pipe<E>(
    private val name: String,
    private val queue: Queue<E>,
    private val executor: Executor,
    private val consumer: Consumer<E>,
) : Runnable, AutoCloseable {
    private val size = AtomicInteger()

    @Volatile var isOpen: Boolean = true
        private set

    fun send(item: E, timeout: Long = 0): Boolean {
        if (offer(item)) return true
        if (timeout <= 0) return false

        val deadline = System.currentTimeMillis() + timeout

        while (deadline > System.currentTimeMillis()) {
            if (offer(item)) return true
            Thread.sleep(1)
        }

        return false
    }

    private fun offer(item: E): Boolean {
        check(isOpen) { "Pipe is closed: $name" }
        if (!queue.offer(item)) return false
        if (size.incrementAndGet() == 1) schedule()
        return true
    }

    private fun schedule() {
        try {
            executor.execute(this)
        } catch (e: Exception) {
            isOpen = false
            LOGGER.error(e) { "Failed to schedule consume task for pipe: $name" }
        }
    }

    override fun run() {
        try {
            consumer.accept(queue.poll())
        } catch (e: Error) {
            throw e
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (t: Throwable) {
            LOGGER.error(t) { "Failed to consume element from pipe: $name" }
        } finally {
            if (isOpen && size.decrementAndGet() > 0) schedule()
        }
    }

    override fun close() {
        isOpen = false
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        fun <E> Executor.newPipe(
            name: String,
            queue: Queue<E> = MpscChunkedArrayQueue(262_144),
            consumer: Consumer<E>,
        ): Pipe<E> = Pipe(name, queue, this, consumer)
    }
}