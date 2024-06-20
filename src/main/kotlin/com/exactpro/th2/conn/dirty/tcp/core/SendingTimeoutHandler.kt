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

import com.exactpro.th2.common.event.Event
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Consumer
import javax.annotation.concurrent.GuardedBy
import javax.annotation.concurrent.ThreadSafe
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/**
 * [SendingTimeoutHandler] keeps track of failed attempts to get a result from a [Future].
 * Whenever the attempt is successful and any of the previous attempts is failed
 * an [Event] is created with information about how many attempts was failed before the current successful one.
 */
@ThreadSafe
class SendingTimeoutHandler private constructor(
    private val minTimeout: Long,
    private val maxTimeout: Long,
    private val eventConsumer: Consumer<Event>
) {
    private val lock = ReentrantReadWriteLock()

    @GuardedBy("lock")
    private var currentTimeout: Long = maxTimeout

    @GuardedBy("lock")
    private var attempts = 0

    init {
        require(maxTimeout >= minTimeout) { "max timeout must be greater than min timeout" }
        require(maxTimeout > 0) { "max timeout must be greater than zero" }
        require(minTimeout > 0) { "min timeout must be greater than zero" }
    }

    @Throws(ExecutionException::class, InterruptedException::class, TimeoutException::class)
    fun <T> getWithTimeout(future: Future<T>): T = try {
        val timeout = getCurrentTimeout()
        future.get(timeout, TimeUnit.MILLISECONDS).also {
            val attempts: Int = lock.write {
                val prevAttempts = this.attempts
                this.attempts = 0
                currentTimeout = maxTimeout
                prevAttempts
            }
            if (attempts > 0) {
                generateEvent(attempts)
            }
        }
    } catch (ex: ExecutionException) {
        attemptFailed()
        throw ex
    } catch (ex: InterruptedException) {
        attemptFailed()
        throw ex
    } catch (ex: TimeoutException) {
        attemptFailed()
        throw ex
    }

    private fun attemptFailed() {
        lock.write {
            attempts += 1
            currentTimeout = max(minTimeout.toDouble(), (currentTimeout / 2).toDouble()).toLong()
        }
    }

    private fun generateEvent(attempts: Int) {
        eventConsumer.accept(
            Event.start().endTimestamp()
                .status(Event.Status.FAILED)
                .name("Message sending attempt successful after $attempts failed attempt(s)")
                .type("MessageSendingAttempts")
        )
    }

    fun getCurrentTimeout(): Long = lock.read { currentTimeout }

    fun getDeadline(): Long = lock.read { System.currentTimeMillis() + currentTimeout }

    companion object {
        @JvmStatic
        fun create(
            minTimeout: Long,
            maxTimeout: Long,
            eventConsumer: Consumer<Event>,
        ): SendingTimeoutHandler = SendingTimeoutHandler(minTimeout, maxTimeout, eventConsumer)
    }
}