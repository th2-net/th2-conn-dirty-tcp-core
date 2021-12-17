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
import java.util.concurrent.atomic.AtomicReference

class ActionStreamExecutor(
    private val executor: Executor,
    private val onError: (stream: String, error: Throwable) -> Unit
) {
    private val streams = ConcurrentHashMap<String, Action>()

    fun execute(stream: String, action: Runnable) {
        // add backpressure?
        streams.compute(stream) { _, previous -> Action(action, stream, previous) }
    }

    private inner class Action(
        private val action: Runnable,
        private val stream: String,
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
            next.getAndSet(null).run()
        }
    }

    companion object {
        private val NOP = Runnable {}
    }
}