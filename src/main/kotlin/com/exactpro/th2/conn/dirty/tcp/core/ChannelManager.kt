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

import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS

class ChannelManager(
    private val channels: Map<String, Channel>,
    private val executor: ScheduledExecutorService,
) {
    private val stopFutures = HashMap<String, Future<*>>()

    fun isRunning(sessionAlias: String): Boolean = getChannel(sessionAlias).isOpen

    fun open(sessionAlias: String, stopAfter: Long = 0) = getChannel(sessionAlias).apply {
        synchronized(this) {
            if (!isOpen) return@apply else open()
            if (stopAfter <= 0) return@apply
            stopFutures[sessionAlias] = executor.schedule(::close, stopAfter, MILLISECONDS)
        }
    }

    fun openAll(stopAfter: Long = 0) = channels.keys.forEach { sessionAlias ->
        open(sessionAlias, stopAfter)
    }

    fun close(sessionAlias: String) = getChannel(sessionAlias).apply {
        synchronized(this) {
            stopFutures[sessionAlias]?.cancel(true)
            close()
        }
    }

    fun closeAll() = channels.keys.forEach(::close)

    private fun getChannel(sessionAlias: String) = requireNotNull(channels[sessionAlias]) {
        "Unknown session alias: $sessionAlias"
    }
}