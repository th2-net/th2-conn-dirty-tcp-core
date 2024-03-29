/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.dirty.tcp.core.util

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.function.Supplier

fun <T> ScheduledExecutorService.submitWithRetry(delay: Long, task: Supplier<Result<T>>): Future<T> = CompletableFuture<T>().apply {
    execute(object : Runnable {
        override fun run() {
            try {
                task.get().onSuccess(::complete).onFailure(::completeExceptionally)
            } catch (e: Exception) {
                if (!isCancelled) schedule(this, delay, MILLISECONDS)
            }
        }
    })
}
