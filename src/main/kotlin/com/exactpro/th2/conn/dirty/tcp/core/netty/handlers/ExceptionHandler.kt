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

package com.exactpro.th2.conn.dirty.tcp.core.netty.handlers

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import mu.KotlinLogging

class ExceptionHandler(
    private val onError: (Throwable) -> Unit,
    private val onEvent: (Runnable) -> Unit
) : ChannelInboundHandlerAdapter() {
    private val logger = KotlinLogging.logger {}

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.error(cause) { "Error during communication with: ${ctx.channel().remoteAddress()}" }

        onEvent {
            logger.info { "Closing channel due to error" }

            cause.runCatching(onError).onFailure {
                logger.error(it) { "Failed to handle error during communication with: ${ctx.channel().remoteAddress()}" }
            }

            ctx.close()
        }
    }
}