/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.conn.dirty.tcp.core.util.tryCatch
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant

class MainHandler(
    private val onConnect: () -> Unit,
    private val onReceive: (ByteBuf) -> ByteBuf?,
    private val onMessage: (ByteBuf, Instant) -> Unit,
    private val onDisconnect: () -> Unit,
    private val onEvent: (Runnable) -> Unit
) : ByteToMessageDecoder() {
    private val logger = KotlinLogging.logger {}

    override fun channelActive(ctx: ChannelHandlerContext) {
        logger.debug { "Connected to: ${ctx.channel().remoteAddress()}" }
        super.channelActive(ctx)

        onEvent {
            tryCatch(onConnect).onFailure {
                ctx.fireExceptionCaught("Failed to handle connect to: ${ctx.channel().remoteAddress()}", it)
            }
        }
    }

    override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
        val receiveTime = Instant.now()
        logger.trace { "Attempting to decode data received from: ${ctx.channel().remoteAddress()} (data: ${hexDump(buf)})" }

        while (buf.isReadable) {
            val message = buf.readMessage(ctx) ?: break

            onEvent {
                tryCatch { onMessage(message, receiveTime) }.onFailure {
                    ctx.fireExceptionCaught("Failed to handle message received from: ${ctx.channel().remoteAddress()} (message: ${hexDump(message)})", it)
                }
            }
        }
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        logger.debug { "Disconnected from: ${ctx.channel().remoteAddress()}" }
        super.channelInactive(ctx)

        onEvent {
            tryCatch(onDisconnect).onFailure {
                ctx.fireExceptionCaught("Failed to handle disconnect from: ${ctx.channel().remoteAddress()}", it)
            }
        }
    }

    private fun ByteBuf.readMessage(ctx: ChannelHandlerContext): ByteBuf? {
        return tryCatch(onReceive).onFailure(ctx::fireExceptionCaught).getOrNull()
    }

    companion object {
        private fun ChannelHandlerContext.fireExceptionCaught(message: String, cause: Throwable) = fireExceptionCaught(IllegalStateException(message, cause))
    }
}