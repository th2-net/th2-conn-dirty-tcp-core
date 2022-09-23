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

package com.exactpro.th2.conn.dirty.tcp.core.netty

import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security
import com.exactpro.th2.conn.dirty.tcp.core.netty.handlers.ExceptionHandler
import com.exactpro.th2.conn.dirty.tcp.core.netty.handlers.MainHandler
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.flush.FlushConsolidationHandler
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslHandler
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import java.net.InetSocketAddress
import java.util.concurrent.Executor
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class TcpChannel(
    private val address: InetSocketAddress,
    security: Security,
    group: EventLoopGroup,
    executor: Executor,
    handler: ITcpChannelHandler,
) {
    private val lock = ReentrantReadWriteLock()

    private val bootstrap = Bootstrap().apply {
        group(group)
        channel(NioSocketChannel::class.java)
        option(ChannelOption.TCP_NODELAY, true)
        remoteAddress(address)
        handler(ChannelHandler(address, security, executor, handler))
    }

    private lateinit var channel: Channel

    val isOpen: Boolean
        get() = lock.read { ::channel.isInitialized && channel.isActive }

    fun open(): ChannelFuture = lock.write {
        if (!isOpen) return bootstrap.connect().apply { channel = channel() }
        return channel.newFailedFuture(IllegalStateException("Already connected to: $address"))
    }

    fun send(data: ByteBuf): ChannelFuture = lock.read {
        if (!isOpen) return channel.newFailedFuture(IllegalStateException("Cannot send message. Not connected to: $address"))
        while (!channel.isWritable && channel.isActive) Thread.sleep(1)
        channel.writeAndFlush(data)
    }

    fun close(): ChannelFuture = lock.write {
        if (isOpen) return channel.close()
        return channel.newFailedFuture(IllegalStateException("Not connected to: $address"))
    }

    private class ChannelHandler(
        private val address: InetSocketAddress,
        private val security: Security,
        private val executor: Executor,
        private val handler: ITcpChannelHandler,
    ) : ChannelInitializer<Channel>() {
        override fun initChannel(ch: Channel): Unit = ch.pipeline().run {
            addLast("flusher", FlushConsolidationHandler(256, true))
            if (security.ssl) addLast("ssl", createSslHandler(address, security, ch.alloc()))
            addLast("main", MainHandler(handler::onOpen, handler::onReceive, handler::onMessage, handler::onClose, executor::execute))
            addLast("exception", ExceptionHandler(handler::onError, executor::execute))
        }

        private fun createSslHandler(address: InetSocketAddress, security: Security, allocator: ByteBufAllocator): SslHandler {
            val context = SslContextBuilder.forClient()

            when {
                security.acceptAllCerts -> context.trustManager(InsecureTrustManagerFactory.INSTANCE)
                security.certFile != null -> context.trustManager(security.certFile)
            }

            val engine = when (security.sni) {
                true -> context.build().newEngine(allocator, address.hostName, address.port)
                else -> context.build().newEngine(allocator)
            }

            return SslHandler(engine)
        }
    }
}