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

import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel.Security
import com.exactpro.th2.conn.dirty.tcp.core.netty.handlers.ExceptionHandler
import com.exactpro.th2.conn.dirty.tcp.core.netty.handlers.MainHandler
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslHandler
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import mu.KotlinLogging
import java.net.InetSocketAddress
import java.util.concurrent.Executor
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class TcpChannel(
    val address: InetSocketAddress,
    val security: Security,
    group: EventLoopGroup,
    executor: Executor,
    handler: ITcpChannelHandler,
) {
    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantReadWriteLock()
    private lateinit var channel: Channel
    private val bootstrap = Bootstrap().apply {
        group(group)
        channel(NioSocketChannel::class.java)
        remoteAddress(address)
        handler(object : ChannelInitializer<Channel>() {
            override fun initChannel(ch: Channel): Unit = ch.pipeline().run {
                if (security.ssl) {
                    val context = SslContextBuilder.forClient().apply {
                        when {
                            security.acceptAllCerts -> trustManager(InsecureTrustManagerFactory.INSTANCE)
                            security.certFile != null -> trustManager(security.certFile)
                        }
                    }.build()

                    val engine = when (security.sni) {
                        true -> context.newEngine(ch.alloc(), address.hostName, address.port)
                        else -> context.newEngine(ch.alloc())
                    }

                    addLast("ssl", SslHandler(engine))
                }

                addLast("main", MainHandler(handler::onOpen, handler::onReceive, handler::onMessage, handler::onClose, executor::execute))
                addLast("exception", ExceptionHandler(handler::onError, executor::execute))
            }
        })
    }

    val isOpen: Boolean
        get() = lock.read { ::channel.isInitialized && channel.isActive }

    fun open() = lock.write {
        when (isOpen) {
            true -> logger.warn { "Already connected to: $address" }
            else -> channel = bootstrap.connect().sync().channel()
        }
    }

    fun send(data: ByteBuf) = lock.read<Unit> {
        check(isOpen) { "Cannot send message. Not connected to: $address" }
        channel.writeAndFlush(data).sync()
    }

    fun close() = lock.write<Unit> {
        when (isOpen) {
            false -> logger.warn { "Not connected to: $address" }
            else -> channel.close().sync()
        }
    }
}