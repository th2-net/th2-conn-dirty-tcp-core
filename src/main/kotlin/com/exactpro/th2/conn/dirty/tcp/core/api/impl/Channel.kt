/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.tcp.core.ChannelFactory.MessageAcceptor
import com.exactpro.th2.conn.dirty.tcp.core.Pipe.Companion.newPipe
import com.exactpro.th2.conn.dirty.tcp.core.RateLimiter
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IMangler
import com.exactpro.th2.conn.dirty.tcp.core.netty.ITcpChannelHandler
import com.exactpro.th2.conn.dirty.tcp.core.netty.TcpChannel
import com.exactpro.th2.conn.dirty.tcp.core.util.nextMessageId
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.buffer.Unpooled
import io.netty.channel.EventLoopGroup
import io.netty.handler.traffic.GlobalTrafficShapingHandler
import mu.KotlinLogging
import org.jctools.queues.SpscUnboundedArrayQueue
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import com.exactpro.th2.common.event.Event as CommonEvent
import io.netty.util.concurrent.Future as NettyFuture

class Channel(
    override val address: InetSocketAddress,
    override val security: Security,
    override val attributes: Map<String, Any>,
    override val sessionGroup: String,
    override val sessionAlias: String,
    private val autoReconnect: Boolean,
    private val reconnectDelay: Long,
    maxMessageRate: Int,
    private val publishConnectEvents: Boolean,
    private val handler: IHandler,
    private val mangler: IMangler,
    private val onEvent: (Event) -> Unit,
    private val onMessage: MessageAcceptor,
    private val executor: ScheduledExecutorService,
    eventLoopGroup: EventLoopGroup,
    shaper: GlobalTrafficShapingHandler,
    private val eventId: EventID,
) : IChannel, ITcpChannelHandler {
    private val logger = KotlinLogging.logger {}
    private val bookName = eventId.bookName
    private val ioExecutor =
        Executor(executor.newPipe("io-executor-$sessionAlias", SpscUnboundedArrayQueue(65_536), Runnable::run)::send)
    private val sendExecutor =
        Executor(executor.newPipe("send-executor-$sessionAlias", SpscUnboundedArrayQueue(65_536), Runnable::run)::send)
    private val limiter = RateLimiter(maxMessageRate)
    private val channel = TcpChannel(address, security, eventLoopGroup, ioExecutor, shaper, this)
    private val lock = ReentrantLock()

    @Volatile
    private var reconnectEnabled = true

    private var openFuture = CompletableFuture.completedFuture(Unit)
    private var closeFuture = CompletableFuture.completedFuture(Unit)

    private val reconnect: Boolean
        get() = autoReconnect && reconnectEnabled

    override val isOpen: Boolean
        get() = channel.isOpen

    override fun open(): CompletableFuture<Unit> {
        logger.debug { "Trying to connect to: $address (session: $sessionAlias)" }

        reconnectEnabled = autoReconnect

        lock.withLock {
            if (isOpen) {
                logger.warn { "Already connected to: $address (session: $sessionAlias)" }
                return openFuture
            }

            if (!openFuture.isDone) {
                logger.warn { "Already connecting to: $address (session: $sessionAlias)" }
                return openFuture
            }

            openFuture = CompletableFuture()

            val connectTask = object : Runnable {
                override fun run() {
                    if (publishConnectEvents) onInfo("Connecting to: $address (session: $sessionAlias)")

                    val channelFuture = channel.open()

                    channelFuture.onSuccess { openFuture.complete(Unit) }

                    channelFuture.onFailure {
                        onError("Failed to connect to: $address (session: $sessionAlias)", it)

                        when {
                            !reconnect -> openFuture.completeExceptionally(it)
                            !openFuture.isCancelled -> executor.schedule(this, reconnectDelay, MILLISECONDS)
                        }
                    }

                    channelFuture.onCancel {
                        onInfo("Cancelled connect to: $address (session: $sessionAlias)")

                        when {
                            !reconnect -> openFuture.cancel(true)
                            !openFuture.isCancelled -> executor.schedule(this, reconnectDelay, MILLISECONDS)
                        }
                    }
                }
            }

            executor.execute(connectTask)

            return openFuture
        }
    }

    override fun send(
        message: ByteBuf,
        metadata: MutableMap<String, String>,
        eventId: EventID?,
        mode: SendMode,
    ): CompletableFuture<MessageID> = CompletableFuture<MessageID>().apply {
        try {
            lock.lock()
            limiter.acquire()

            check(isOpen) { "Cannot send message. Not connected to: $address (session: $sessionAlias)" }

            val buffer = message.asExpandable()

            if (mode.handle) handler.onOutgoing(this@Channel, buffer, metadata)

            val event = if (mode.mangle) mangler.onOutgoing(this@Channel, buffer, metadata) else null
            val messageId = nextMessageId(bookName, sessionGroup, sessionAlias, SECOND)

            thenRunAsync({
                if (mode.mangle) mangler.postOutgoing(this@Channel, buffer, metadata)
                event?.run { storeEvent(messageID(messageId), eventId ?: this@Channel.eventId) }
                onMessage.accept(buffer, messageId, metadata, eventId)
            }, sendExecutor)

            channel.send(buffer.asReadOnly()).apply {
                onSuccess { complete(messageId) }
                onFailure { completeExceptionally(it) }
                onCancel { cancel(true) }
            }
        } catch (e: Exception) {
            completeExceptionally(e)
        } finally {
            lock.unlock()
        }
    }

    override fun close(): CompletableFuture<Unit> {
        logger.debug { "Trying to disconnect from: $address (session: $sessionAlias)" }

        reconnectEnabled = false

        lock.withLock {
            if (!isOpen) {
                logger.warn { "Already disconnected from: $address (session: $sessionAlias)" }
                return closeFuture
            }

            if (!closeFuture.isDone) {
                logger.warn { "Already disconnecting from: $address (session: $sessionAlias)" }
                return closeFuture
            }

            if (publishConnectEvents) onInfo("Disconnecting from: $address (session: $sessionAlias)")

            closeFuture = CompletableFuture()
            val channelFuture = channel.close()

            channelFuture.onSuccess { closeFuture.complete(Unit) }

            channelFuture.onFailure {
                onError("Failed to disconnect from: $address (session: $sessionAlias)", it)
                closeFuture.completeExceptionally(it)
            }

            channelFuture.onCancel {
                onInfo("Cancelled disconnect from: $address (session: $sessionAlias)")
                closeFuture.cancel(true)
            }

            return closeFuture
        }
    }

    override fun onOpen() {
        if (publishConnectEvents) onInfo("Connected to: $address (session: $sessionAlias)")
        handler.onOpen(this)
        mangler.onOpen(this)
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        logger.trace { "Received data on '$sessionAlias' session: ${hexDump(buffer)}" }
        return handler.onReceive(this, buffer)
    }

    override fun onMessage(message: ByteBuf) {
        logger.trace { "Received message on '$sessionAlias' session: ${hexDump(message)}" }
        val metadata = handler.onIncoming(this, message.asReadOnly())
        mangler.onIncoming(this, message.asReadOnly(), metadata)

        val messageCopy = Unpooled.copiedBuffer(message)
        message.release()

        onMessage.accept(messageCopy, nextMessageId(bookName, sessionGroup, sessionAlias, FIRST), metadata, null)
    }

    override fun onError(cause: Throwable): Unit = onError("Error on: $address (session: $sessionAlias)", cause)

    override fun onClose() {
        if (publishConnectEvents) onInfo("Disconnected from: $address (session: $sessionAlias)")

        runCatching(handler::onClose).onFailure(::onError)
        runCatching(mangler::onClose).onFailure(::onError)

        if (reconnect) {
            executor.schedule({ if (!isOpen && reconnect) open() }, reconnectDelay, MILLISECONDS)
        }
    }

    private fun onInfo(message: String) {
        logger.info(message)
        storeEvent(message.toEvent(), eventId)
    }

    private fun onError(message: String, cause: Throwable) {
        logger.error(message, cause)
        storeEvent(message.toErrorEvent(cause), eventId)
    }

    private fun storeEvent(event: CommonEvent, parentEventId: EventID) = onEvent(event.toProto(parentEventId))

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <V, F : NettyFuture<V>> F.onSuccess(action: () -> Unit): F {
            return addListener { if (isSuccess) action() } as F
        }

        @Suppress("UNCHECKED_CAST")
        fun <V, F : NettyFuture<V>> F.onFailure(action: (cause: Throwable) -> Unit): F {
            return addListener { cause()?.run(action) } as F
        }

        @Suppress("UNCHECKED_CAST")
        fun <V, F : NettyFuture<V>> F.onCancel(action: () -> Unit): F {
            return addListener { if (isCancelled) action() } as F
        }
    }
}
