/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.dirty.tcp.core.Pipe.Companion.newPipe
import com.exactpro.th2.conn.dirty.tcp.core.RateLimiter
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode.HANDLE_AND_MANGLE
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.netty.ITcpChannelHandler
import com.exactpro.th2.conn.dirty.tcp.core.netty.TcpChannel
import com.exactpro.th2.conn.dirty.tcp.core.util.asExpandable
import com.exactpro.th2.conn.dirty.tcp.core.util.attachMessage
import com.exactpro.th2.conn.dirty.tcp.core.util.eventId
import com.exactpro.th2.conn.dirty.tcp.core.util.submitWithRetry
import com.exactpro.th2.conn.dirty.tcp.core.util.toByteBuf
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toMessage
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.channel.EventLoopGroup
import mu.KotlinLogging
import org.jctools.queues.MpscUnboundedArrayQueue
import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import com.exactpro.th2.common.event.Event as CommonEvent

class Channel(
    private val defaultAddress: InetSocketAddress,
    private val defaultSecurity: Security,
    private val sessionAlias: String,
    private val autoReconnect: Boolean,
    private val reconnectDelay: Long,
    maxMessageRate: Int,
    private val publishConnectEvents: Boolean,
    private val handler: IProtocolHandler,
    private val mangler: IProtocolMangler,
    private val onEvent: (Event) -> Unit,
    private val onMessage: (RawMessage) -> Unit,
    private val executor: ScheduledExecutorService,
    private val eventLoopGroup: EventLoopGroup,
    val parentEventId: EventID,
) : IChannel, ITcpChannelHandler {
    private val logger = KotlinLogging.logger {}
    private val ioExecutor = Executor(executor.newPipe("io-executor-$sessionAlias", MpscUnboundedArrayQueue(65_536), Runnable::run)::send)
    private val sendExecutor = Executor(executor.newPipe("send-executor-$sessionAlias-events-$sessionAlias", MpscUnboundedArrayQueue(65_536), Runnable::run)::send)
    private val limiter = RateLimiter(maxMessageRate)
    private val lock = ReentrantLock()

    @Volatile private var reconnectEnabled = true
    @Volatile private var channel = createChannel(defaultAddress, defaultSecurity)
    private var connectFuture: Future<Unit> = CompletableFuture.completedFuture(Unit)

    private val reconnect: Boolean
        get() = autoReconnect && reconnectEnabled

    override val address: InetSocketAddress
        get() = channel.address

    override val isOpen: Boolean
        get() = channel.isOpen

    override val security: Security
        get() = channel.security

    override fun open() = open(defaultAddress, defaultSecurity)

    override fun open(address: InetSocketAddress, security: Security): Unit = openAsync(address, security).get()

    private fun openAsync(address: InetSocketAddress, security: Security): Future<Unit> {
        logger.debug { "Trying to connect to: $address (session: $sessionAlias)" }

        reconnectEnabled = autoReconnect

        lock.withLock {
            if (isOpen) {
                logger.warn { "Already connected to: ${channel.address} (session: $sessionAlias)" }
                return connectFuture
            }

            if (!connectFuture.isDone) {
                logger.warn { "Already connecting to: ${channel.address} (session: $sessionAlias)" }
                return connectFuture
            }

            if (address != channel.address || security != channel.security) {
                channel = createChannel(address, security)
            }

            connectFuture = executor.submitWithRetry(reconnectDelay) {
                if (publishConnectEvents) onInfo("Connecting to: $address (session: $sessionAlias)")

                val result = runCatching(channel::open).onFailure {
                    onError("Failed to connect to: $address (session: $sessionAlias)", it)
                }

                when {
                    result.isFailure && reconnect -> throw result.exceptionOrNull()!!
                    else -> result
                }
            }

            return connectFuture
        }
    }

    fun send(message: RawMessage): CompletableFuture<MessageID> = sendInternal(
        message = message.body.toByteBuf(),
        metadata = message.metadata.propertiesMap.toMutableMap(),
        parentEventId = message.eventId
    )

    override fun send(message: ByteBuf, metadata: Map<String, String>, mode: SendMode): Future<MessageID> = sendInternal(
        message = message,
        metadata = metadata.toMutableMap(),
        mode = mode
    )

    private fun sendInternal(
        message: ByteBuf,
        metadata: MutableMap<String, String>,
        mode: SendMode = HANDLE_AND_MANGLE,
        parentEventId: EventID? = null,
    ) = CompletableFuture<MessageID>().apply {
        try {
            lock.lock()
            limiter.acquire()

            check(isOpen) { "Cannot send message. Not connected to: $address (session: $sessionAlias)" }

            val buffer = message.asExpandable()

            if (mode.handle) handler.onOutgoing(buffer, metadata)

            val event = if (mode.mangle) mangler.onOutgoing(buffer, metadata) else null
            val protoMessage = buffer.toMessage(sessionAlias, SECOND, metadata, parentEventId)

            thenRunAsync({
                if (mode.mangle) mangler.afterOutgoing(buffer, metadata)
                event?.run { storeEvent(attachMessage(protoMessage), parentEventId ?: this@Channel.parentEventId) }
                onMessage(protoMessage)
            }, sendExecutor)

            channel.send(buffer.asReadOnly()).addListener {
                when (val cause = it.cause()) {
                    null -> complete(protoMessage.metadata.id)
                    else -> completeExceptionally(cause)
                }
            }
        } catch (e: Exception) {
            completeExceptionally(e)
        } finally {
            lock.unlock()
        }
    }

    override fun close() {
        reconnectEnabled = false

        lock.withLock {
            if (isOpen) {
                onInfo("Disconnecting from: $address (session: $sessionAlias)")

                runCatching(channel::close).onFailure {
                    onError("Failed to disconnect from: $address (session: $sessionAlias)", it)
                }
            }
        }
    }

    override fun onOpen() {
        if (publishConnectEvents) onInfo("Connected to: $address (session: $sessionAlias)")
        handler.onOpen()
        mangler.onOpen()
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        logger.trace { "Received data on '$sessionAlias' session: ${hexDump(buffer)}" }
        return handler.onReceive(buffer)
    }

    override fun onMessage(message: ByteBuf) {
        logger.trace { "Received message on '$sessionAlias' session: ${hexDump(message)}" }
        val metadata = handler.onIncoming(message.asReadOnly())
        mangler.onIncoming(message.asReadOnly(), metadata)
        val protoMessage = message.toMessage(sessionAlias, FIRST, metadata)
        onMessage(protoMessage)
        message.release()
    }

    override fun onError(cause: Throwable) = onError("Error on: $address (session: $sessionAlias)", cause)

    override fun onClose() {
        if (publishConnectEvents) onInfo("Disconnected from: $address (session: $sessionAlias)")

        runCatching(handler::onClose).onFailure(::onError)
        runCatching(mangler::onClose).onFailure(::onError)

        if (reconnect) {
            executor.schedule({ if (!isOpen && reconnect) openAsync(defaultAddress, defaultSecurity) }, reconnectDelay, MILLISECONDS)
        }
    }

    private fun onInfo(message: String) {
        logger.info(message)
        storeEvent(message.toEvent(), parentEventId)
    }

    private fun onError(message: String, cause: Throwable) {
        logger.error(message, cause)
        storeEvent(message.toErrorEvent(cause), parentEventId)
    }

    private fun storeEvent(event: CommonEvent, parentEventId: EventID) = onEvent(event.toProto(parentEventId))

    private fun createChannel(address: InetSocketAddress, security: Security) = TcpChannel(
        address,
        security,
        eventLoopGroup,
        ioExecutor,
        this
    )

    data class Security(
        val ssl: Boolean = false,
        val sni: Boolean = false,
        val certFile: File? = null,
        val acceptAllCerts: Boolean = false,
    )
}
